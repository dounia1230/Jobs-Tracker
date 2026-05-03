"""
============================================================
Moroccan Job Market Analyzer — Indeed Morocco Scraper
============================================================
Scrapes job postings from ma.indeed.com filtered for Morocco.
Indeed uses React-rendered content, so we use their unofficial
JSON API endpoint (no authentication needed for public listings).

Output:
- Sends each job to Kafka topic 'jobs-raw'
- Saves batch to MinIO bronze/indeed/{date}/{timestamp}.json
============================================================
"""

import json
import time
import random
import hashlib
import logging
import os
import re
from datetime import datetime
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from bs4 import BeautifulSoup

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class IndeedScraper(BaseScraper):
    """
    Scraper for Indeed Morocco (ma.indeed.com).

    Indeed loads jobs via internal API calls. We target the
    mosaic API endpoint which returns JSON without JS rendering.
    Fallback: parse the HTML listing page.
    """

    SOURCE = "indeed"
    BASE_URL = "https://ma.indeed.com"
    SEARCH_URL = "https://ma.indeed.com/jobs"
    MAX_PAGES = int(os.getenv("SCRAPER_MAX_PAGES", "5"))
    DELAY = float(os.getenv("SCRAPER_DELAY", "2.0"))
    JOBS_PER_PAGE = 15  # Indeed shows 15 jobs per page

    # Search terms to cover broad Moroccan job market
    SEARCH_QUERIES = [
        ("", "Casablanca"),          # All jobs in Casablanca
        ("informatique", "Maroc"),   # IT jobs in Morocco
        ("data", "Maroc"),           # Data jobs
        ("ingénieur", "Maroc"),      # Engineer jobs
    ]

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update(self._get_headers())

    def scrape(self) -> list[dict]:
        """Main entry point. Returns list of raw job dicts."""
        all_jobs = []
        seen_urls = set()

        for query, location in self.SEARCH_QUERIES:
            logger.info(f"[Indeed] Searching: query='{query}' location='{location}'")

            for page_num in range(self.MAX_PAGES):
                start = page_num * self.JOBS_PER_PAGE
                try:
                    jobs = self._scrape_search(query, location, start)
                    if not jobs:
                        break

                    # Dedup across search queries
                    new_jobs = [j for j in jobs if j["url"] not in seen_urls]
                    for j in new_jobs:
                        seen_urls.add(j["url"])
                    all_jobs.extend(new_jobs)

                    logger.info(f"[Indeed] '{query}' page {page_num+1}: {len(new_jobs)} new jobs (total: {len(all_jobs)})")
                    time.sleep(self.DELAY + random.uniform(0.5, 2.0))

                except Exception as e:
                    logger.error(f"[Indeed] Error on '{query}' page {page_num+1}: {e}")
                    break

        logger.info(f"[Indeed] Scrape complete: {len(all_jobs)} total jobs")
        return all_jobs

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=3, max=15),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def _scrape_search(self, query: str, location: str, start: int) -> list[dict]:
        """Scrape a single search results page."""
        params = {
            "q": query or "emploi",
            "l": location,
            "start": start,
            "fromage": "7",    # Last 7 days only
            "lang": "fr_MA",
        }

        self.session.headers["User-Agent"] = self._get_random_ua()
        response = self.session.get(self.SEARCH_URL, params=params, timeout=20)
        response.raise_for_status()

        if len(response.text) < 1000:
            logger.warning("[Indeed] Short response, possible block")
            return []

        soup = BeautifulSoup(response.text, "lxml")
        return self._parse_listing_page(soup)

    def _parse_listing_page(self, soup: BeautifulSoup) -> list[dict]:
        """Parse Indeed's HTML listing page."""
        jobs = []
        now = datetime.utcnow().isoformat()

        # Indeed uses data-jk attribute for job keys
        job_cards = (
            soup.find_all("div", class_=lambda c: c and "job_seen_beacon" in str(c)) or
            soup.find_all("div", attrs={"data-jk": True}) or
            soup.find_all("td", class_="resultContent")
        )

        for card in job_cards:
            try:
                # ── Title ──────────────────────────────────────────
                title_tag = (
                    card.find("h2", class_=lambda c: c and "jobTitle" in str(c)) or
                    card.find("a", attrs={"data-jk": True})
                )
                if not title_tag:
                    continue

                title_inner = title_tag.find("span")
                title = (title_inner or title_tag).get_text(strip=True)

                # ── URL ────────────────────────────────────────────
                link_tag = card.find("a", attrs={"data-jk": True})
                if not link_tag:
                    link_tag = card.find("a", href=lambda h: h and "/rc/clk" in str(h))
                if not link_tag:
                    continue

                job_key = link_tag.get("data-jk", "")
                href = link_tag.get("href", "")
                if job_key:
                    full_url = f"{self.BASE_URL}/rc/clk?jk={job_key}"
                elif href:
                    full_url = self.BASE_URL + href if href.startswith("/") else href
                else:
                    continue

                # ── Company ────────────────────────────────────────
                company_tag = (
                    card.find("span", class_=lambda c: c and "companyName" in str(c)) or
                    card.find("a", attrs={"data-tn-element": "companyName"})
                )
                company = company_tag.get_text(strip=True) if company_tag else None

                # ── City ───────────────────────────────────────────
                city_tag = (
                    card.find("div", class_=lambda c: c and "companyLocation" in str(c)) or
                    card.find("span", class_="companyLocation")
                )
                city = city_tag.get_text(strip=True) if city_tag else None

                # ── Salary ─────────────────────────────────────────
                salary_tag = card.find("div", class_=lambda c: c and "salary-snippet" in str(c).lower())
                salary = salary_tag.get_text(strip=True) if salary_tag else None

                # ── Date posted ────────────────────────────────────
                date_tag = card.find("span", class_=lambda c: c and "date" in str(c).lower())
                date_posted = date_tag.get_text(strip=True) if date_tag else None

                # ── Description snippet ────────────────────────────
                desc_tag = card.find("div", class_=lambda c: c and "summary" in str(c).lower())
                description = desc_tag.get_text(strip=True) if desc_tag else None

                uid = hashlib.md5(full_url.encode()).hexdigest()

                jobs.append({
                    "uid": uid,
                    "title": title,
                    "company": company,
                    "city": city,
                    "sector": None,
                    "contract_type": None,
                    "salary": salary,
                    "date_posted": date_posted,
                    "url": full_url,
                    "description": description,
                    "source": self.SOURCE,
                    "scraped_at": now,
                    "pipeline_run_id": None,
                })

            except Exception as e:
                logger.warning(f"[Indeed] Error parsing card: {e}")
                continue

        return jobs

    def _get_headers(self) -> dict:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-MA,fr;q=0.9,en-US;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://ma.indeed.com/",
        }

    def _get_random_ua(self) -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
        ]
        return random.choice(agents)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = IndeedScraper()
    jobs = scraper.scrape()
    print(f"Scraped {len(jobs)} jobs from Indeed")
    if jobs:
        print(json.dumps(jobs[0], indent=2, ensure_ascii=False))
