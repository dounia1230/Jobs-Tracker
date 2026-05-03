"""
============================================================
Moroccan Job Market Analyzer — Emploi.ma Scraper
============================================================
Scrapes job postings from emploi.ma — Morocco's second largest
job board, strong in SME sector jobs.

Output:
- Sends each job to Kafka topic 'jobs-raw'
- Saves batch to MinIO bronze/emploi/{date}/{timestamp}.json
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
from typing import Optional, Generator

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class EmploiScraper(BaseScraper):
    """
    Scraper for emploi.ma — second-largest Moroccan job board.

    Jobs are paginated at:
    https://www.emploi.ma/recherche-jobs-maroc/keyword/all/page/{page}
    """

    SOURCE = "emploi"
    BASE_URL = "https://www.emploi.ma"
    LISTING_URL = "https://www.emploi.ma/recherche-jobs-maroc?page={page}"
    MAX_PAGES = int(os.getenv("SCRAPER_MAX_PAGES", "5"))
    DELAY = float(os.getenv("SCRAPER_DELAY", "2.0"))

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update(self._get_headers())

    def scrape(self) -> list[dict]:
        """Main entry point. Returns list of raw job dicts."""
        all_jobs = []
        logger.info(f"[Emploi.ma] Starting scrape — max {self.MAX_PAGES} pages")

        for page_num in range(1, self.MAX_PAGES + 1):
            try:
                jobs_on_page = list(self._scrape_page(page_num))
                if not jobs_on_page:
                    logger.info(f"[Emploi.ma] No jobs on page {page_num}, stopping")
                    break

                all_jobs.extend(jobs_on_page)
                logger.info(f"[Emploi.ma] Page {page_num}: {len(jobs_on_page)} jobs (total: {len(all_jobs)})")

                # Rate limit: respectful scraping
                time.sleep(self.DELAY + random.uniform(0.3, 1.0))

            except Exception as e:
                logger.error(f"[Emploi.ma] Error on page {page_num}: {e}")
                continue

        logger.info(f"[Emploi.ma] Scrape complete: {len(all_jobs)} total jobs")
        return all_jobs

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse HTML page with retry logic."""
        self.session.headers.update({"User-Agent": self._get_random_ua()})
        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        if len(response.text) < 500:
            logger.warning(f"[Emploi.ma] Suspiciously short response at {url}")
            return None

        return BeautifulSoup(response.text, "lxml")

    def _scrape_page(self, page_num: int) -> Generator[dict, None, None]:
        """Scrape a single page and yield job dicts."""
        url = self.LISTING_URL.format(page=page_num)
        soup = self._fetch_page(url)
        if soup is None:
            return

        job_cards = (
            soup.select("div.card.card-job") or
            soup.find_all("article", class_=lambda c: c and "job" in str(c).lower()) or
            soup.find_all("div", class_=lambda c: c and "job-item" in str(c).lower()) or
            soup.select("div.views-row")
        )

        for card in job_cards:
            job = self._parse_card(card)
            if job:
                yield job

    def _parse_card(self, card) -> Optional[dict]:
        """Parse a single job card HTML element."""
        try:
            now = datetime.utcnow().isoformat()

            # ── Title and URL ──────────────────────────────────────
            title_tag = card.find("h3") or card.find("h2")
            if not title_tag:
                return None

            link_tag = title_tag.find("a") or card.find("a", href=lambda h: h and "/offre" in h)
            title = title_tag.get_text(" ", strip=True)
            relative_url = link_tag.get("href", "") if link_tag else ""
            if not title:
                return None

            full_url = (
                self.BASE_URL + relative_url
                if relative_url.startswith("/")
                else relative_url or f"{self.BASE_URL}/recherche-jobs-maroc#{hashlib.md5(title.encode()).hexdigest()}"
            )
            card_text = card.get_text(" ", strip=True)

            # ── Company ───────────────────────────────────────────
            detail = card.select_one("div.card-job-detail") or card
            lines = [line.strip() for line in detail.get_text("\n", strip=True).splitlines() if line.strip()]
            company = lines[1] if len(lines) > 1 else None

            # ── City / Location ───────────────────────────────────
            location_tag = card.find("li", class_="localisation")
            city = location_tag.get_text(" ", strip=True) if location_tag else None
            if not city and " - " in title:
                city = title.rsplit(" - ", 1)[-1].strip()

            # ── Date posted ───────────────────────────────────────
            date_tag = (
                card.find("span", class_=lambda c: c and "date" in str(c).lower()) or
                card.find("time")
            )
            date_posted = (
                date_tag.get("datetime") or date_tag.get_text(strip=True)
                if date_tag else None
            )

            # ── Contract type ─────────────────────────────────────
            contract_tag = card.find("span", class_=lambda c: c and "contract" in str(c).lower())
            contract_type = contract_tag.get_text(" ", strip=True) if contract_tag else None
            if not contract_type:
                contract_match = re.search(r"Contrat propos\s*:\s*([A-Za-z]+)", card_text, re.IGNORECASE)
                contract_type = contract_match.group(1).upper() if contract_match else None

            # ── Salary ────────────────────────────────────────────
            salary_tag = card.find("span", class_=lambda c: c and "salary" in str(c).lower())
            salary = salary_tag.get_text(strip=True) if salary_tag else None

            # ── Sector ────────────────────────────────────────────
            sector_tag = card.find("span", class_=lambda c: c and "sector" in str(c).lower())
            sector = sector_tag.get_text(strip=True) if sector_tag else None

            # ── Description snippet (from card preview) ───────────
            desc_tag = card.find("div", class_=lambda c: c and "description" in str(c).lower())
            description = desc_tag.get_text(" ", strip=True) if desc_tag else card_text

            uid = hashlib.md5(full_url.encode()).hexdigest()

            return {
                "uid": uid,
                "title": title,
                "company": company,
                "city": city,
                "sector": sector,
                "contract_type": contract_type,
                "salary": salary,
                "date_posted": date_posted,
                "url": full_url,
                "description": description,
                "source": self.SOURCE,
                "scraped_at": now,
                "pipeline_run_id": None
            }

        except Exception as e:
            logger.warning(f"[Emploi.ma] Error parsing card: {e}")
            return None

    def _get_headers(self) -> dict:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-MA,fr;q=0.9,ar;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }

    def _get_random_ua(self) -> str:
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]
        return random.choice(agents)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = EmploiScraper()
    jobs = scraper.scrape()
    print(f"Scraped {len(jobs)} jobs from Emploi.ma")
    if jobs:
        print(json.dumps(jobs[0], indent=2, ensure_ascii=False))
