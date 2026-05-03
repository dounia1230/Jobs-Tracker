"""
============================================================
Moroccan Job Market Analyzer — Rekrute.com Scraper
============================================================
Scrapes job postings from rekrute.com, the largest Moroccan
job board. Handles pagination, rate limiting, and error recovery.

Output:
- Sends each job to Kafka topic 'jobs-raw'
- Saves batch to MinIO bronze/rekrute/{date}/{timestamp}.json
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

from base_scraper import BaseScraper, JobPosting

logger = logging.getLogger(__name__)


class RekruteScraper(BaseScraper):
    """
    Scraper for rekrute.com — Morocco's largest job board.

    Rekrute uses server-side rendering, so standard HTML parsing works.
    Jobs are listed at: https://www.rekrute.com/offres.html?p={page}
    """

    SOURCE = "rekrute"
    BASE_URL = "https://www.rekrute.com"
    LISTING_URL = "https://www.rekrute.com/offres.html"
    MAX_PAGES = int(os.getenv("SCRAPER_MAX_PAGES", "5"))
    DELAY = float(os.getenv("SCRAPER_DELAY", "2.0"))

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update(self._get_headers())

    def scrape(self) -> list[dict]:
        """
        Main entry point. Iterates through all listing pages.
        Returns a list of raw job dictionaries.
        """
        all_jobs = []
        logger.info(f"[Rekrute] Starting scrape — max {self.MAX_PAGES} pages")

        for page_num in range(1, self.MAX_PAGES + 1):
            try:
                jobs_on_page = list(self._scrape_page(page_num))
                if not jobs_on_page:
                    logger.info(f"[Rekrute] No jobs found on page {page_num}, stopping pagination")
                    break

                all_jobs.extend(jobs_on_page)
                logger.info(f"[Rekrute] Page {page_num}: found {len(jobs_on_page)} jobs (total: {len(all_jobs)})")

                # Respectful rate limiting with jitter
                delay = self.DELAY + random.uniform(0.5, 1.5)
                time.sleep(delay)

            except Exception as e:
                logger.error(f"[Rekrute] Error on page {page_num}: {e}")
                continue  # Skip bad pages, continue scraping

        logger.info(f"[Rekrute] Scrape complete: {len(all_jobs)} total jobs")
        return all_jobs

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a URL and return parsed BeautifulSoup object."""
        # Rotate user agent on each request
        self.session.headers.update({"User-Agent": self._get_random_ua()})

        response = self.session.get(url, timeout=15)
        response.raise_for_status()

        # Detect if we got blocked (rekrute returns 200 with CAPTCHA page)
        if "captcha" in response.text.lower() or len(response.text) < 500:
            logger.warning(f"[Rekrute] Possible block detected at {url}")
            time.sleep(10)  # Back off
            return None

        return BeautifulSoup(response.text, "lxml")

    def _scrape_page(self, page_num: int) -> Generator[dict, None, None]:
        """
        Scrapes a single listing page and yields job dicts.
        Rekrute listing URL: /offres.html?p={page}
        """
        url = f"{self.LISTING_URL}?p={page_num}"
        soup = self._fetch_page(url)

        if soup is None:
            return

        # Rekrute currently wraps listings in <li class="post-id">.
        job_cards = soup.select("ul.job-list li.post-id")

        if not job_cards:
            job_cards = [
                link.find_parent("li") or link.find_parent("div") or link.parent
                for link in soup.select("a.titreJob")
            ]

        for card in job_cards:
            job = self._parse_job_card(card)
            if job:
                yield job

    def _parse_job_card(self, card) -> Optional[dict]:
        """
        Parses a single job card HTML element into a raw job dict.
        Returns None if card is invalid or essential data is missing.
        """
        try:
            now = datetime.utcnow().isoformat()

            # ── Title and URL ──────────────────────────────────────
            title_tag = (
                card.find("a", class_="titreJob") or
                card.find("h3", class_="titreJob") or
                card.find("a", href=lambda h: h and "/offre-" in h)
            )
            if not title_tag:
                return None

            raw_title = title_tag.get_text(" ", strip=True)
            relative_url = title_tag.get("href", "")
            if not relative_url or not raw_title:
                return None

            full_url = self.BASE_URL + relative_url if relative_url.startswith("/") else relative_url
            title_parts = [part.strip() for part in raw_title.split("|")]
            title = title_parts[0]
            city_from_title = title_parts[1].replace("(Maroc)", "").strip() if len(title_parts) > 1 else None
            card_text = card.get_text(" ", strip=True)

            # ── Company ───────────────────────────────────────────
            company_tag = card.find("a", class_="nameCompany") or card.find("span", class_="company")
            company = company_tag.get_text(" ", strip=True) if company_tag else None
            if not company:
                company_match = re.search(r"\|\s*(.*?)\s+recrute\b", card_text, re.IGNORECASE)
                company = company_match.group(1).strip() if company_match else None

            # ── City ──────────────────────────────────────────────
            city_tag = card.find("span", class_="city") or card.find("li", class_="location")
            city = city_tag.get_text(" ", strip=True) if city_tag else city_from_title

            # ── Date posted ───────────────────────────────────────
            date_tag = card.find("span", class_="date") or card.find("li", class_="date")
            date_posted = date_tag.get_text(" ", strip=True) if date_tag else None
            if not date_posted:
                date_match = re.search(r"Publication\s*:\s*du\s*([0-9/]+)", card_text, re.IGNORECASE)
                date_posted = date_match.group(1) if date_match else None

            # ── Contract type ─────────────────────────────────────
            contract_tag = card.find("li", class_="contrat") or card.find("span", class_="type")
            contract_type = contract_tag.get_text(" ", strip=True) if contract_tag else None
            if not contract_type:
                contract_match = re.search(r"Type de contrat propos[ée]\s*:\s*([A-Za-z]+)", card_text, re.IGNORECASE)
                contract_type = contract_match.group(1).upper() if contract_match else None

            # ── Sector ────────────────────────────────────────────
            sector_tag = card.find("li", class_="sector")
            sector = sector_tag.get_text(" ", strip=True) if sector_tag else None
            if not sector:
                sector_match = re.search(r"Secteur d'activit[ée]\s*:\s*(.*?)\s+Fonction\s*:", card_text, re.IGNORECASE)
                sector = sector_match.group(1).strip() if sector_match else None

            # ── Generate a stable unique ID ───────────────────────
            uid = hashlib.md5(full_url.encode()).hexdigest()

            return {
                "uid": uid,
                "title": title,
                "company": company,
                "city": city,
                "sector": sector,
                "contract_type": contract_type,
                "salary": None,          # Rekrute rarely shows salary on listing
                "date_posted": date_posted,
                "url": full_url,
                "source": self.SOURCE,
                "description": card_text,
                "scraped_at": now,
                "pipeline_run_id": None  # Set by Airflow
            }

        except Exception as e:
            logger.warning(f"[Rekrute] Error parsing card: {e}")
            return None

    def _get_headers(self) -> dict:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    def _get_random_ua(self) -> str:
        """Returns a random browser User-Agent string."""
        agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        ]
        return random.choice(agents)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = RekruteScraper()
    jobs = scraper.scrape()
    print(f"Scraped {len(jobs)} jobs from Rekrute")
    if jobs:
        print(json.dumps(jobs[0], indent=2, ensure_ascii=False))
