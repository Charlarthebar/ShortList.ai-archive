#!/usr/bin/env python3
"""
Mackinac Center Michigan Salary Scraper
=======================================

Scrapes Michigan government and education employee salary data from
the Mackinac Center's public transparency database.

Author: ShortList.ai
Date: 2026-01-15
"""

import os
import sys
import time
import csv
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mackinac_scrape.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

BASE_URL = 'https://www.mackinac.org/salaries'
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'michigan')


def fetch_page(report: str, page: int, retries: int = 5) -> Optional[List[Dict]]:
    """Fetch a single page of salary data."""
    params = {
        'report': report,
        'sort': 'wage2024-desc',
        'columns': 'recent',
        'page': str(page)
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }

    for attempt in range(retries):
        try:
            resp = requests.get(BASE_URL, params=params, headers=headers, timeout=30)

            # Handle rate limiting specifically
            if resp.status_code == 429:
                wait_time = min(60, 5 * (attempt + 1))
                log.warning(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('tbody')

            if not table:
                return []

            rows = table.find_all('tr')
            results = []

            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    name = cells[0].get_text(strip=True)
                    employer = cells[1].get_text(strip=True)
                    position = cells[2].get_text(strip=True)

                    # Skip placeholder rows
                    if not name or name == ',' or not any(c.isalpha() for c in name):
                        continue

                    # Get wages (remaining cells)
                    wages = []
                    for cell in cells[3:]:
                        wage_text = cell.get_text(strip=True)
                        wage_text = wage_text.replace(',', '').replace('$', '')
                        if wage_text and wage_text != 'n/a':
                            try:
                                wages.append(float(wage_text))
                            except ValueError:
                                wages.append(None)
                        else:
                            wages.append(None)

                    # Use most recent non-null wage as salary
                    salary = None
                    for w in wages:
                        if w and w > 0:
                            salary = w
                            break

                    if salary and salary >= 15000:
                        results.append({
                            'name': name,
                            'employer': employer,
                            'position': position,
                            'salary': salary,
                            'wages': wages
                        })

            return results

        except requests.exceptions.HTTPError as e:
            if '429' in str(e):
                wait_time = min(60, 5 * (attempt + 1))
                log.warning(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                log.warning(f"  Attempt {attempt+1} failed for page {page}: {e}")
                time.sleep(2 ** attempt)
        except Exception as e:
            log.warning(f"  Attempt {attempt+1} failed for page {page}: {e}")
            time.sleep(2 ** attempt)

    return None


def scrape_report(report: str, max_pages: int, output_file: str):
    """Scrape all pages for a report type."""
    log.info(f"\n{'='*60}")
    log.info(f"Scraping {report} data (up to {max_pages} pages)")
    log.info(f"{'='*60}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_records = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        records = fetch_page(report, page)

        if records is None:
            log.error(f"  Failed to fetch page {page}, stopping")
            break

        if len(records) == 0:
            empty_streak += 1
            if empty_streak >= 3:
                log.info(f"  3 empty pages in a row, stopping at page {page}")
                break
        else:
            empty_streak = 0
            all_records.extend(records)

        if page % 50 == 0:
            log.info(f"  Page {page}: {len(all_records):,} records so far")

        # Be very polite - 2s between requests to avoid rate limiting
        time.sleep(2)

    # Write to CSV
    output_path = os.path.join(OUTPUT_DIR, output_file)
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'employer', 'position', 'salary'])
        writer.writeheader()
        for record in all_records:
            writer.writerow({
                'name': record['name'],
                'employer': record['employer'],
                'position': record['position'],
                'salary': record['salary']
            })

    log.info(f"  Completed: {len(all_records):,} records written to {output_file}")
    return len(all_records)


def main():
    log.info("=" * 60)
    log.info("MACKINAC CENTER MICHIGAN SALARY SCRAPER")
    log.info("=" * 60)

    total = 0

    # Scrape state employees
    count = scrape_report('state', max_pages=800, output_file='michigan_state.csv')
    total += count

    # Scrape education employees
    count = scrape_report('education', max_pages=6000, output_file='michigan_education.csv')
    total += count

    log.info(f"\n{'='*60}")
    log.info(f"SCRAPING COMPLETE")
    log.info(f"{'='*60}")
    log.info(f"Total records: {total:,}")


if __name__ == "__main__":
    main()
