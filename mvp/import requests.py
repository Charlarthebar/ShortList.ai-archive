import requests
import pandas as pd
from time import sleep

# =========================
# CONFIG
# =========================

ADZUNA_APP_ID = "516145e7"
ADZUNA_APP_KEY = "34e6f3959d58461538354d674d48b881"

USAJOBS_API_KEY = "Y45Z7Gortr/kTzQhJgxHjDoY16NrzYFTY4IXgMbxs0o="
USAJOBS_EMAIL = "charlielai3@gmail.com"

ZIP_CODE = "02906"
RADIUS_MILES = 50
RESULTS_PER_PAGE = 50

# =========================
# ADZUNA JOBS
# =========================

def fetch_adzuna_jobs(zip_code, pages=2):
    jobs = []

    for page in range(1, pages + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "where": zip_code,
            "distance": RADIUS_MILES,
            "results_per_page": RESULTS_PER_PAGE,
            "content-type": "application/json"
        }

        r = requests.get(url, params=params)
        data = r.json()

        for j in data.get("results", []):
            jobs.append({
                "job_id": j.get("id"),
                "title": j.get("title"),
                "employer": j.get("company", {}).get("display_name"),
                "sector": "private",
                "source": "adzuna",
                "salary_min": j.get("salary_min"),
                "salary_max": j.get("salary_max"),
                "location": j.get("location", {}).get("display_name"),
                "zip": zip_code,
                "remote": "remote" in j.get("title", "").lower(),
                "posted_date": j.get("created"),
                "url": j.get("redirect_url")
            })

        sleep(1)  # rate-limit safety

    return jobs


# =========================
# USAJOBS (FEDERAL)
# =========================

def fetch_usajobs(zip_code, pages=3):
    jobs = []
    headers = {
        "Authorization-Key": USAJOBS_API_KEY,
        "User-Agent": USAJOBS_EMAIL
    }

    for page in range(1, pages + 1):
        params = {
            "LocationName": zip_code,
            "Radius": RADIUS_MILES,
            "ResultsPerPage": RESULTS_PER_PAGE,
            "Page": page
        }

        r = requests.get(
            "https://data.usajobs.gov/api/search",
            headers=headers,
            params=params
        )

        data = r.json()
        items = data.get("SearchResult", {}).get("SearchResultItems", [])

        for item in items:
            job = item["MatchedObjectDescriptor"]
            jobs.append({
                "job_id": job.get("PositionID"),
                "title": job.get("PositionTitle"),
                "employer": job.get("OrganizationName"),
                "sector": "federal",
                "source": "usajobs",
                "salary_min": job.get("PositionRemuneration", [{}])[0].get("MinimumRange"),
                "salary_max": job.get("PositionRemuneration", [{}])[0].get("MaximumRange"),
                "location": job.get("PositionLocation", [{}])[0].get("LocationName"),
                "zip": zip_code,
                "remote": job.get("RemoteIndicator"),
                "posted_date": job.get("PublicationStartDate"),
                "url": job.get("PositionURI")
            })

        sleep(1)

    return jobs


# =========================
# RUN PIPELINE
# =========================

def run_pipeline():
    adzuna_jobs = fetch_adzuna_jobs(ZIP_CODE)
    usajobs = fetch_usajobs(ZIP_CODE)

    all_jobs = adzuna_jobs + usajobs
    df = pd.DataFrame(all_jobs)

    df.to_csv(f"jobs_{ZIP_CODE}.csv", index=False)
    print(f"Saved {len(df)} jobs to jobs_{ZIP_CODE}.csv")

    return df


if __name__ == "__main__":
    run_pipeline()
