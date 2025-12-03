import requests
import json
import os
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

REPO_OWNER = "Scytale-exercise"
REPO_NAME = "Scytale_repo"

GITHUB_API_URL = "https://api.github.com"
GITHUB_TOKEN = "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"

def get_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Scytale-Exercise-Script"
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    return headers

def handle_rate_limit(response: requests.Response) -> None:
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    if remaining == 0:
        reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
        sleep_time = max(0, reset_time - int(time.time())) + 1
        logger.warning(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
        time.sleep(sleep_time)

def make_github_request(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    url = f"{GITHUB_API_URL}{endpoint}"
    try:
        response = requests.get(url, headers=get_headers(), params=params)
        handle_rate_limit(response)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

def fetch_all_pages(endpoint: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    all_data = []
    url = f"{GITHUB_API_URL}{endpoint}"
    current_params = params.copy() if params else {}
    current_params["per_page"] = 100
    current_params["page"] = 1

    while True:
        try:
            response = requests.get(url, headers=get_headers(), params=current_params)
            handle_rate_limit(response)
            response.raise_for_status()

            data = response.json()
            if not data:
                break

            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)

            if "next" not in response.links:
                break

            current_params["page"] += 1

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {current_params.get('page')} for {url}: {e}")
            break

    return all_data

def save_data(data: Any) -> str:
    os.makedirs("data", exist_ok=True)
    filename = f"compliance_report_{time.time() * 1000}.json"
    filepath = os.path.join("data", filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Data saved to {filepath}")
    except IOError as e:
        logger.error(f"Failed to save data to {filepath}: {e}")

    return filepath

def fetch_pr_data(**kwargs: Any) -> str:
    logger.info(f"Fetching data for {REPO_OWNER}/{REPO_NAME}...")

    prs = fetch_all_pages(f"/repos/{REPO_OWNER}/{REPO_NAME}/pulls", params={"state": "all"})

    if not prs:
        logger.warning("No PRs found or error occurred.")
        return ""

    all_pr_data: List[Dict[str, Any]] = []

    for pr in prs:
        pr_number = pr.get("number")
        logger.info(f"Processing PR #{pr_number}")

        pr_info = {
            "metadata": {
                "number": pr_number,
                "title": pr.get("title"),
                "state": pr.get("state"),
                "merged_at": pr.get("merged_at"),
                "author": {
                    "login": pr.get("user", {}).get("login") if pr.get("user") else None,
                    "id": pr.get("user", {}).get("id") if pr.get("user") else None
                },
                "branches": {
                    "base": pr.get("base", {}).get("ref") if pr.get("base") else None,
                    "head": pr.get("head", {}).get("ref") if pr.get("head") else None
                }
            }
        }

        reviews = fetch_all_pages(f"/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{pr_number}/reviews")
        review_data = []
        approved_count = 0

        if reviews:
            for review in reviews:
                state = review.get("state")
                review_data.append({
                    "id": review.get("id"),
                    "user": review.get("user", {}).get("login") if review.get("user") else None,
                    "state": state
                })
                if state == "APPROVED":
                    approved_count += 1

        pr_info["review_info"] = {
            "reviews": review_data,
            "approved_count": approved_count
        }

        commits = fetch_all_pages(f"/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{pr_number}/commits")
        commit_data = []
        if commits:
            for commit in commits:
                commit_data.append({
                    "sha": commit.get("sha"),
                    "author": commit.get("commit", {}).get("author")
                })

        pr_info["commits"] = {
            "count": len(commit_data),
            "list": commit_data
        }

        head_sha = pr.get("head", {}).get("sha") if pr.get("head") else None
        status_info = {}

        if head_sha:
            status_resp = make_github_request(f"/repos/{REPO_OWNER}/{REPO_NAME}/commits/{head_sha}/status")
            if status_resp:
                check_runs = []
                for status in status_resp.get("statuses", []):
                    check_runs.append({
                        "name": status.get("context"),
                        "conclusion": status.get("state"),
                        "completed_at": status.get("updated_at")
                    })

                status_info = {
                    "combined_state": status_resp.get("state"),
                    "checks": check_runs
                }

        pr_info["status_checks"] = status_info

        all_pr_data.append(pr_info)

    return save_data(all_pr_data)