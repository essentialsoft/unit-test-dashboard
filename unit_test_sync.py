#!/usr/bin/env python3
"""
Retrieve unit test coverage data from Coveralls API for all repos in repositories.yml
and export to CSV for dashboard import (e.g., Looker Studio).
"""

import csv
import json
import urllib.error
import urllib.request
from pathlib import Path

import yaml


# Default GitHub org for repos - modify if your repos use a different org
DEFAULT_ORG = "CBIIT"

# Paths
SCRIPT_DIR = Path(__file__).parent
REPOS_FILE = SCRIPT_DIR / "repositories.yml"
OUTPUT_CSV = SCRIPT_DIR / "unit_test_coverage.csv"


def load_repositories() -> list[dict]:
    """Load repository list from repositories.yml."""
    with open(REPOS_FILE) as f:
        data = yaml.safe_load(f)
    return data.get("repositories", [])


def fetch_coverage_page(org: str, repo_name: str, page: int = 1) -> dict | None:
    """Fetch a single page of coverage builds from Coveralls API. Returns None on failure."""
    url = f"https://coveralls.io/github/{org}/{repo_name}.json?page={page}"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"  Warning: {repo_name} page {page} - HTTP {e.code} ({e.reason})")
        return None
    except urllib.error.URLError as e:
        print(f"  Warning: {repo_name} page {page} - {e.reason}")
        return None
    except Exception as e:
        print(f"  Warning: {repo_name} page {page} - {e}")
        return None


def fetch_all_coverage_builds(org: str, repo_name: str) -> list[dict]:
    """Fetch entire build history for a repo (all pages). Returns list of build dicts."""
    builds = []
    page = 1
    while True:
        data = fetch_coverage_page(org, repo_name, page)
        if not data:
            break
        page_builds = data.get("builds", [])
        if not page_builds:
            break
        builds.extend(page_builds)
        total_pages = data.get("pages", 1)
        if page >= total_pages:
            break
        page += 1
    return builds


def extract_row(repo_name: str, build: dict, program: str = "", project: str = "") -> dict:
    """Extract dashboard-relevant fields from a single build in Coveralls API response."""
    def num(val, default=0):
        return default if val is None else val

    return {
        "build_id": build.get("id") or "",
        "repo_name": repo_name,
        "program": program,
        "project": project,
        "full_repo": build.get("repo_name") or "",
        "branch": build.get("branch") or "",
        "covered_percent": round(num(build.get("covered_percent")), 2),
        "covered_lines": num(build.get("covered_lines")),
        "missed_lines": num(build.get("missed_lines")),
        "relevant_lines": num(build.get("relevant_lines")),
        "covered_branches": num(build.get("covered_branches")),
        "missed_branches": num(build.get("missed_branches")),
        "relevant_branches": num(build.get("relevant_branches")),
        "coverage_change": build.get("coverage_change"),
        "commit_sha": build.get("commit_sha", ""),
        "commit_message": (build.get("commit_message", "") or "")[:200],
        "calculated_at": build.get("calculated_at", ""),
        "url": build.get("url", ""),
    }


def main():
    repos = load_repositories()
    if not repos:
        print("No repositories found in repositories.yml")
        return

    # Support optional org per repo, fallback to default
    org = DEFAULT_ORG
    rows = []
    for repo in repos:
        name = repo.get("name")
        if not name:
            continue
        repo_org = repo.get("org", org)
        print(f"Fetching {repo_org}/{name}...")
        builds = fetch_all_coverage_builds(repo_org, name)
        program = repo.get("program", "") or ""
        project = repo.get("project", "") or ""
        for build in builds:
            rows.append(extract_row(name, build, program, project))
        if builds:
            print(f"  Retrieved {len(builds)} builds")

    if not rows:
        print("No coverage data retrieved. Check repo names and org.")
        return

    # Write CSV
    fieldnames = list(rows[0].keys())
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
