#!/usr/bin/env python3
"""
Retrieve unit test coverage data from Coveralls API for all repos in repositories.yml
and export to CSV for dashboard import (e.g., Looker Studio).
"""

import csv
import json
import re
import urllib.error
import urllib.request
from pathlib import Path

import yaml


# Default GitHub org for repos - modify if your repos use a different org
DEFAULT_ORG = "CBIIT"

# Branches included in export: main, master, or semver-style names (e.g. 3.6.0, v1.2.3).
_VERSION_BRANCH = re.compile(r"^v?\d+(\.\d+)+$")


def is_allowed_branch(branch: str) -> bool:
    if not branch or not branch.strip():
        return False
    name = branch.strip()
    if name in ("main", "master"):
        return True
    return bool(_VERSION_BRANCH.fullmatch(name))


# Paths
SCRIPT_DIR = Path(__file__).parent
REPOS_FILE = SCRIPT_DIR / "repositories.yml"
OUTPUT_CSV = f'{SCRIPT_DIR}/data/unit_test_coverage.csv'


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
        # Coveralls reports 0–100; store as 0–1 for dashboard (e.g. 82 -> 0.82).
        "covered_percent": round(num(build.get("covered_percent")) / 100, 4),
        "covered_lines": num(build.get("covered_lines")),
        "missed_lines": num(build.get("missed_lines")),
        "total_lines": num(build.get("relevant_lines")),
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
        kept = 0
        for build in builds:
            if not is_allowed_branch(build.get("branch") or ""):
                continue
            rows.append(extract_row(name, build, program, project))
            kept += 1
        if builds:
            print(f"  Retrieved {len(builds)} builds, {kept} on allowed branches")

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
