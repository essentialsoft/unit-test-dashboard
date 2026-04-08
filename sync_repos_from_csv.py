#!/usr/bin/env python3
"""
Load program, project, and repo from projects-mapping.csv and write to repositories.yml.
The YAML is built entirely from the CSV—no existing YAML data is used.
"""

import csv
import re
from pathlib import Path

import yaml

SCRIPT_DIR = Path(__file__).parent
CSV_FILE = SCRIPT_DIR / "projects-mapping.csv"
REPOS_FILE = SCRIPT_DIR / "repositories.yml"


def parse_csv_repos(csv_path: Path) -> list[dict]:
    """Parse projects-mapping.csv and return list of {name, program, project} per unique repo."""
    seen: set[str] = set()
    repos: list[dict] = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            program = (row.get("Program") or "").strip()
            project = (row.get("Project") or "").strip()
            url = (row.get("Repository") or row.get("Repository ") or "").strip()
            if not url or "github.com" not in url:
                continue
            match = re.search(r"github\.com/[^/]+/([^/]+?)(?:/tree/|$)", url, re.I)
            repo_name = match.group(1) if match else None
            if not repo_name:
                continue
            key = repo_name.lower()
            if key in seen:
                continue
            seen.add(key)
            repos.append({
                "name": repo_name,
                "program": program,
                "project": project,
            })
    return repos


def main():
    if not CSV_FILE.exists():
        print(f"CSV file not found: {CSV_FILE}")
        return

    repos = parse_csv_repos(CSV_FILE)
    data = {"repositories": repos}

    with open(REPOS_FILE, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"Wrote {len(repos)} repositories to {REPOS_FILE} from {CSV_FILE}")


if __name__ == "__main__":
    main()
