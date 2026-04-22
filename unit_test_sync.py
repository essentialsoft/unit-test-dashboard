#!/usr/bin/env python3
"""
Retrieve unit test coverage data from Coveralls API for all repos in repositories.yml
and export to CSV for dashboard import (e.g., Looker Studio).

Only includes builds with calculated_at on or after 2026-01-01 in US Eastern (see MIN_BUILD_CUTOFF).

Per repository, only the last build of each calendar month (US Eastern) is kept. Months with no build from
the first month with data through the current Eastern calendar month are filled by copying the prior month's
repo row (same metrics; calculated_at = US Eastern end of that month in UTC, or current UTC time if that
instant is still in the future; year_month is always the filled month).

coverage_change is computed per repo as (current covered_percent − previous month's), not from the API.

Also writes project_coverage.csv and program_coverage.csv: one row per project / program per Eastern
calendar month; covered_percent = sum(covered_lines) / sum(total_lines) in that bucket; coverage_change
is MoM on that ratio.

Each output CSV includes collected_at: the run date in YYYY-MM-DD (US Eastern).

If data/repository_coverage.csv already exists, the script re-reads it. When the file's latest month for a
repo is the current Eastern month, only that month's builds are requested from the API. When the latest month
is earlier, the API is queried for that month through the current month; older months are taken from the file.
"""

import calendar
import copy
import csv
import json
import re
from collections import defaultdict
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml


# Default GitHub org for repos - modify if your repos use a different org
DEFAULT_ORG = "CBIIT"

# Only builds on or after midnight at the start of this calendar date (US Eastern) are exported.
MIN_BUILD_CUTOFF = datetime(2026, 1, 1, tzinfo=ZoneInfo("America/New_York"))

_EASTERN = ZoneInfo("America/New_York")


def eastern_month_start(y: int, m: int) -> datetime:
    """First instant of the given US Eastern calendar month (timezone-aware)."""
    return datetime(y, m, 1, 0, 0, 0, 0, tzinfo=_EASTERN)


def parse_year_month_ym(s: str) -> tuple[int, int] | None:
    """Parse YYYY-MM from a CSV year_month value."""
    if not s or not str(s).strip():
        return None
    raw = str(s).strip()
    if len(raw) < 7 or raw[4] != "-":
        return None
    try:
        y, mo = int(raw[:4]), int(raw[5:7])
    except ValueError:
        return None
    if not (1 <= mo <= 12):
        return None
    return (y, mo)


def current_eastern_year_month() -> tuple[int, int]:
    n = datetime.now(timezone.utc).astimezone(_EASTERN)
    return (n.year, n.month)


@dataclass(frozen=True)
class IncrementalWindow:
    """US Eastern (year, month) range to re-fetch from Coveralls for one repo.

    - oldest: first month to pull from the API (inclusive). None = full backfill to MIN_BUILD_CUTOFF.
    """

    oldest: tuple[int, int] | None = None


def _max_year_month_for_rows(rows: list[dict]) -> tuple[int, int] | None:
    best: tuple[int, int] | None = None
    for row in rows:
        ym = parse_year_month_ym(row.get("year_month") or "")
        if ym is None:
            continue
        if best is None or (ym[0], ym[1]) > (best[0], best[1]):
            best = ym
    return best


def load_existing_repository_rows(path: Path) -> dict[str, list[dict]]:
    """Load prior repository_coverage.csv rows, keyed by repo name. Missing file -> empty."""
    if not path.is_file():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        by_repo: dict[str, list[dict]] = defaultdict(list)
        for row in r:
            name = (row.get("repo_name") or "").strip()
            if not name:
                continue
            by_repo[name].append(dict(row))
    return dict(by_repo)


def refresh_window_for_repo(
    existing_for_repo: list[dict] | None,
) -> tuple[IncrementalWindow, tuple[int, int] | None, tuple[int, int]]:
    """Decide (fetch window, latest existing YM, current Eastern YM).

    - If the repo has no prior data: full fetch; no months are carried over from file.
    - If latest month == current month: fetch only the current month; base = all months before current.
    - If latest < current: fetch from that month through current; base = months before latest.
    - If latest > current (bad data / skew): only refresh the current month; base = months before current.
    """
    now_ym = current_eastern_year_month()
    ex = existing_for_repo
    if not ex:
        return (IncrementalWindow(oldest=None), None, now_ym)
    em = _max_year_month_for_rows(ex)
    if em is None:
        return (IncrementalWindow(oldest=None), None, now_ym)
    ey, eM = em
    cy, cM = now_ym
    if (ey, eM) == (cy, cM):
        return (IncrementalWindow(oldest=now_ym), em, now_ym)
    if (ey, eM) < (cy, cM):
        return (IncrementalWindow(oldest=em), em, now_ym)
    return (IncrementalWindow(oldest=now_ym), em, now_ym)


def existing_rows_before_month(
    existing: list[dict], first_fetched_ym: tuple[int, int]
) -> list[dict]:
    """Rows to keep as-is: US Eastern YYYY-MM strictly before the first month re-fetched from the API."""
    if not existing:
        return []
    y0, m0 = first_fetched_ym
    out: list[dict] = []
    for r in existing:
        ym = parse_year_month_ym(r.get("year_month") or "")
        if ym is not None and (ym[0], ym[1]) < (y0, m0):
            out.append(r)
    return out


# Branches included in export: main, master, or semver-style names (e.g. 3.6.0, v1.2.3).
_VERSION_BRANCH = re.compile(r"^v?\d+(\.\d+)+$")


def parse_calculated_at(raw: str) -> datetime | None:
    """Parse Coveralls calculated_at (ISO 8601); return None if missing or invalid."""
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_allowed_branch(branch: str) -> bool:
    # temporarily allow all branches
    return True
    if not branch or not branch.strip():
        return False
    name = branch.strip()
    if name in ("main", "master"):
        return True
    return bool(_VERSION_BRANCH.fullmatch(name))


def select_last_build_each_month(builds: list[dict]) -> list[dict]:
    """One build per US Eastern calendar month: the latest by calculated_at within that month."""
    tz = ZoneInfo("America/New_York")
    best: dict[tuple[int, int], tuple[dict, datetime]] = {}
    for build in builds:
        ts = parse_calculated_at(build.get("calculated_at") or "")
        if ts is None:
            continue
        local = ts.astimezone(tz)
        key = (local.year, local.month)
        if key not in best or ts > best[key][1]:
            best[key] = (build, ts)
    ordered = sorted(best.values(), key=lambda x: x[1], reverse=True)
    return [b for b, _ in ordered]


# Paths
SCRIPT_DIR = Path(__file__).parent
REPOS_FILE = SCRIPT_DIR / "repositories.yml"
REPO_COVERAGE_CSV = SCRIPT_DIR / "data" / "repository_coverage.csv"
PROJECT_COVERAGE_CSV = SCRIPT_DIR / "data" / "project_coverage.csv"
PROGRAM_COVERAGE_CSV = SCRIPT_DIR / "data" / "program_coverage.csv"


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


def fetch_all_coverage_builds(
    org: str, repo_name: str, *, oldest_eastern_ym: tuple[int, int] | None = None
) -> list[dict]:
    """Fetch build history for a repo (paginated), newest first.

    Stops when a build is before the lower bound: max(MIN_BUILD_CUTOFF, first instant of
    *oldest_eastern_ym* in US Eastern) when *oldest_eastern_ym* is set, otherwise MIN_BUILD_CUTOFF only.
    No further pages are read once that stop triggers.
    """
    if oldest_eastern_ym is not None:
        oy, om = oldest_eastern_ym
        lower_bound = max(MIN_BUILD_CUTOFF, eastern_month_start(oy, om))
    else:
        lower_bound = MIN_BUILD_CUTOFF
    builds = []
    page = 1
    while True:
        data = fetch_coverage_page(org, repo_name, page)
        if not data:
            break
        page_builds = data.get("builds", [])
        if not page_builds:
            break
        for build in page_builds:
            ts = parse_calculated_at(build.get("calculated_at") or "")
            if ts is None:
                continue
            if ts < lower_bound:
                return builds
            builds.append(build)
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
        # Set from calculated_at after gap-fill (US Eastern YYYY-MM).
        "year_month": "",
        # Coveralls reports 0–100; store as 0–1 for dashboard (e.g. 82 -> 0.82).
        "covered_percent": round(num(build.get("covered_percent")) / 100, 4),
        "coverage_change": "",
        "covered_lines": num(build.get("covered_lines")),
        "missed_lines": num(build.get("missed_lines")),
        "total_lines": num(build.get("relevant_lines")),
        "commit_sha": build.get("commit_sha", ""),
        "commit_message": (build.get("commit_message", "") or "")[:200],
        "calculated_at": build.get("calculated_at", ""),
        "url": build.get("url", ""),
    }


def add_coverage_change_vs_prior_month(rows_oldest_first: list[dict]) -> None:
    """Set coverage_change on each row: covered_percent minus previous month (same 0–1 scale). First row -> ''."""
    prev: float | None = None
    for row in rows_oldest_first:
        pct = float(row["covered_percent"])
        if prev is None:
            row["coverage_change"] = ""
        else:
            row["coverage_change"] = round(pct - prev, 4)
        prev = pct


def fill_repo_monthly_gaps(repo_rows: list[dict]) -> list[dict]:
    """Dense US Eastern month series: carry forward the previous row for months with no build.

    Range is from the earliest month with data through the later of (last month with data, current Eastern month).
    Synthetic rows use calculated_at = end of that month (US Eastern) as UTC, unless that time is still in the
    future, in which case use current UTC time. year_month is always set to the month being filled.
    Returns rows oldest-first (by calculated_at for ordering).
    """
    eastern = ZoneInfo("America/New_York")
    if not repo_rows:
        return []

    by_ym: dict[tuple[int, int], dict] = {}
    for row in repo_rows:
        ts = parse_calculated_at(row.get("calculated_at") or "")
        if ts is None:
            continue
        local = ts.astimezone(eastern)
        ym = (local.year, local.month)
        existing = by_ym.get(ym)
        if existing is None or ts > (parse_calculated_at(existing.get("calculated_at") or "") or datetime.min.replace(tzinfo=timezone.utc)):
            by_ym[ym] = row

    if not by_ym:
        return repo_rows

    min_ym = min(by_ym.keys())
    now_eastern = datetime.now(timezone.utc).astimezone(eastern)
    current_ym = (now_eastern.year, now_eastern.month)
    end_ym = max(max(by_ym.keys()), current_ym)

    def next_ym(ym: tuple[int, int]) -> tuple[int, int]:
        y, m = ym
        if m == 12:
            return (y + 1, 1)
        return (y, m + 1)

    def ym_le(a: tuple[int, int], b: tuple[int, int]) -> bool:
        return (a[0], a[1]) <= (b[0], b[1])

    filled: list[dict] = []
    prev_row: dict | None = None
    cur = min_ym
    while ym_le(cur, end_ym):
        if cur in by_ym:
            prev_row = by_ym[cur]
            filled.append(prev_row)
        elif prev_row is not None:
            synth = copy.deepcopy(prev_row)
            y, m = cur
            last_day = calendar.monthrange(y, m)[1]
            at_end_local = datetime(y, m, last_day, 23, 59, 59, tzinfo=eastern)
            at_end_utc = at_end_local.astimezone(timezone.utc)
            now_utc = datetime.now(timezone.utc)
            if at_end_utc > now_utc:
                synth["calculated_at"] = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                synth["calculated_at"] = at_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            synth["year_month"] = f"{y:04d}-{m:02d}"
            prev_row = synth
            filled.append(synth)
        cur = next_ym(cur)

    return filled


def set_repo_row_year_month(row: dict) -> None:
    """US Eastern calendar month as YYYY-MM from calculated_at (gap-filled rows already set year_month)."""
    if (row.get("year_month") or "").strip():
        return
    ts = parse_calculated_at(row.get("calculated_at") or "")
    if ts is None:
        row["year_month"] = ""
        return
    local = ts.astimezone(ZoneInfo("America/New_York"))
    row["year_month"] = f"{local.year:04d}-{local.month:02d}"


def _sort_key_calculated_at(record: dict) -> datetime:
    ts = parse_calculated_at(record.get("calculated_at") or "")
    if ts is not None:
        return ts
    return datetime.min.replace(tzinfo=timezone.utc)


def _repo_row_newest_first_key(row: dict) -> tuple[datetime, str]:
    """Stable newest-first: tie-break synthetic rows that share the same calculated_at (now)."""
    return (_sort_key_calculated_at(row), row.get("year_month") or "")


def _repo_row_group_year_month(row: dict) -> tuple[int, int] | None:
    """(year, month) for rollups: use repo year_month when set, else US Eastern from calculated_at."""
    raw = (row.get("year_month") or "").strip()
    if raw and len(raw) >= 7 and raw[4] == "-":
        try:
            y = int(raw[:4])
            m = int(raw[5:7])
            if 1 <= m <= 12:
                return (y, m)
        except ValueError:
            pass
    ts = parse_calculated_at(row.get("calculated_at") or "")
    if ts is None:
        return None
    local = ts.astimezone(ZoneInfo("America/New_York"))
    return (local.year, local.month)


def build_project_coverage_rows(repo_rows: list[dict]) -> list[dict]:
    """Roll per-repo rows into one row per (project, US Eastern year-month).

    covered_percent = sum(covered_lines) / sum(total_lines). coverage_change is MoM on that ratio.
    """
    groups: dict[tuple[str, int, int], list[dict]] = defaultdict(list)
    for row in repo_rows:
        ym_parts = _repo_row_group_year_month(row)
        if ym_parts is None:
            continue
        y, m = ym_parts
        proj = row.get("project") or ""
        groups[(proj, y, m)].append(row)

    aggregated: list[dict] = []
    for (proj, y, m), bucket in groups.items():
        sum_cov = sum(int(row["covered_lines"]) for row in bucket)
        sum_tot = sum(int(row["total_lines"]) for row in bucket)
        pct = 0.0 if sum_tot == 0 else round(sum_cov / sum_tot, 4)
        programs = sorted({(row.get("program") or "") for row in bucket if (row.get("program") or "").strip()})
        program_str = "; ".join(programs) if programs else ""
        latest_row = max(
            bucket,
            key=lambda r: parse_calculated_at(r.get("calculated_at") or "")
            or datetime.min.replace(tzinfo=timezone.utc),
        )
        aggregated.append(
            {
                "program": program_str,
                "project": proj,
                "year_month": f"{y:04d}-{m:02d}",
                "covered_percent": pct,
                "covered_lines": sum_cov,
                "total_lines": sum_tot,
                "repo_count": len(bucket),
                "calculated_at": latest_row.get("calculated_at", ""),
                "coverage_change": "",
            }
        )

    by_project: dict[str, list[dict]] = defaultdict(list)
    for row in aggregated:
        by_project[row["project"]].append(row)

    for plist in by_project.values():
        plist.sort(key=lambda r: r["year_month"])
        add_coverage_change_vs_prior_month(plist)

    out: list[dict] = []
    for project in sorted(by_project.keys()):
        rows_p = sorted(by_project[project], key=lambda r: r["year_month"], reverse=True)
        out.extend(rows_p)
    return out


def build_program_coverage_rows(repo_rows: list[dict]) -> list[dict]:
    """Roll per-repo rows into one row per (program, US Eastern year-month).

    Same aggregation as project rollup, keyed by program.
    """
    groups: dict[tuple[str, int, int], list[dict]] = defaultdict(list)
    for row in repo_rows:
        ym_parts = _repo_row_group_year_month(row)
        if ym_parts is None:
            continue
        y, m = ym_parts
        prog = row.get("program") or ""
        groups[(prog, y, m)].append(row)

    aggregated: list[dict] = []
    for (prog, y, m), bucket in groups.items():
        sum_cov = sum(int(row["covered_lines"]) for row in bucket)
        sum_tot = sum(int(row["total_lines"]) for row in bucket)
        pct = 0.0 if sum_tot == 0 else round(sum_cov / sum_tot, 4)
        projects = sorted({(row.get("project") or "") for row in bucket if (row.get("project") or "").strip()})
        project_str = "; ".join(projects) if projects else ""
        latest_row = max(
            bucket,
            key=lambda r: parse_calculated_at(r.get("calculated_at") or "")
            or datetime.min.replace(tzinfo=timezone.utc),
        )
        aggregated.append(
            {
                "program": prog,
                "project": project_str,
                "year_month": f"{y:04d}-{m:02d}",
                "covered_percent": pct,
                "covered_lines": sum_cov,
                "total_lines": sum_tot,
                "repo_count": len(bucket),
                "calculated_at": latest_row.get("calculated_at", ""),
                "coverage_change": "",
            }
        )

    by_program: dict[str, list[dict]] = defaultdict(list)
    for row in aggregated:
        by_program[row["program"]].append(row)

    for plist in by_program.values():
        plist.sort(key=lambda r: r["year_month"])
        add_coverage_change_vs_prior_month(plist)

    out: list[dict] = []
    for program in sorted(by_program.keys()):
        rows_p = sorted(by_program[program], key=lambda r: r["year_month"], reverse=True)
        out.extend(rows_p)
    return out


def main():
    repos = load_repositories()
    if not repos:
        print("No repositories found in repositories.yml")
        return

    # Support optional org per repo, fallback to default
    org = DEFAULT_ORG
    existing_by_repo = load_existing_repository_rows(REPO_COVERAGE_CSV)
    rows = []
    for repo in repos:
        name = repo.get("name")
        if not name:
            continue
        repo_org = repo.get("org", org)
        program = repo.get("program", "") or ""
        project = repo.get("project", "") or ""
        window, _latest, _cur = refresh_window_for_repo(existing_by_repo.get(name) or [])
        if window.oldest is not None:
            oy, om = window.oldest
            base = existing_rows_before_month(existing_by_repo.get(name) or [], window.oldest)
            mode = f"incremental from {oy:04d}-{om:02d}"
        else:
            base = []
            mode = "full"
        print(f"Fetching {repo_org}/{name}... ({mode})")
        builds = fetch_all_coverage_builds(repo_org, name, oldest_eastern_ym=window.oldest)
        allowed = [b for b in builds if is_allowed_branch(b.get("branch") or "")]
        monthly = select_last_build_each_month(allowed)
        chrono = sorted(monthly, key=_sort_key_calculated_at)
        from_api = [extract_row(name, b, program, project) for b in chrono]
        with_builds = len(from_api)
        repo_rows = base + from_api
        repo_rows = fill_repo_monthly_gaps(repo_rows)
        for row in repo_rows:
            set_repo_row_year_month(row)
        add_coverage_change_vs_prior_month(repo_rows)
        repo_rows.sort(key=_repo_row_newest_first_key, reverse=True)
        rows.extend(repo_rows)
        if builds or base:
            print(
                f"  {len(builds)} build(s) from API, {len(allowed)} on allowed branches, "
                f"{with_builds} month(s) in range; {len(base)} month(s) from file "
                f"-> {len(repo_rows)} rows after gap-fill"
            )

    if not rows:
        print("No coverage data retrieved. Check repo names and org.")
        return

    collected_at = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    for row in rows:
        row["collected_at"] = collected_at

    # Write repository-level CSV
    REPO_COVERAGE_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(REPO_COVERAGE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {REPO_COVERAGE_CSV}")

    # Project-level rollup (same month key and line sums as repo data)
    project_rows = build_project_coverage_rows(rows)
    if project_rows:
        for r in project_rows:
            r["collected_at"] = collected_at
        pf = list(project_rows[0].keys())
        with open(PROJECT_COVERAGE_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=pf)
            writer.writeheader()
            writer.writerows(project_rows)
        print(f"Wrote {len(project_rows)} rows to {PROJECT_COVERAGE_CSV}")

    program_rows = build_program_coverage_rows(rows)
    if program_rows:
        for r in program_rows:
            r["collected_at"] = collected_at
        pfields = list(program_rows[0].keys())
        with open(PROGRAM_COVERAGE_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=pfields)
            writer.writeheader()
            writer.writerows(program_rows)
        print(f"Wrote {len(program_rows)} rows to {PROGRAM_COVERAGE_CSV}")


if __name__ == "__main__":
    main()
