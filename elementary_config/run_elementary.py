# Runs Elementary after dbt to generate observability report

import subprocess
import sys
from pathlib import Path
from datetime import datetime

SUPPLY_DIR  = Path("supply")
DEMAND_DIR  = Path("demand")
REPORT_DIR  = Path("elementary_config/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def run_elementary(project_dir: Path, domain: str) -> bool:
    print(f"Running Elementary for {domain} domain...")

    # Step 1: run elementary dbt models to collect metadata
    result = subprocess.run(
        ["dbt", "run", "--select", "elementary", "--profiles-dir", "."],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Elementary dbt run failed:\n{result.stderr}")
        return False
    print(f"Elementary models updated")

    # Step 2: generate HTML report
    report_path = REPORT_DIR / f"{domain}_report_{datetime.now().strftime('%Y%m%d')}.html"
    result = subprocess.run(
        [
            "edr", "report",
            "--profiles-dir", ".",
            "--file-path", str(report_path.resolve()),
        ],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Elementary report failed:\n{result.stderr}")
        return False

    print(f"Report saved to {report_path}")
    return True


def main():
    print("Elementary Observability Runner")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    supply_ok = run_elementary(SUPPLY_DIR, "supply")
    demand_ok = run_elementary(DEMAND_DIR, "demand")

    if supply_ok and demand_ok:
        print("All Elementary reports generated")
        print("Open reports in elementary_config/reports/")
        sys.exit(0)
    else:
        print("Some Elementary reports failed — check logs above")
        sys.exit(1)


if __name__ == "__main__":
    main()