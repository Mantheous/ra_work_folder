"""
paths.py — Project root resolver.

This module provides the canonical path to ra_work_folder so that any script
in the project can import it and build absolute paths without hard-coding the
W: drive or any other machine-specific prefix.

Usage:
    from Utilities.paths import PROJECT_ROOT

    csv_path = PROJECT_ROOT / "Civil_Status" / "Cher" / "cher_communes.csv"
"""

from pathlib import Path

# This file lives at <project_root>/Utilities/paths.py, so two .parent calls
# walk up to the project root.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
