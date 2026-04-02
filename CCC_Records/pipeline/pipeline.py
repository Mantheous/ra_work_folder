import subprocess
import sys
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Arguments for search.py
QUERY = '"Kings Mountain" AND "Co 4479"'
RECORD_GROUP = "146"

# Scripts to run in order
PIPELINE_ROOT = Path(__file__).resolve().parent
SCRIPTS = [
    PIPELINE_ROOT / "search.py",
    PIPELINE_ROOT / "text_extractor.py",
    PIPELINE_ROOT / "segmenter.py",
    PIPELINE_ROOT / "nlp" / "gliner_nlp.py",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_script(script_path: Path, args: list[str] | None = None):
    """Run a python script in a subprocess."""
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    log.info("=" * 60)
    log.info("RUNNING: %s %s", script_path.name, " ".join(args) if args else "")
    log.info("=" * 60)
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        log.error("Script %s failed with exit code %d", script_path.name, e.returncode)
        sys.exit(e.returncode)

def main():
    """Execute the full pipeline sequence."""
    log.info("Starting Pipeline Orchestration")
    
    for script in SCRIPTS:
        if not script.exists():
            log.error("Script not found: %s", script)
            sys.exit(1)
        
        # Pass parameters to search.py
        args = []
        if script.name == "search.py":
            args = [QUERY, "--record-group", RECORD_GROUP]
        
        run_script(script, args)
    
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)

if __name__ == "__main__":
    main()