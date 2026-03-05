from pathlib import Path

# Project root (…/sap-rename-toolkit)
ROOT_DIR = Path(__file__).resolve().parents[2]

# Single mappings directory
MAPPINGS_DIR = ROOT_DIR / "mappings"

def ensure_dirs():
    MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)