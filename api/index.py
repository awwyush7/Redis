import sys
from pathlib import Path

# Vercel runs from /var/task; make the repo root importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from web.app import app  # noqa: F401  (Vercel picks up `app` from this module)
