import sys
from pathlib import Path

# Ensure project root (containing app.py) is importable as module 'app'
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
