"""
Convenience script – runs without installing the package.

Usage (from repo root):
    python scripts/fetch_address.py "Keizersgracht 123, 1015 CJ Amsterdam"
"""

import sys
from pathlib import Path

# Allow running from repo root without pip install
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from watmoetikbieden.cli import main  # noqa: E402

if __name__ == "__main__":
    main()
