"""Google Cloud Function entry points.

These must be in main.py in same directory as requirements.txt and cannot be nested
inside another package.
"""

from probe_scraper.glean_push import main as glean_push

__all__ = ["glean_push"]
