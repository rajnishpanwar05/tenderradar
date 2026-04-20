# config/__init__.py — re-export everything from config.config
# Allows both `from config import X` and `from config.config import X` to work.
from config.config import *  # noqa: F401, F403
