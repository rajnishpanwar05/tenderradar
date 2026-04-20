# database/__init__.py — re-export all public symbols from database.db
# Allows both `from database import X` and `from database.db import X` to work.
from database.db import *  # noqa: F401, F403
