# run_api.py — root shim; real entry-point lives in api/run_api.py
from api.run_api import main  # noqa: F401
if __name__ == "__main__":
    main()
