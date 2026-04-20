# scrapers/portals/ — One file per portal, inherits from BaseScraper subclass.
#
# Naming convention: {portal_name}.py  (lowercase, underscores)
# Each file exports a single class: class {PortalName}Scraper(...)
#
# To register a new portal:
#   1. Create scrapers/portals/{name}.py
#   2. Add selectors to config/selectors/{name}.yaml
#   3. Register in main.py with --flag argument
