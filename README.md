# Greenpeace USA Campaign Scraper

This repository contains a small scraper that crawls Greenpeace USA issue pages and extracts companies targeted by campaigns (toxics, oceans, climate, plastics, industrial pollution).

Usage
-----

1. Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Set the Firecrawl API key in your shell:

```bash
export FIRECRAWL_API_KEY="your_key_here"
```

3. Run the script:

```bash
python3 greenpeace_scraper-2.py
```

Notes
-----
- The script reads the API key from the `FIRECRAWL_API_KEY` environment variable; there are no keys hardcoded in the repository.
- Review `.gitignore` to keep secrets out of commits. If you accidentally commit a key, rotate it immediately and remove from git history.

Files
-----
- `greenpeace_scraper-2.py`: Main scraper script (uses `firecrawl` client).
- `requirements.txt`: Python dependencies.
- `.gitignore`: Ignored files to keep sensitive/stale files out of the repo.

Next steps
----------
- Provide the GitHub remote URL if you want me to add it and push the `main` branch.

Latest
------
- Repository pushed to: https://github.com/tanaydangaich/detox_campaign_fashion
- Local commit: `bc14704` (Add Greenpeace scraper and docs)

