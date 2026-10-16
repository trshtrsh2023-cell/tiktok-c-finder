# TikTok Comment Finder

Finds comments written by `TARGET_USERNAME` (default `i7si_u`) on the videos of the accounts listed in
`followers.csv`, and appends them to a Google Sheet
(`Timestamp | Video URL | Target Username | Comment Text | Likes | Video Owner`).

## 1. Google Sheets credentials
1. Google Cloud Console -> enable **Google Sheets API** + **Google Drive API**.
2. IAM & Admin -> Service Accounts -> create -> Keys -> **Add key -> JSON**.
3. Share your Google Sheet (Editor) with the service account `client_email`.
4. **Never commit the JSON file.** Paste its whole content into a secret / env var `GOOGLE_CREDENTIALS_JSON`.

## 2A. Run on GitHub Actions (`.github/workflows/tiktok_cron.yml`)
Repo -> Settings -> Secrets and variables -> Actions:
- Secrets: `GOOGLE_CREDENTIALS_JSON`, `GOOGLE_SHEET_NAME`, `TARGET_USERNAME` (optional), `PROXY_SERVER/USERNAME/PASSWORD` (optional)
- Variables (optional): `RUN_MINUTES` (default 60, max 330), `CONCURRENCY` (default 2)

Each run scans until the time budget ends, then stops cleanly. `history.db` is saved with the Actions cache,
so the next run **resumes** (and the script also reads the Sheet to avoid duplicates if the cache is lost).
Start it manually from the **Actions** tab -> *Run workflow*. Private repos get ~2,000 free minutes/month.

## 2B. Run on Render (24/7 worker, better for the full followers list)
Background Worker -> Runtime **Docker** (uses `Dockerfile`) -> add the env vars -> attach a Disk at `/data`
and set `DB_PATH=/data/history.db`. Leave `RUN_ONCE` / `MAX_RUNTIME_MINUTES` unset.

## 3. Environment variables
| Variable | Default | Meaning |
|---|---|---|
| `TARGET_USERNAME` | `i7si_u` | user whose comments are searched |
| `GOOGLE_SHEET_NAME` / `GOOGLE_SHEET_ID` | – | the sheet |
| `GOOGLE_CREDENTIALS_JSON` | – | service-account JSON (raw or base64) |
| `MODE` | `followers` | or `discover` (search/explore) |
| `FOLLOWERS_FILE` | `followers.csv` | `.csv` / `.xlsx` / `.txt` |
| `CONCURRENCY` | `2` | parallel browser contexts (~300 MB each) |
| `PROFILE_MAX_VIDEOS` / `MAX_VIDEOS_PER_PROFILE` | `40` / `20` | videos listed / opened per profile |
| `COMMENT_SCROLLS` | `15` | max comment pages per video |
| `RUN_ONCE` / `MAX_RUNTIME_MINUTES` | off / `0` | one pass with a time limit (CI) |
| `PROXY_SERVER` ... | – | residential proxy if TikTok blocks datacenter IPs |

## 4. Notes
- `followers.csv` contains real usernames: keep the repository **private**.
- TikTok often blocks datacenter IPs (GitHub/Render) with captchas; a proxy usually helps. Respect TikTok's Terms of Service.
- The very first full pass over ~1,864 public accounts takes many hours/days; progress is saved and resumed.
