# Attention → Close QA Score Sync

Automatically syncs QA scores from Attention AI call scorecards to the corresponding leads in Close CRM. Runs hourly via GitHub Actions triggered by cron-job.org.

## How It Matches

For each scored Attention call:
1. Extracts the prospect email (excludes @modern-amenities.com)
2. Fetches ALL Close meetings and filters to the last 2 hours
3. Matches on **meeting title** — e.g. "Sarah Peltzer Machado and Vendingpreneurs Consultation" matches between both systems
4. Verifies the prospect email exists on that lead's contacts
5. Updates the lead's QA Score custom field

## Setup

### 1. Create the GitHub repo
- Create a new private repo (e.g. `attention-close-sync`)
- Push these files to it

### 2. Add GitHub Secrets
Go to **Settings → Secrets and variables → Actions → New repository secret**:

| Secret name | Value |
|---|---|
| `ATTENTION_API_KEY` | Your Attention Bearer token |
| `CLOSE_API_KEY` | Your Close API key |

### 3. Set up cron-job.org
- Go to [cron-job.org](https://cron-job.org) and create a free account
- Create a new cron job with this URL (replace with your details):
  ```
  https://api.github.com/repos/YOUR_USERNAME/YOUR_REPO/actions/workflows/sync.yml/dispatches
  ```
- Set schedule: every hour
- Add header: `Authorization: Bearer YOUR_GITHUB_PAT`
- Add header: `Content-Type: application/json`
- Set body: `{"ref": "main"}`

### 4. Test it
- Go to **Actions** tab in GitHub
- Click **Attention → Close QA Sync**
- Click **Run workflow** to trigger manually
- Watch the logs

## Config

Edit the top of `sync.py` to adjust:
- `LOOKBACK_HOURS` — how far back to look for calls (default: 2)
- `VALID_TITLE_KEYWORDS` — meeting title keywords that qualify as sales calls
- `INTERNAL_DOMAIN` — your company's email domain to exclude from prospect matching

## Expected Runtime
- ~2-5 minutes per run
- ~150-350 Close API calls per run
