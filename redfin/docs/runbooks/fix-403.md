# Steps to Fix Your Redfin Scraper

## The Problem
Your cookies are **10 months old** (last updated May 2025). The critical `aws-waf-token` has expired, causing 403 blocks.

## Solution: Export Fresh Cookies

### Step 1: Install Cookie Exporter
1. Open Chrome/Brave
2. Install extension: **"Cookie-Editor"** or **"EditThisCookie"**
3. Visit: https://www.redfin.ca

### Step 2: Browse Normally
1. **Log in to your Redfin account** (if you have one)
2. Browse 5-10 properties manually
3. Wait 30 seconds on each page
4. This generates a fresh `aws-waf-token`

### Step 3: Export Cookies
1. On redfin.ca, click the cookie extension
2. Export all cookies as JSON
3. Save/overwrite to: `app/Redfin/chrome_cookies.json`

### Step 4: Wait Before Scraping
- **Wait 24 hours** for your IP to cool down
- When ready, run with these settings:
  ```python
  test = True
  MAX_URLS = 10  # Start small!
  WAIT_SEC = 3
  max_workers = 1
  ```

## Important Notes
- Fresh cookies last ~2-3 months
- Re-export every time you get 403 errors
- Never scrape more than 50 properties per day
- Use delays of 3+ seconds between requests

## Alternative: Use Different Machine/Network
- If you can't wait, try from:
  - Different WiFi network
  - Mobile hotspot
  - VPN service
  - Friend's computer
