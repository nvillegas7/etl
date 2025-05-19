#!/usr/bin/env python3
import os
import argparse
import pandas as pd
from seleniumwire import webdriver            # ← selenium-wire!
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse
from datetime import datetime

def scrape_mlb_odds(proxy_url=None, headless=True, chromium_path=None):
    """
    Scrape the first MLB game odds from tab.com.au.
    Returns a pandas DataFrame with columns ['team','odds'].
    """
    # ─── Selenium-Wire options 
    seleniumwire_opts = {}
    if proxy_url:
        parsed = urlparse(proxy_url)
        creds = f"{parsed.username}:{parsed.password}@" if parsed.username else ""
        hostport = f"{parsed.hostname}:{parsed.port}"
        proxy_addr = f"{parsed.scheme}://{creds}{hostport}"
        seleniumwire_opts['proxy'] = {
            'http':  proxy_addr,
            'https': proxy_addr,
            'no_proxy': 'localhost,127.0.0.1'
        }

    # ─── Chrome setup
    chrome_opts = Options()
    # if headless:
    #     chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    )
    if chromium_path:
        chrome_opts.binary_location = chromium_path
    elif os.path.exists("/usr/bin/chromium"):
        chrome_opts.binary_location = "/usr/bin/chromium"

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_opts,
        seleniumwire_options=seleniumwire_opts
    )
    wait = WebDriverWait(driver, 15)

    try:
        mlb_url = (
            "https://www.tab.com.au/"
            "sports/betting/Baseball/competitions/"
            "Major%20League%20Baseball"
        )
        driver.get(mlb_url)

        # ─── wait for all games to render ────────────────────────────────────
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".template-item")))
        game_cards = driver.find_elements(By.CSS_SELECTOR, ".template-item")

        rows = []
        for card in game_cards:
            raw_time = card.find_element(
                By.CSS_SELECTOR,
                "li.meta-data-item[data-test='close-time']"
            ).text.strip()
            if not raw_time:
                continue
            # assume current year and UTC
            dt = datetime.strptime(f"{datetime.now().year} {raw_time}", "%Y %a %d %b %H:%M")
            start_time = dt.isoformat()

            # game name
            game_name = card.find_element(By.CSS_SELECTOR, ".match-name-text").text.strip()

            # split into team1 / team2
            parts = [t.strip() for t in game_name.split(" v ", 1)]
            team1, team2 = parts if len(parts) == 2 else (parts[0], "")

            # head-to-head odds: pick the first two .animate-odd under this card
            odd_elems = card.find_elements(By.CSS_SELECTOR, ".animate-odd")
            odds1 = odd_elems[0].text.strip() if len(odd_elems) > 0 else ""
            odds2 = odd_elems[1].text.strip() if len(odd_elems) > 1 else ""

            rows.append({
                "game":   game_name,
                "team1":  team1,
                "odds1":  odds1,
                "team2":  team2,
                "odds2":  odds2
            })

        return rows

    finally:
        driver.quit()

def main():
    parser = argparse.ArgumentParser(
        description="Scrape first MLB game odds from tab.com.au and output as CSV"
    )
    parser.add_argument(
        "-x", "--proxy",
        help="HTTP proxy URL (e.g. http://user:pass@host:port) or set WEB_PROXY env var",
        default=os.environ.get("WEB_PROXY")
    )
    parser.add_argument(
        "--no-headless",
        action="store_false",
        dest="headless",
        help="Run with a visible browser window (for debugging)"
    )
    parser.add_argument(
        "--chromium-path",
        help="Path to Chromium/Chrome binary if not on default PATH",
        default=os.environ.get("CHROMIUM_PATH")
    )
    parser.add_argument(
        "-o", "--output",
        help="Write CSV to this file instead of stdout",
        default=None
    )

    args = parser.parse_args()
    df = scrape_mlb_odds(
        proxy_url=args.proxy,
        headless=args.headless,
        chromium_path=args.chromium_path
    )

    csv_text = df.to_csv(index=False)
    if args.output:
        with open(args.output, "w") as f:
            f.write(csv_text)
        print(f"Wrote CSV to {args.output}")
    else:
        print(csv_text)

if __name__ == "__main__":
    main()
