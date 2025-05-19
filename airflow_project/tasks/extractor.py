from airflow.models import Variable
import os
from seleniumwire import webdriver   
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse
from datetime import datetime

def extractor(**kwargs):
     # ─── Selenium-Wire options 
    seleniumwire_opts = {}
    proxy_url = Variable.get("proxy_url", 
            default_var=""
        )
    print(proxy_url)
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

    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")                # required in many containers
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--disable-dev-shm-usage")     # avoids /dev/shm issues
    chrome_opts.add_argument("--disable-setuid-sandbox")    # important for rootless
    chrome_opts.add_argument("--single-process")            # helps in low-resource envs
    chrome_opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    )

    # ─── Point at the apt-installed Chromium binary ────────────────────────
    chrome_bin = os.environ.get("CHROMIUM_PATH", "/usr/bin/chromium")
    if not os.path.isfile(chrome_bin):
        raise RuntimeError(f"Chrome binary not found at {chrome_bin!r}")
    chrome_opts.binary_location = chrome_bin

    # ─── Point at the apt-installed Chromedriver ───────────────────────────
    driver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
    if not os.path.isfile(driver_path):
        raise RuntimeError(f"Chromedriver not found at {driver_path!r}")
    service = Service(driver_path)

    driver = webdriver.Chrome(
        service=service,
        options=chrome_opts,
        seleniumwire_options=seleniumwire_opts  # only if you need your proxy
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
            # game name
            game_name = card.find_element(By.CSS_SELECTOR, ".match-name-text").text.strip()

            # scheduled time (e.g. "Sat 17 May 2:20")
            try:
                raw_time = card.find_element(
                    By.CSS_SELECTOR,
                    "li.meta-data-item[data-test='close-time']"
                ).text.strip()
                # assume current year and UTC
                dt = datetime.strptime(f"{datetime.now().year} {raw_time}", "%Y %a %d %b %H:%M")
                start_time = dt.isoformat()
                print(start_time)
            except Exception:
                start_time = None

            if not start_time:
                continue

            # split into team1 / team2
            parts = [t.strip() for t in game_name.split(" v ", 1)]
            team1, team2 = parts if len(parts) == 2 else (parts[0], "")

            # head-to-head odds
            odd_elems = card.find_elements(By.CSS_SELECTOR, ".animate-odd")
            odds1 = odd_elems[0].text.strip() if len(odd_elems) > 0 else None
            odds2 = odd_elems[1].text.strip() if len(odd_elems) > 1 else None

            rows.append({
                "source":     "tab",
                "game_time":  start_time,
                "team1":      team1,
                "odds1":      odds1,
                "team2":      team2,
                "odds2":      odds2
            })

        return rows

    finally:
        driver.quit()
