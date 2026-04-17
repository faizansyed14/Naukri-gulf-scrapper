"""
scraper_core.py  –  scraping engine (used by Flask app)

Browser: visible Chrome by default so you can see the page load. Set SCRAPER_HEADLESS=1
to run headless (e.g. on a server).

ChromeDriver resolution (works best on restricted WiFi that blocks GitHub):
  CHROMEDRIVER_* env → project chromedriver.exe → PATH / common Windows paths
  → download matching ChromeDriver from Chrome for Testing (storage.googleapis.com;
     JSON from googlechromelabs.github.io — not GitHub releases)
  → optional Selenium Manager (set SCRAPER_DISABLE_SELENIUM_MANAGER=1 to skip if it hangs)
  → optional webdriver-manager (often uses GitHub; set SCRAPER_DISABLE_WEBDRIVER_MANAGER=1 to skip)

If no driver works, scrape_url falls back to HTTP GET (urllib).

Tip: On networks that block tooling, put a matching chromedriver.exe next to app.py or set
CHROMEDRIVER_PATH — no downloads or Selenium Manager needed.
"""

import io
import json
import os
import re
import sys
import time
import random
import shutil
import zipfile
import logging
import subprocess
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    StaleElementReferenceException,
    TimeoutException,
)

log = logging.getLogger("ScraperCore")

# Common Windows locations where chromedriver.exe might live
_WIN_FALLBACK_PATHS = [
    r"C:\chromedriver\chromedriver.exe",
    r"C:\Program Files\Google\Chrome\chromedriver.exe",
    r"C:\Program Files (x86)\Google\Chrome\chromedriver.exe",
    os.path.join(os.path.expanduser("~"), "chromedriver.exe"),
    os.path.join(os.path.expanduser("~"), "Downloads", "chromedriver.exe"),
]

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_DRIVER_KEYS = ("CHROMEDRIVER_PATH", "CHROMEDRIVER", "WEBDRIVER_CHROME_DRIVER")


def _blocked_reason(html: str) -> Optional[str]:
    """Best-effort detection of bot-block/captcha pages (common on cloud hosts)."""
    if not html:
        return None
    t = re.sub(r"\s+", " ", html).lower()
    needles = [
        "captcha",
        "verify you are human",
        "access denied",
        "unusual traffic",
        "cloudflare",
        "attention required",
        "incapsula",
        "perimeterx",
        "blocked",
        "/cdn-cgi/",
    ]
    for n in needles:
        if n in t:
            return n
    return None


def _env_chromedriver_path() -> Optional[str]:
    for key in _ENV_DRIVER_KEYS:
        p = os.environ.get(key, "").strip()
        if p and os.path.isfile(p):
            return p
    return None


def _which_chromedriver() -> Optional[str]:
    for name in ("chromedriver", "chromedriver.exe"):
        p = shutil.which(name)
        if p and os.path.isfile(p):
            return p
    return None


# Chrome for Testing — zip lives on storage.googleapis.com (often reachable when github.io is not)
_CFT_ZIP_TEMPLATE = (
    "https://storage.googleapis.com/chrome-for-testing-public/{version}/win64/chromedriver-win64.zip"
)
_JSON_LAST_GOOD = (
    "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json"
)


def _get_chrome_version_windows() -> Optional[str]:
    """Return Chrome version like 131.0.6778.109 from registry or chrome.exe --version."""
    if sys.platform != "win32":
        return None
    try:
        import winreg

        for root, sub in (
            (winreg.HKEY_CURRENT_USER, r"Software\Google\Chrome\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Wow6432Node\Google\Chrome\BLBeacon"),
        ):
            try:
                key = winreg.OpenKey(root, sub)
                try:
                    val, _ = winreg.QueryValueEx(key, "version")
                finally:
                    winreg.CloseKey(key)
                if isinstance(val, str) and re.fullmatch(r"\d+\.\d+\.\d+\.\d+", val.strip()):
                    return val.strip()
            except OSError:
                continue
    except Exception as e:
        log.debug("Registry Chrome version: %s", e)

    chrome_paths = []
    pf = os.environ.get("PROGRAMFILES", r"C:\Program Files")
    pfx86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
    local = os.environ.get("LOCALAPPDATA", "")
    for base in (
        os.path.join(pf, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(pfx86, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(local, "Google", "Chrome", "Application", "chrome.exe"),
    ):
        if base and os.path.isfile(base):
            chrome_paths.append(base)

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    for exe in chrome_paths:
        try:
            out = subprocess.run(
                [exe, "--version"],
                capture_output=True,
                text=True,
                timeout=20,
                creationflags=creationflags,
            )
            text = (out.stdout or "") + (out.stderr or "")
            m = re.search(r"(\d+\.\d+\.\d+\.\d+)", text)
            if m:
                return m.group(1)
        except (OSError, subprocess.TimeoutExpired) as e:
            log.debug("chrome --version: %s", e)
    return None


def _http_get_bytes(url: str, timeout: float = 45) -> Optional[bytes]:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        log.warning("GET %s failed: %s", url, e)
        return None


def _extract_chromedriver_exe_from_zip(data: bytes, dest_exe: str) -> bool:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = [n for n in zf.namelist() if n.endswith("chromedriver.exe")]
            if not names:
                log.warning("Zip has no chromedriver.exe")
                return False
            inner = sorted(names, key=len)[0]
            with zf.open(inner) as src, open(dest_exe, "wb") as dst:
                shutil.copyfileobj(src, dst)
        return True
    except Exception as e:
        log.warning("Unzip chromedriver failed: %s", e)
        return False


def _win64_chromedriver_zip_url_from_json(data: dict, chrome_version: str) -> Optional[str]:
    """Pick chromedriver win64 zip URL from CfT JSON; prefer exact Chrome version, else same major."""
    major = chrome_version.split(".")[0]

    def pick_from_downloads(downloads: dict) -> Optional[str]:
        items = (downloads or {}).get("chromedriver") or []
        for it in items:
            if it.get("platform") == "win64" and it.get("url"):
                return str(it["url"])
        return None

    # last-known-good: channels.Stable
    ch = data.get("channels") or {}
    for key in ("Stable", "Beta", "Dev", "Canary"):
        c = ch.get(key)
        if not c:
            continue
        ver = (c.get("version") or "").strip()
        if ver and ver.split(".")[0] == major:
            u = pick_from_downloads(c.get("downloads") or {})
            if u:
                log.info("Using CfT channel %s version %s", key, ver)
                return u

    # known-good list: versions[]
    versions = data.get("versions") or []
    exact = None
    same_major = []
    for v in versions:
        ver = (v.get("version") or "").strip()
        if not ver:
            continue
        u = pick_from_downloads(v.get("downloads") or {})
        if not u:
            continue
        if ver == chrome_version:
            exact = u
            break
        if ver.split(".")[0] == major:
            same_major.append((ver, u))
    if exact:
        return exact
    if same_major:
        same_major.sort(key=lambda x: [int(p) for p in x[0].split(".")], reverse=True)
        log.info("Using closest CfT chromedriver for major %s: %s", major, same_major[0][0])
        return same_major[0][1]
    return None


def _download_chromedriver_to_project() -> bool:
    """
    Download chromedriver.exe into the project folder (Windows).
    Uses storage.googleapis.com first; falls back to parsing CfT JSON for a win64 zip URL.
    """
    if sys.platform != "win32":
        return False

    chrome_ver = _get_chrome_version_windows()
    if not chrome_ver:
        log.warning("Could not detect Google Chrome version (is Chrome installed?)")
        return False

    dest = os.path.join(_PROJECT_DIR, "chromedriver.exe")
    log.info("Detected Chrome %s — fetching matching ChromeDriver…", chrome_ver)

    zip_url = _CFT_ZIP_TEMPLATE.format(version=chrome_ver)
    zdata = _http_get_bytes(zip_url, timeout=45)
    if not zdata:
        # Small JSON only — avoid known-good-versions (multi‑MB), which was slowing first run.
        raw = _http_get_bytes(_JSON_LAST_GOOD, timeout=25)
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
                zip_url = _win64_chromedriver_zip_url_from_json(data, chrome_ver)
                if zip_url:
                    zdata = _http_get_bytes(zip_url, timeout=45)
            except json.JSONDecodeError:
                pass

    if not zdata:
        log.warning("Could not download ChromeDriver zip (network or no matching build).")
        return False

    tmp = dest + ".download"
    try:
        if os.path.isfile(tmp):
            os.remove(tmp)
        if not _extract_chromedriver_exe_from_zip(zdata, tmp):
            return False
        os.replace(tmp, dest)
    except OSError as e:
        log.warning("Could not write chromedriver.exe: %s", e)
        return False

    log.info("ChromeDriver saved to %s", dest)
    return True


@dataclass
class Job:
    title: str = ""
    company: str = ""
    location: str = ""
    experience: str = ""
    posted_date: str = ""
    job_type: str = ""
    industry: str = ""
    description_snippet: str = ""
    easy_apply: str = ""
    employer_active: str = ""
    source_url: str = ""
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())


def _chrome_options() -> Options:
    opts = Options()
    # Faster DOM ready; does not wait for every image/font.
    opts.page_load_strategy = "eager"

    # Allow explicitly specifying Chrome/Chromium binary path (useful on Linux containers).
    chrome_bin = os.environ.get("CHROME_BIN", "").strip()
    if chrome_bin:
        try:
            opts.binary_location = chrome_bin
        except Exception:
            pass

    headless = os.environ.get("SCRAPER_HEADLESS", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-gpu")
    else:
        opts.add_argument("--start-maximized")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--log-level=3")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return opts


def _anti_detect(driver: webdriver.Chrome) -> None:
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )


def _try_chrome_with_path(driver_path: str, label: str, opts: Options) -> Optional[webdriver.Chrome]:
    try:
        log.info("Driver: trying %s → %s", label, driver_path)
        driver = webdriver.Chrome(service=Service(driver_path), options=opts)
        _anti_detect(driver)
        log.info("Driver: %s ✔", label)
        return driver
    except Exception as e:
        log.warning("Driver %s failed: %s", label, e)
        return None


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def _build_driver() -> webdriver.Chrome:
    """
    Resolve ChromeDriver: local paths first, then CfT zip download (no GitHub),
    then optional Selenium Manager / webdriver-manager (can hang on locked-down WiFi).
    """
    opts = _chrome_options()
    # Only meaningful on Windows. In Linux containers this file (if committed) can cause
    # confusing warnings and delays before the PATH driver is used.
    project_exe = os.path.join(_PROJECT_DIR, "chromedriver.exe") if sys.platform == "win32" else ""
    disable_sm = _env_flag("SCRAPER_DISABLE_SELENIUM_MANAGER")
    disable_wdm = _env_flag("SCRAPER_DISABLE_WEBDRIVER_MANAGER")

    env_path = _env_chromedriver_path()
    if env_path:
        d = _try_chrome_with_path(env_path, "env CHROMEDRIVER_PATH", opts)
        if d:
            return d

    if project_exe and os.path.isfile(project_exe):
        d = _try_chrome_with_path(project_exe, "project chromedriver.exe", opts)
        if d:
            return d
        # Do NOT delete local driver automatically. On restricted WiFi it forces
        # network-based re-download attempts, which looks like "Chrome not opening".
        log.warning("Local project chromedriver.exe failed to start; keeping file for diagnostics.")

    which_path = _which_chromedriver()
    if which_path:
        d = _try_chrome_with_path(which_path, "PATH chromedriver", opts)
        if d:
            return d

    for path in _WIN_FALLBACK_PATHS:
        if os.path.isfile(path):
            d = _try_chrome_with_path(path, f"known path ({path})", opts)
            if d:
                return d

    # If we already have local drivers available, skip Selenium Manager / webdriver-manager
    # by default (they may require network/tooling endpoints).
    has_local_driver = bool(env_path or os.path.isfile(project_exe) or which_path) or any(
        os.path.isfile(p) for p in _WIN_FALLBACK_PATHS
    )
    if has_local_driver:
        disable_sm = True
        disable_wdm = True

    # Before Selenium Manager: fetch driver from Chrome for Testing (Google Storage, not GitHub).
    if sys.platform == "win32":
        if _download_chromedriver_to_project() and os.path.isfile(project_exe):
            d = _try_chrome_with_path(project_exe, "downloaded ChromeDriver (CfT)", opts)
            if d:
                return d

    # Selenium Manager — may block a long time or fail when GitHub / update endpoints are blocked.
    if not disable_sm:
        try:
            log.info("Driver: Selenium Manager (auto-resolve ChromeDriver)…")
            driver = webdriver.Chrome(options=opts)
            _anti_detect(driver)
            log.info("Driver: Selenium Manager OK")
            return driver
        except Exception as e:
            log.warning("Selenium Manager failed: %s", e)
    else:
        log.info("Driver: skipping Selenium Manager (SCRAPER_DISABLE_SELENIUM_MANAGER=1)")

    if not disable_wdm:
        try:
            from webdriver_manager.chrome import ChromeDriverManager

            log.info("Driver: webdriver-manager…")
            path = ChromeDriverManager().install()
            d = _try_chrome_with_path(path, "webdriver-manager", opts)
            if d:
                return d
        except Exception as e:
            log.warning("webdriver-manager failed: %s", e)
    else:
        log.info("Driver: skipping webdriver-manager (SCRAPER_DISABLE_WEBDRIVER_MANAGER=1)")

    raise RuntimeError(
        "Could not start ChromeDriver.\n"
        "On WiFi that blocks GitHub/tools: put a matching chromedriver.exe in this folder, or set "
        "CHROMEDRIVER_PATH to chromedriver.exe (download once from a good network from "
        "https://googlechromelabs.github.io/chrome-for-testing/ ).\n"
        "Optional: set SCRAPER_DISABLE_SELENIUM_MANAGER=1 if Selenium Manager hangs.\n"
        "If Chrome is unavailable, the scraper will try a plain HTTP download instead."
    )


def _fetch_html_requests(url: str, timeout: float = 35) -> Optional[str]:
    """Fetch HTML without a browser (works when Chrome/driver is missing; page must not be JS-only)."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return raw.decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as e:
        log.warning("HTTP fetch failed for %s: %s", url, e)
        return None


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize_listing_base(url: str) -> str:
    """
    Strip trailing -N pagination suffix from Naukrigulf listing URLs when it is
    clearly a page number, not part of the location slug.

    Single-area URLs look like jobs-in-dubai-2 or jobs-in-abu-dhabi-2 (few hyphen
    segments after jobs-in-). Composite slugs such as
    jobs-in-dammam-and-khobar-and-eastern-province-20 end with digits that belong
    to the location key, not pagination — stripping those breaks the listing and
    pagination clicks (Dubai/Abu Dhabi keep working; Dammam-style URLs do not).
    """
    u = url.strip().split("#")[0].split("?")[0].rstrip("/")
    if "jobs-in-" not in u.lower() and "/jobs-" not in u.lower():
        return u
    m = re.search(r"-(\d{1,4})$", u)
    if not m:
        return u
    page_digits = m.group(1)
    try:
        path = (urlparse(u).path or "").lower()
    except Exception:
        path = ""
    marker = "jobs-in-"
    idx = path.find(marker)
    if idx == -1:
        return u
    rest = path[idx + len(marker) :].lstrip("/")
    if not rest:
        return u
    parts = rest.split("-")
    if not parts or parts[-1] != page_digits:
        return u
    # Pagination: jobs-in-<one or two words>-N → at most 3 segments (e.g. saudi-arabia-2).
    if len(parts) > 3:
        return u
    return re.sub(r"-\d{1,4}$", "", u, flags=re.I)


def listing_page_url(base_url: str, page: int) -> str:
    """Build listing pagination URL for supported portals."""
    if _is_gulftalent_url(base_url):
        return _gulftalent_listing_page_url(base_url, page)

    if _is_bayt_url(base_url):
        base = _normalize_bayt_base(base_url)
        if page <= 1:
            return base
        return f"{base}?page={page}"

    # Default: Naukrigulf style
    base = _normalize_listing_base(base_url)
    if page <= 1:
        return base
    return f"{base}-{page}"


def _naukrigulf_listing_page_url(page1_canonical: str, page_num: int) -> str:
    """
    Build Naukrigulf listing URL for page N from the real page-1 URL (after redirects).

    Naukrigulf stacks page index on the path: …/jobs-in-…-dubai, …/jobs-in-…-dubai-2, and
    …/jobs-in-…-eastern-province-20, …/jobs-in-…-eastern-province-20-2 — not listing_page_url()
    re-normalizing an URL that already ends with -2 (that would wrongly produce …-2-3).
    """
    u = page1_canonical.strip().split("#")[0].rstrip("/")
    if page_num <= 1:
        return u
    return f"{u}-{int(page_num)}"


def _is_gulftalent_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return "gulftalent.com" in host


def _is_bayt_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return "bayt.com" in host


def _is_naukrigulf_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        host = ""
    return "naukrigulf.com" in host


def _is_naukrigulf_dammam_composite_listing(url: str) -> bool:
    """Dammam/Khobar/Eastern Province composite SRP — use path pagination URLs only."""
    try:
        return "jobs-in-dammam-and-khobar-and-eastern-province" in (url or "").lower()
    except Exception:
        return False


# Bayt job URLs: /en/<country-slug>/jobs/<title>-<id>/ (UAE, Saudi Arabia, etc.)
_BAYT_JOB_LINK_RE = re.compile(r"/en/[^/]+/jobs/[^/]+-\d+/?")
_BAYT_LOC_LINK_RE = re.compile(r"/en/[^/]+/jobs/jobs-in-")


def _normalize_gulftalent_base(url: str) -> str:
    """Strip trailing /N pagination suffix from GulfTalent path-style listing URLs."""
    u = url.strip().split("#")[0].split("?")[0].rstrip("/")
    # GulfTalent: .../abu-dhabi/2, /3, ...
    if re.search(r"/\d{1,4}$", u):
        return re.sub(r"/\d{1,4}$", "", u)
    return u


def _gulftalent_listing_page_url(url: str, page: int) -> str:
    """
    GulfTalent uses two styles:
    - Search / filtered URLs: .../jobs/search?... — paginate with ?page=N (path /jobs/search/2 is 404).
    - Location-style URLs: .../abu-dhabi/2 — path suffix pagination.
    """
    url = url.strip().split("#")[0]
    parsed = urlparse(url)
    path = (parsed.path or "").rstrip("/")

    # Only the search listing uses ?page=N; other /jobs/... URLs use /path/2
    use_query_page = "/jobs/search" in path.lower()
    if use_query_page:
        if page <= 1:
            return url
        pairs = [
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if k.lower() != "page"
        ]
        pairs.append(("page", str(page)))
        new_query = urlencode(pairs)
        return urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, "")
        )

    base = _normalize_gulftalent_base(url)
    if page <= 1:
        return base
    return f"{base}/{page}"


def _is_gulftalent_search_url(url: str) -> bool:
    """True for .../jobs/search?... — must use in-page pagination clicks, not /search/N URLs."""
    if not _is_gulftalent_url(url):
        return False
    try:
        return "/jobs/search" in (urlparse(url).path or "").lower()
    except Exception:
        return False


def _gulftalent_wait_listing_table(driver: webdriver.Chrome, timeout: float = 25.0) -> None:
    """Wait for SPA to render the jobs table (Position / Company / …)."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: "Position" in (d.page_source or "")
            and "Company" in (d.page_source or "")
            and "Location" in (d.page_source or "")
        )
    except TimeoutException:
        log.warning("GulfTalent: table headers not seen within %ss", timeout)


def _gulftalent_find_page_link(driver: webdriver.Chrome, page_num: int):
    """Return a clickable element for pagination page number, or None."""
    num = str(int(page_num))
    xpaths = [
        f"//ul[contains(@class,'pagination')]//a[normalize-space()='{num}']",
        f"//*[contains(@class,'pagination')]//a[normalize-space()='{num}']",
        f"//nav//a[normalize-space()='{num}']",
        f"//a[contains(@href,'/jobs/search') and normalize-space()='{num}']",
        f"//a[contains(@href,'page=') and normalize-space()='{num}']",
        f"//a[@role='button' and normalize-space()='{num}']",
    ]
    for xp in xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def _gulftalent_click_page_number(driver: webdriver.Chrome, page_num: int) -> bool:
    """Click the pagination control for the given page number (same tab / SPA)."""
    deadline = time.time() + 20.0
    while time.time() < deadline:
        el = _gulftalent_find_page_link(driver, page_num)
        if el is None:
            time.sleep(0.35)
            continue
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.45)
            driver.execute_script("arguments[0].click();", el)
        except StaleElementReferenceException:
            continue
        except ElementClickInterceptedException:
            time.sleep(0.5)
            continue
        except Exception as e:
            log.debug("GulfTalent click: %s", e)
            time.sleep(0.4)
            continue

        time.sleep(1.0)
        _gulftalent_wait_listing_table(driver, 22)
        time.sleep(0.45)
        return True

    log.info("GulfTalent: pagination link for page %s not found", page_num)
    return False


def _scrape_gulftalent_search_click_pagination(url: str, max_pages: int) -> dict:
    """
    Single Chrome session: open search URL, parse table, click 2, 3, … in the pagination bar.
    Ignores parallel workers — required because listing is one in-page app route.
    """
    all_jobs: list[dict] = []
    seen: set[str] = set()
    pages_scraped = 0
    total_jobs_reported: Optional[int] = None
    start_url = url.strip().split("#")[0]

    try:
        driver = _build_driver()
    except Exception as e:
        return {
            "jobs": [],
            "count": 0,
            "pages_scraped": 0,
            "total_jobs_reported": None,
            "error": str(e),
        }

    try:
        log.info("GulfTalent search: loading %s (click pagination up to %s pages)", start_url, max_pages)
        driver.get(start_url)
        _gulftalent_wait_listing_table(driver, 25)
        time.sleep(0.5)

        for page in range(1, max_pages + 1):
            html = driver.page_source
            cur = driver.current_url
            soup = BeautifulSoup(html, "lxml")
            if page == 1:
                total_jobs_reported = _extract_total_job_count_gulftalent(soup)

            batch = _parse_html(html, cur)
            pages_scraped = page
            log.info("GulfTalent search: page %s — %s job row(s) in table", page, len(batch))

            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)

            if page >= max_pages:
                break

            next_page = page + 1
            if not _gulftalent_click_page_number(driver, next_page):
                log.info("GulfTalent search: stopping — could not open page %s", next_page)
                break

        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": None,
        }
    except Exception as exc:
        log.error("GulfTalent search click scrape: %s", exc)
        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": str(exc),
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _bayt_wait_listing_ready(driver: webdriver.Chrome, timeout: float = 30.0) -> None:
    """Wait until Bayt renders job cards or the results header (JS-heavy listing)."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: (
                _BAYT_JOB_LINK_RE.search(d.page_source or "") is not None
                or re.search(r"\bjobs\s+found\b", d.page_source or "", re.I) is not None
            )
        )
    except TimeoutException:
        log.warning("Bayt: listing did not appear within %ss", timeout)


def _bayt_find_page_link(driver: webdriver.Chrome, page_num: int):
    """Return a clickable pagination control for the given page number."""
    num = str(int(page_num))
    xpaths = [
        f"//ul[contains(@class,'pagination')]//a[normalize-space()='{num}']",
        f"//*[contains(@class,'pagination')]//a[normalize-space()='{num}']",
        f"//*[contains(@class,'paging')]//a[normalize-space()='{num}']",
        f"//nav//a[normalize-space()='{num}']",
        f"//a[contains(@href,'bayt.com') and contains(@href,'page={num}')]",
        f"//a[contains(@href,'?page={num}')]",
        f"//a[contains(@href,'&page={num}')]",
        f"//a[contains(@href,'/page/{num}/')]",
        f"//a[@aria-label='Page {num}' or @aria-label=\"Page {num}\"]",
        f"//button[normalize-space()='{num}']",
    ]
    for xp in xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def _bayt_click_page_number(driver: webdriver.Chrome, page_num: int) -> bool:
    """Click pagination for page N (SPA / in-page updates)."""
    deadline = time.time() + 22.0
    while time.time() < deadline:
        el = _bayt_find_page_link(driver, page_num)
        if el is None:
            time.sleep(0.4)
            continue
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.45)
            driver.execute_script("arguments[0].click();", el)
        except StaleElementReferenceException:
            continue
        except ElementClickInterceptedException:
            time.sleep(0.55)
            continue
        except Exception as e:
            log.debug("Bayt click: %s", e)
            time.sleep(0.45)
            continue

        time.sleep(1.1)
        _bayt_wait_listing_ready(driver, 25)
        time.sleep(0.45)
        return True

    log.info("Bayt: pagination link for page %s not found", page_num)
    return False


def _scrape_bayt_click_pagination(url: str, max_pages: int) -> dict:
    """
    Single Chrome session: load listing URL, parse cards, click 2, 3, … until max_pages or end.
    Matches GulfTalent search flow; workers are ignored.
    """
    all_jobs: list[dict] = []
    seen: set[str] = set()
    pages_scraped = 0
    total_jobs_reported: Optional[int] = None
    start_url = url.strip().split("#")[0]

    try:
        driver = _build_driver()
    except Exception as e:
        return {
            "jobs": [],
            "count": 0,
            "pages_scraped": 0,
            "total_jobs_reported": None,
            "error": str(e),
        }

    try:
        log.info("Bayt: loading %s (click pagination up to %s pages)", start_url, max_pages)
        driver.get(start_url)
        _bayt_wait_listing_ready(driver, 30)
        time.sleep(0.55)

        for page in range(1, max_pages + 1):
            html = driver.page_source
            cur = driver.current_url
            soup = BeautifulSoup(html, "lxml")
            if page == 1:
                total_jobs_reported = _extract_total_job_count_bayt(soup)

            batch = _parse_html(html, cur)
            pages_scraped = page
            log.info("Bayt: page %s — %s job row(s)", page, len(batch))

            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)

            if page >= max_pages:
                break

            next_page = page + 1
            if not _bayt_click_page_number(driver, next_page):
                log.info("Bayt: stopping — could not open page %s", next_page)
                break

        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": None,
        }
    except Exception as exc:
        log.error("Bayt click scrape: %s", exc)
        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": str(exc),
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _naukrigulf_wait_listing_ready(driver: webdriver.Chrome, timeout: float = 28.0) -> None:
    """Wait for Naukrigulf SRP job cards (same markers as listing parser)."""
    sel = (
        "[class*='srp-tuple'],[class*='job-card'],[class*='jobCard'],"
        "[class*='np-tuple'],article[data-job-id],li[data-job-id],div[data-job-id]"
    )
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, sel)) > 0
        )
    except TimeoutException:
        log.warning("Naukrigulf: job cards not seen within %ss", timeout)


def _naukrigulf_scroll_for_pagination(driver: webdriver.Chrome) -> None:
    """Pagination is often below the fold; relative job links also end in -digits."""
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.92);")
        time.sleep(0.35)
    except Exception:
        pass


def _naukrigulf_find_page_link(driver: webdriver.Chrome, page_num: int):
    """Find pagination link for page N (path …-N matches listing_page_url style)."""
    num = int(page_num)
    if num <= 1:
        return None
    nstr = str(num)
    _naukrigulf_scroll_for_pagination(driver)
    xpaths = [
        f"//ul[contains(@class,'pagination')]//a[normalize-space()='{nstr}']",
        f"//*[contains(@class,'pagination')]//a[normalize-space()='{nstr}']",
        f"//*[contains(@class,'pgntn') or contains(@class,'paging')]//a[normalize-space()='{nstr}']",
        f"//a[contains(@href,'naukrigulf.com') and normalize-space()='{nstr}']",
        f"//nav//a[normalize-space()='{nstr}']",
        f"//button[normalize-space()='{nstr}']",
        f"//*[contains(@class,'pagination')]//button[normalize-space()='{nstr}']",
    ]
    for xp in xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            if el.is_displayed():
                return el
        except Exception:
            continue
    cur = driver.current_url
    try:
        for a in driver.find_elements(
            By.CSS_SELECTOR, "a[href*='naukrigulf.com'], a[href*='jobs-in']"
        ):
            try:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                full = urljoin(cur, href)
                try:
                    host = (urlparse(full).netloc or "").lower()
                except Exception:
                    continue
                if "naukrigulf.com" not in host:
                    continue
                path = urlparse(full).path.rstrip("/")
                if "/jobs-in-" not in path.lower():
                    continue
                m = re.search(r"-(\d+)$", path)
                if m and int(m.group(1)) == num and a.is_displayed():
                    return a
            except Exception:
                continue
    except Exception:
        pass
    return None


def _naukrigulf_listing_snapshot(driver: webdriver.Chrome) -> tuple[str, ...]:
    """First few job ids on the listing — used to detect pagination after Next click."""
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "[data-job-id]")
        return tuple((e.get_attribute("data-job-id") or "").strip() for e in els[:6])
    except Exception:
        return ()


def _naukrigulf_find_next_link(driver: webdriver.Chrome):
    """Pagination 'Next' control (Naukrigulf often exposes this when page numbers are truncated)."""
    _naukrigulf_scroll_for_pagination(driver)
    xpaths = [
        "//a[@rel='next']",
        "//ul[contains(@class,'pagination')]//li[contains(@class,'next')]//a",
        "//*[contains(@class,'pagination')]//a[contains(@class,'next')]",
        "//*[contains(@class,'pgntn') or contains(@class,'paging')]//a[contains(@class,'next')]",
        "//a[contains(@href,'naukrigulf.com') and contains(@aria-label,'Next')]",
        "//a[contains(@aria-label,'Next') or contains(@aria-label,'next')]",
        "//button[contains(@aria-label,'Next') or contains(@aria-label,'next')]",
        "//*[@role='button' and (contains(@aria-label,'Next') or contains(@aria-label,'next'))]",
        "//a[contains(@class,'next') and contains(@href,'jobs-in')]",
        "//*[contains(@class,'pagination')]//a[contains(@class,'frwd') or contains(@class,'forward')]",
    ]
    for xp in xpaths:
        try:
            for el in driver.find_elements(By.XPATH, xp):
                try:
                    if not el.is_displayed():
                        continue
                    if (el.get_attribute("aria-disabled") or "").lower() == "true":
                        continue
                    cls = (el.get_attribute("class") or "").lower()
                    if "disabled" in cls:
                        continue
                    return el
                except Exception:
                    continue
        except Exception:
            continue
    return None


def _naukrigulf_click_next(driver: webdriver.Chrome) -> bool:
    """Click Next; return True if listing content or URL changed."""
    before_url = driver.current_url
    before_snap = _naukrigulf_listing_snapshot(driver)
    deadline = time.time() + 22.0
    while time.time() < deadline:
        el = _naukrigulf_find_next_link(driver)
        if el is None:
            time.sleep(0.4)
            continue
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.45)
            driver.execute_script("arguments[0].click();", el)
        except StaleElementReferenceException:
            continue
        except ElementClickInterceptedException:
            time.sleep(0.55)
            continue
        except Exception as e:
            log.debug("Naukrigulf next click: %s", e)
            time.sleep(0.45)
            continue

        time.sleep(1.0)
        _naukrigulf_wait_listing_ready(driver, 26)
        time.sleep(0.45)
        after_url = driver.current_url
        after_snap = _naukrigulf_listing_snapshot(driver)
        if after_url != before_url or (after_snap and after_snap != before_snap):
            return True
        time.sleep(0.5)
    log.info("Naukrigulf: Next control not found or listing did not change")
    return False


def _naukrigulf_click_page_number(driver: webdriver.Chrome, page_num: int) -> bool:
    deadline = time.time() + 22.0
    while time.time() < deadline:
        el = _naukrigulf_find_page_link(driver, page_num)
        if el is None:
            time.sleep(0.4)
            continue
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.45)
            driver.execute_script("arguments[0].click();", el)
        except StaleElementReferenceException:
            continue
        except ElementClickInterceptedException:
            time.sleep(0.55)
            continue
        except Exception as e:
            log.debug("Naukrigulf click: %s", e)
            time.sleep(0.45)
            continue

        time.sleep(1.0)
        _naukrigulf_wait_listing_ready(driver, 26)
        time.sleep(0.45)
        return True

    log.info("Naukrigulf: pagination link for page %s not found", page_num)
    return False


def _scrape_naukrigulf_dammam_url_pagination(url: str, max_pages: int) -> dict:
    """
    Dammam/Khobar/Eastern Province only: open each listing page with GET …/jobs-in-…-2, -3, …
    (same path rule as Jeddah; clicks/Next are skipped for this listing only).
    """
    all_jobs: list[dict] = []
    seen: set[str] = set()
    pages_scraped = 0
    total_jobs_reported: Optional[int] = None
    raw = url.strip().split("#")[0]
    base = _normalize_listing_base(raw).strip().split("#")[0].rstrip("/")

    try:
        driver = _build_driver()
    except Exception as e:
        return {
            "jobs": [],
            "count": 0,
            "pages_scraped": 0,
            "total_jobs_reported": None,
            "error": str(e),
        }

    try:
        log.info(
            "Naukrigulf (Dammam composite): URL pagination base=%s up to %s page(s)",
            base,
            max_pages,
        )
        for page in range(1, max_pages + 1):
            page_url = _naukrigulf_listing_page_url(base, page)
            log.info("Naukrigulf (Dammam): fetching page %s — %s", page, page_url)
            driver.get(page_url)
            _naukrigulf_wait_listing_ready(driver, 28)
            time.sleep(0.45)

            html = driver.page_source
            cur = driver.current_url
            soup = BeautifulSoup(html, "lxml")
            if page == 1:
                total_jobs_reported = _extract_total_job_count(soup)

            batch = _parse_html(html, cur)
            pages_scraped = page
            log.info("Naukrigulf (Dammam): page %s — %s job row(s)", page, len(batch))

            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)

        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": None,
        }
    except Exception as exc:
        log.error("Naukrigulf Dammam URL scrape: %s", exc)
        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": str(exc),
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _scrape_naukrigulf_click_pagination(url: str, max_pages: int) -> dict:
    """
    One Chrome window: open page-1 listing, parse, click 2, 3, … (same UX as Bayt/GulfTalent).
    Avoids full reload per ?page URL; workers ignored.
    """
    all_jobs: list[dict] = []
    seen: set[str] = set()
    pages_scraped = 0
    total_jobs_reported: Optional[int] = None
    raw = url.strip().split("#")[0]
    start_url = _normalize_listing_base(raw)

    try:
        driver = _build_driver()
    except Exception as e:
        return {
            "jobs": [],
            "count": 0,
            "pages_scraped": 0,
            "total_jobs_reported": None,
            "error": str(e),
        }

    try:
        log.info(
            "Naukrigulf: loading %s (click pagination up to %s pages)",
            start_url,
            max_pages,
        )
        driver.get(start_url)
        _naukrigulf_wait_listing_ready(driver, 28)
        time.sleep(0.55)
        r = _blocked_reason(driver.page_source)
        if r:
            raise RuntimeError(f"Naukrigulf appears to be blocking this host (signal: {r}).")
        page1_url = driver.current_url.strip().split("#")[0].rstrip("/")
        log.info("Naukrigulf: page-1 canonical URL (after redirect): %s", page1_url)

        for page in range(1, max_pages + 1):
            html = driver.page_source
            r = _blocked_reason(html)
            if r:
                raise RuntimeError(f"Naukrigulf appears to be blocking this host (signal: {r}).")
            cur = driver.current_url
            soup = BeautifulSoup(html, "lxml")
            if page == 1:
                total_jobs_reported = _extract_total_job_count(soup)

            batch = _parse_html(html, cur)
            pages_scraped = page
            log.info("Naukrigulf: page %s — %s job row(s)", page, len(batch))

            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)

            if page >= max_pages:
                break

            next_page = page + 1
            if not _naukrigulf_click_page_number(driver, next_page):
                log.info(
                    "Naukrigulf: page %s link not found, trying Next",
                    next_page,
                )
                if not _naukrigulf_click_next(driver):
                    nav_url = _naukrigulf_listing_page_url(page1_url, next_page)
                    log.info(
                        "Naukrigulf: opening page %s via URL (click/Next failed): %s",
                        next_page,
                        nav_url,
                    )
                    try:
                        driver.get(nav_url)
                        _naukrigulf_wait_listing_ready(driver, 28)
                        time.sleep(0.45)
                    except Exception as e:
                        log.info("Naukrigulf: navigation failed: %s", e)
                        log.info("Naukrigulf: stopping — could not open page %s", next_page)
                        break

        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": None,
        }
    except Exception as exc:
        log.error("Naukrigulf click scrape: %s", exc)
        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": str(exc),
        }
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _normalize_bayt_base(url: str) -> str:
    """Strip query and trailing pagination from Bayt listing URLs."""
    u = url.strip().split("#")[0].split("?")[0].rstrip("/")
    return u + "/"


def _job_dedupe_key(job: dict) -> str:
    return "|".join(
        [
            (job.get("title") or "").strip(),
            (job.get("company") or "").strip(),
            (job.get("location") or "").strip(),
            (job.get("posted_date") or "").strip(),
        ]
    )


def _extract_total_job_count(soup: BeautifulSoup) -> Optional[int]:
    """Parse 'Showing 8,919 Jobs' / 'X Jobs in …' from listing header."""
    text = soup.get_text(" ", strip=True)
    for pat in (
        r"Showing\s+([\d,]+)\s+Jobs",
        r"([\d,]+)\s+Jobs\s+in\b",
        r"([\d,]+)\s+jobs\s+found",
    ):
        m = re.search(pat, text, re.I)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def _extract_total_job_count_gulftalent(soup: BeautifulSoup) -> Optional[int]:
    text = soup.get_text(" ", strip=True)
    m = re.search(r"([\d,]+)\s+Jobs\s+found", text, re.I)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def _extract_total_job_count_bayt(soup: BeautifulSoup) -> Optional[int]:
    text = soup.get_text(" ", strip=True)
    # e.g. "2K jobs found", "3813 jobs found"
    m = re.search(r"(\d+(?:[.,]\d+)?\s*[kK]?)\s+jobs\s+found", text, re.I)
    if not m:
        return None
    raw = m.group(1).strip().lower().replace(",", "")
    try:
        if raw.endswith("k"):
            return int(float(raw[:-1]) * 1000)
        return int(float(raw))
    except ValueError:
        return None


def _badges_from_card(card) -> tuple:
    """Easy Apply / Employer Active style labels (text only, no logos)."""
    raw = _clean(card.get_text(" ", strip=True)).lower()
    easy = "Yes" if "easy apply" in raw else ""
    emp = "Yes" if "employer active" in raw else ""
    return easy, emp


def _posted_time_from_card(card) -> str:
    """Relative time like '1 min ago' from card text (no logos — text only)."""
    blob = _clean(card.get_text(" ", strip=True))
    m = re.search(
        r"(\d+\s*(?:min|mins|hr|hrs|hour|hours|day|days|week|weeks|month|months)s?\s+ago)",
        blob,
        re.I,
    )
    if m:
        return m.group(1).strip()
    for sel in [
        "[class*='posted']",
        "[class*='ago']",
        "[class*='time']",
        "time[datetime]",
    ]:
        el = card.select_one(sel)
        if el:
            t = _clean(el.get_text() or el.get("datetime", ""))
            if t and len(t) < 120:
                return t
    m = re.search(
        r"(Posted\s+(?:on\s+)?[^\n]{3,80})",
        blob,
        re.I,
    )
    if m:
        return _clean(m.group(1))[:100]
    return ""


def _get_html(driver, url: str) -> Optional[str]:
    try:
        driver.get(url)
        WebDriverWait(driver, 12).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "[class*='job-card'],[class*='jobCard'],[class*='srp-tuple'],"
                 "article,[class*='np-tuple'],[data-job-id]")
            )
        )
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(random.uniform(0.6, 1.1))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(0.4, 0.9))
        return driver.page_source
    except TimeoutException:
        return driver.page_source
    except Exception as exc:
        log.error("Error fetching %s: %s", url, exc)
        return None


def _extract_job(card, source_url: str) -> Job:
    """Parse one listing card: text fields only (no job URL, no company logos)."""
    for im in card.find_all("img"):
        im.decompose()

    job = Job(source_url=source_url)
    easy, emp = _badges_from_card(card)
    job.easy_apply = easy
    job.employer_active = emp

    for sel in ["[class*='designation']", "[class*='job-title']",
                "[class*='jobtitle']", "h2 a", "h3 a", "a[title]"]:
        el = card.select_one(sel)
        if el:
            job.title = _clean(el.get_text() or el.get("title", ""))
            break
    if not job.title:
        for tag in ["h2", "h3", "h4", "strong"]:
            el = card.find(tag)
            if el:
                job.title = _clean(el.get_text())
                break

    for sel in ["[class*='company']", "[class*='org']", "[class*='employer']"]:
        el = card.select_one(sel)
        if el:
            job.company = _clean(el.get_text())
            break

    for sel in ["[class*='location']", "[class*='city']", "[class*='loc']"]:
        el = card.select_one(sel)
        if el:
            job.location = _clean(el.get_text())
            break

    for sel in ["[class*='exp']", "[class*='experience']", "[class*='yrs']"]:
        el = card.select_one(sel)
        if el:
            job.experience = _clean(el.get_text())
            break

    for sel in ["[class*='posted']", "[class*='date']", "[class*='ago']", "time"]:
        el = card.select_one(sel)
        if el:
            txt = _clean(el.get_text() or el.get("datetime", ""))
            if txt:
                job.posted_date = txt
                break
    if not job.posted_date:
        job.posted_date = _posted_time_from_card(card)

    for sel in ["[class*='job-type']", "[class*='jobtype']"]:
        el = card.select_one(sel)
        if el:
            job.job_type = _clean(el.get_text())
            break

    for sel in ["[class*='industry']", "[class*='sector']", "[class*='category']"]:
        el = card.select_one(sel)
        if el:
            job.industry = _clean(el.get_text())
            break

    for sel in ["[class*='desc']", "[class*='summary']", "[class*='snippet']", "p"]:
        el = card.select_one(sel)
        if el and len(el.get_text(strip=True)) > 20:
            job.description_snippet = _clean(el.get_text())[:300]
            break

    return job


def _parse_html(html: str, source_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    if _is_gulftalent_url(source_url):
        return _parse_html_gulftalent(soup, source_url)
    if _is_bayt_url(source_url):
        return _parse_html_bayt(soup, source_url)

    selectors = [
        "[class*='srp-tuple']", "[class*='job-card']", "[class*='jobCard']",
        "[class*='np-tuple']", "article[data-job-id]", "li[data-job-id]", "div[data-job-id]",
    ]
    cards = []
    for sel in selectors:
        cards = soup.select(sel)
        if cards:
            break

    jobs = []
    for card in cards:
        j = _extract_job(card, source_url)
        # Avoid picking up non-job sections (e.g. "Popular searches") by requiring a company name.
        if j.title and j.company:
            jobs.append(asdict(j))
    return jobs


def _parse_html_gulftalent(soup: BeautifulSoup, source_url: str) -> list[dict]:
    """Parse GulfTalent listing table: Position / Company / Location / Date."""
    # Find the main jobs table by header labels.
    table = None
    for t in soup.select("table"):
        header = _clean(t.get_text(" ", strip=True)).lower()
        if "position" in header and "company" in header and "location" in header and "date" in header:
            table = t
            break
    if table is None:
        return []

    jobs: list[dict] = []
    year = datetime.now().year
    for tr in table.select("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        # Observed structure:
        # td[0] = "Title | Company"
        # td[1] = Location
        # td[2] = Date (e.g. "3 Apr")
        # td[3] = optional (icons/empty)
        first = _clean(tds[0].get_text(" | ", strip=True))
        parts = [p.strip() for p in first.split("|") if p.strip()]
        title = parts[0] if parts else ""
        company = parts[1] if len(parts) > 1 else ""
        location = _clean(tds[1].get_text(" ", strip=True))
        date_txt = _clean(tds[2].get_text(" ", strip=True))
        if not title or not company:
            continue

        # GulfTalent often shows "3 Apr" (no year). Attach current year for sorting.
        if date_txt and not re.search(r"\b\d{4}\b", date_txt):
            date_txt = f"{date_txt} {year}"

        row_text = _clean(tr.get_text(" ", strip=True)).lower()
        easy = "Yes" if "easy apply" in row_text else ""

        job = Job(
            title=title,
            company=company,
            location=location,
            posted_date=date_txt,
            easy_apply=easy,
            employer_active="",
            source_url=source_url,
        )
        jobs.append(asdict(job))

    return jobs


def _parse_html_bayt(soup: BeautifulSoup, source_url: str) -> list[dict]:
    """Parse Bayt listing cards."""
    jobs: list[dict] = []
    year = datetime.now().year

    # Bayt cards observed as li.has-pointer-d
    cards = soup.select("li.has-pointer-d")
    if not cards:
        # fallback: any li with a country-scoped job link
        cards = soup.find_all(
            "li",
            lambda t: t.find("a", href=_BAYT_JOB_LINK_RE),
        )

    for card in cards:
        a = card.find("a", href=_BAYT_JOB_LINK_RE)
        if not a:
            continue
        title = _clean(a.get_text(" ", strip=True))
        comp_el = card.find("a", href=re.compile(r"/en/company/"))
        company = _clean(comp_el.get_text(" ", strip=True)) if comp_el else ""
        loc_el = card.find("a", href=_BAYT_LOC_LINK_RE)
        location = _clean(loc_el.get_text(" ", strip=True)) if loc_el else ""

        blob = _clean(card.get_text(" ", strip=True))
        m = re.search(
            r"(\d+\s+(?:minute|minutes|hour|hours|day|days|week|weeks|month|months)\s+ago)",
            blob,
            re.I,
        )
        posted = m.group(1) if m else ""
        # Some Bayt pages show absolute dates rarely; keep as-is if detected.
        if posted:
            posted_date = posted
        else:
            posted_date = ""

        job = Job(
            title=title,
            company=company,
            location=location,
            posted_date=posted_date,
            easy_apply="Yes" if "easy apply" in blob.lower() else "",
            employer_active="",
            source_url=source_url,
        )
        if job.title and job.company:
            jobs.append(asdict(job))

    return jobs


def _scrape_listing_page_worker(page_num: int, base_url: str) -> dict:
    """Fetch one listing page in its own browser (for parallel runs)."""
    page_url = listing_page_url(base_url, page_num)
    result: dict = {"page": page_num, "jobs": [], "total_reported": None, "error": None}
    driver = None
    try:
        try:
            driver = _build_driver()
        except RuntimeError:
            driver = None
        html = None
        if driver is not None:
            html = _get_html(driver, page_url)
        if not html:
            html = _fetch_html_requests(page_url)
        if not html:
            result["error"] = "no html"
            return result
        soup = BeautifulSoup(html, "lxml")
        if page_num == 1:
            result["total_reported"] = _extract_total_job_count(soup)
        result["jobs"] = _parse_html(html, page_url)
    except Exception as ex:
        log.error("Listing page %s failed: %s", page_num, ex)
        result["error"] = str(ex)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
    return result


def _scrape_pages_worker(page_nums: list[int], base_url: str) -> dict[int, dict]:
    """Fetch multiple listing pages inside ONE browser instance.

    This keeps the number of Chrome openings ~= `workers` (not `max_pages`).
    """
    driver = None
    results: dict[int, dict] = {}
    try:
        for page_num in page_nums:
            page_url = listing_page_url(base_url, page_num)
            result: dict = {"page": page_num, "jobs": [], "total_reported": None, "error": None}

            html = None
            try:
                # Prefer HTTP: on restricted WiFi this avoids Selenium Manager/webdriver-manager delays.
                html = _fetch_html_requests(page_url)
                if not html:
                    if driver is None:
                        try:
                            driver = _build_driver()
                        except RuntimeError:
                            driver = None
                    if driver is not None:
                        html = _get_html(driver, page_url)
            except Exception as ex:
                result["error"] = str(ex)
                results[page_num] = result
                continue

            if not html:
                result["error"] = "no html"
                results[page_num] = result
                continue

            soup = BeautifulSoup(html, "lxml")
            if page_num == 1:
                result["total_reported"] = _extract_total_job_count(soup)

            result["jobs"] = _parse_html(html, page_url)
            results[page_num] = result
    except Exception as exc:
        err = str(exc)
        for p in page_nums:
            results[p] = {"page": p, "jobs": [], "total_reported": None, "error": err}
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    return results


def scrape_url(url: str, max_pages: int = 1, workers: int = 1) -> dict:
    """
    Load listing page(s) and extract job rows from card HTML only (no per-job URLs stored).

    Pagination: Naukrigulf uses in-browser clicks (2, 3, …) except the Dammam/Khobar/
    Eastern Province composite listing, which uses path URLs …-2, …-3 only.

    workers > 1: used only for non–click-mode sites (parallel listing fetches).
    """
    try:
        max_pages = int(max_pages)
    except (TypeError, ValueError):
        max_pages = 1
    max_pages = max(1, min(max_pages, 1000))

    try:
        workers = int(workers)
    except (TypeError, ValueError):
        workers = 1
    workers = max(1, min(workers, 5))

    # GulfTalent /jobs/search is a SPA: same document, pagination in-page. Use one browser
    # and click 2, 3, … — do not open parallel tabs or rely on ?page= URLs alone.
    if _is_gulftalent_search_url(url):
        log.info(
            "GulfTalent /jobs/search: single Chrome session, click pagination (workers=%s ignored).",
            workers,
        )
        return _scrape_gulftalent_search_click_pagination(url, max_pages)

    if _is_bayt_url(url):
        log.info(
            "Bayt: single Chrome session, click pagination (workers=%s ignored).",
            workers,
        )
        return _scrape_bayt_click_pagination(url, max_pages)

    if _is_naukrigulf_url(url):
        if _is_naukrigulf_dammam_composite_listing(url):
            log.info(
                "Naukrigulf (Dammam composite): URL pagination only (workers=%s ignored).",
                workers,
            )
            return _scrape_naukrigulf_dammam_url_pagination(url, max_pages)
        log.info(
            "Naukrigulf: single Chrome session, click pagination (workers=%s ignored).",
            workers,
        )
        return _scrape_naukrigulf_click_pagination(url, max_pages)

    all_jobs: list[dict] = []
    seen: set[str] = set()
    pages_scraped = 0
    total_jobs_reported: Optional[int] = None

    # Parallel: one worker per browser instance (capped by workers).
    if workers > 1 and max_pages > 1:
        log.info(
            "Scraping listings (parallel): %s — %s page(s), %s worker(s)",
            url,
            max_pages,
            workers,
        )
        # Fetch page 1 once to extract the "Showing X Jobs" header total.
        # This makes `total_jobs_reported` independent of `workers`.
        driver = None
        header_used_browser = False
        try:
            page1_url = listing_page_url(url, 1)
            html = None
            # Prefer HTTP for the header total so we don't open an extra Chrome.
            # On restricted WiFi, this must fail fast so we don't wait 30-35s.
            html = _fetch_html_requests(page1_url, timeout=5)
            if not html:
                # Fallback: use browser only if HTTP didn't return HTML.
                try:
                    driver = _build_driver()
                    html = _get_html(driver, page1_url)
                    header_used_browser = True
                except Exception as err:
                    log.warning("Header parse fallback failed: %s", err)
                    driver = None

            if not html:
                return {
                    "jobs": [],
                    "count": 0,
                    "pages_scraped": 0,
                    "total_jobs_reported": None,
                    "error": "Failed to load page 1 (browser and HTTP both failed)",
                }

            soup = BeautifulSoup(html, "lxml")
            if _is_gulftalent_url(page1_url):
                total_jobs_reported = _extract_total_job_count_gulftalent(soup)
            elif _is_bayt_url(page1_url):
                total_jobs_reported = _extract_total_job_count_bayt(soup)
            else:
                total_jobs_reported = _extract_total_job_count(soup)
            batch = _parse_html(html, page1_url)

            pages_scraped = 1
            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

        remaining_pages = list(range(2, max_pages + 1))
        if not remaining_pages:
            return {
                "jobs": all_jobs,
                "count": len(all_jobs),
                "pages_scraped": pages_scraped,
                "total_jobs_reported": total_jobs_reported,
                "error": None,
            }

        # If page 1 already needed the browser, it's usually because the listing pages
        # are not reliably fetchable via HTTP on this WiFi. In that case, skip the
        # HTTP-only timeout phase and go straight to browser fetching for 2..N.
        if header_used_browser:
            pool = min(workers, len(remaining_pages))
            chunks = [remaining_pages[i::pool] for i in range(pool)]

            def _browser_chunk_worker(page_nums: list[int]) -> dict:
                d = None
                out_jobs: list[dict] = []
                try:
                    d = _build_driver()
                    for p in page_nums:
                        page_url = listing_page_url(url, p)
                        html = _get_html(d, page_url)
                        if not html:
                            continue
                        batch = _parse_html(html, page_url)
                        for j in batch:
                            if j.get("title") and j.get("company"):
                                out_jobs.append(j)
                finally:
                    if d is not None:
                        try:
                            d.quit()
                        except Exception:
                            pass
                return {"jobs": out_jobs, "pages_max": max(page_nums) if page_nums else 1}

            chunk_results: list[dict] = []
            with ThreadPoolExecutor(max_workers=pool) as ex:
                futs = [ex.submit(_browser_chunk_worker, chunk) for chunk in chunks if chunk]
                for fut in as_completed(futs):
                    chunk_results.append(fut.result())

            for r in chunk_results:
                batch = r.get("jobs") or []
                if batch:
                    pages_scraped = max(pages_scraped, int(r.get("pages_max", 0)) or 0)
                for j in batch:
                    key = _job_dedupe_key(j)
                    if key in seen:
                        continue
                    seen.add(key)
                    all_jobs.append(j)

            return {
                "jobs": all_jobs,
                "count": len(all_jobs),
                "pages_scraped": pages_scraped or max_pages,
                "total_jobs_reported": total_jobs_reported,
                "error": None,
            }

        # Two-stage parallelism:
        # 1) HTTP-only in parallel for speed and to avoid driver downloads on restricted WiFi.
        # 2) Only if some pages fail HTTP, build ChromeDriver once and fetch those pages sequentially.
        def _http_only_page(page_num: int) -> dict:
            page_url = listing_page_url(url, page_num)
            html = _fetch_html_requests(page_url)
            if not html:
                return {"page": page_num, "jobs": [], "error": "http_failed", "need_browser": True}
            try:
                jobs = _parse_html(html, page_url)
                return {"page": page_num, "jobs": jobs, "error": None, "need_browser": False}
            except Exception as ex:
                return {"page": page_num, "jobs": [], "error": str(ex), "need_browser": True}

        pool = min(workers, len(remaining_pages))
        pages_need_browser: list[int] = []
        http_results: dict[int, list[dict]] = {}

        with ThreadPoolExecutor(max_workers=pool) as ex:
            futs = {ex.submit(_http_only_page, p): p for p in remaining_pages}
            for fut in as_completed(futs):
                pnum = futs[fut]
                try:
                    r = fut.result()
                    if r.get("need_browser"):
                        pages_need_browser.append(pnum)
                    else:
                        http_results[pnum] = r.get("jobs") or []
                except Exception as exn:
                    pages_need_browser.append(pnum)

        for p in sorted(http_results.keys()):
            batch = http_results[p] or []
            if batch:
                pages_scraped = max(pages_scraped, p)
            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)

        if pages_need_browser:
            driver = None
            try:
                driver = _build_driver()
            except Exception as err:
                log.warning("Browser fallback failed (restricted WiFi): %s", err)
                # Return what we have from HTTP; error is optional.
                return {
                    "jobs": all_jobs,
                    "count": len(all_jobs),
                    "pages_scraped": pages_scraped or 0,
                    "total_jobs_reported": total_jobs_reported,
                    "error": f"browser_fallback_failed: {err}",
                }

            try:
                for p in sorted(set(pages_need_browser)):
                    page_url = listing_page_url(url, p)
                    html = _get_html(driver, page_url)
                    if not html:
                        continue
                    batch = _parse_html(html, page_url)
                    if batch:
                        pages_scraped = max(pages_scraped, p)
                    for j in batch:
                        key = _job_dedupe_key(j)
                        if key in seen:
                            continue
                        seen.add(key)
                        all_jobs.append(j)
            finally:
                try:
                    driver.quit()
                except Exception:
                    pass

        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped or max_pages,
            "total_jobs_reported": total_jobs_reported,
            "error": None,
        }

    # Sequential: single browser, lighter on RAM.
    log.info("Scraping listings (sequential): %s — %s page(s)", url, max_pages)
    driver = None
    try:
        for page in range(1, max_pages + 1):
            page_url = listing_page_url(url, page)
            log.info("Listing page %s/%s: %s", page, max_pages, page_url)

            html = None
            try:
                # Prefer HTTP first; only open Chrome if HTML fetch fails.
                # For page 1, fail fast so the "first chrome open" doesn't wait on a long HTTP timeout.
                html = _fetch_html_requests(page_url, timeout=5 if page == 1 else 35)
                if not html and driver is None:
                    try:
                        driver = _build_driver()
                    except RuntimeError as err:
                        log.warning("Chrome/WebDriver unavailable (fallback to HTTP only): %s", err)
                        driver = None
                if not html and driver is not None:
                    html = _get_html(driver, page_url)
            except Exception as ex:
                log.error("Failed to load %s: %s", page_url, ex)
                break

            if not html:
                if page == 1:
                    return {
                        "jobs": [],
                        "count": 0,
                        "pages_scraped": 0,
                        "total_jobs_reported": None,
                        "error": "Failed to load page (browser and HTTP both failed)",
                    }
                break

            soup = BeautifulSoup(html, "lxml")
            if page == 1:
                if _is_gulftalent_url(page_url):
                    total_jobs_reported = _extract_total_job_count_gulftalent(soup)
                elif _is_bayt_url(page_url):
                    total_jobs_reported = _extract_total_job_count_bayt(soup)
                else:
                    total_jobs_reported = _extract_total_job_count(soup)

            batch = _parse_html(html, page_url)
            pages_scraped = page

            for j in batch:
                key = _job_dedupe_key(j)
                if key in seen:
                    continue
                seen.add(key)
                all_jobs.append(j)

            if page > 1 and not batch:
                log.info("No jobs on page %s — stopping.", page)
                break

        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": None,
        }
    except Exception as exc:
        log.error("Scrape error: %s", exc)
        return {
            "jobs": all_jobs,
            "count": len(all_jobs),
            "pages_scraped": pages_scraped,
            "total_jobs_reported": total_jobs_reported,
            "error": str(exc),
        }
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass