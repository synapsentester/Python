import csv
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

import requests

# --- Abhängigkeiten prüfen und ggf. installieren ---
REQUIRED_PACKAGES = ["playwright", "tqdm"]


def ensure_dependencies():
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
        except ImportError:
            print(f"Fehlende Abhängigkeit '{pkg}' wird installiert...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

    # Playwright-Browser installieren
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
    except ImportError:
        print("Playwright konnte nicht importiert werden.")
    else:
        print("Installiere Playwright-Browser...")
        subprocess.check_call(
            [sys.executable, "-m", "playwright", "install", "chromium"]
        )


ensure_dependencies()

from playwright.sync_api import sync_playwright
from tqdm import tqdm


# Pfad zum Skript-Ordner
script_dir = os.path.dirname(os.path.abspath(__file__))

# --- Konfiguration aus JSON laden ---
CONFIG_FILE = os.path.join(script_dir, "config.json")

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

default_download_dir = os.getenv("TEMP") or tempfile.gettempdir()
DOWNLOAD_DIR = config.get("download_dir", default_download_dir)
PAGES = config.get("pages", {})
ARCHIV_DIR = config.get("archiv_dir")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Log-Dateien vorbereiten ---
timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(DOWNLOAD_DIR, f"download_log_{timestamp_str}_HUMAN.log")
csv_file = os.path.join(DOWNLOAD_DIR, f"download_log_{timestamp_str}_MACHINE.csv")
ndjson_file = os.path.join(DOWNLOAD_DIR, f"download_log_{timestamp_str}_MACHINE.ndjson")


# CSV-Header schreiben (ACHTUNG: könnte gelöscht werden da auf NDJSON umgestiegen)
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "Zeitstempel",
            "Kategorie",
            "URL",
            "Web-Hash",
            "Lokaler Pfad",
            "Lokaler MD5",
            "Status",
        ]
    )
# (ACHTUNG: könnte gelöscht werden da auf NDJSON umgestiegen)

def log_console_and_file(
    message,
    category="",
    url="",
    web_hash="",
    local_path="",
    local_md5="",
    status="",
    write_csv=False,  # bleibt für Kompatibilität, wird aber ignoriert
):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Konsole und Human-Log (.log)
    console_message = f"[{timestamp}] [Kategorie: {category}] {message}"
    print(console_message)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(console_message + "\n")

    # NDJSON-Objekt schreiben
    if url or local_path:
        ndjson_entry = {
            "timestamp": timestamp,
            "category": category,
            "source_url": url,
            #"expected_hash": web_hash or "N/A",
            "expected_hash": (web_hash or "N/A").lower(),
            "local_path": local_path or "N/A",
            #"calculated_hash": local_md5 or "N/A",
            "calculated_hash": (local_md5 or "N/A").lower(),
            "status": status or "N/A",
        }
        try:
            with open(ndjson_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(ndjson_entry, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"Fehler beim Schreiben des NDJSON-Logs: {e}")



# --- Datei herunterladen ---
def download_file(url, dest, category="", web_hash=None):
    """Download mit Fortschrittsanzeige, MD5, Log in Konsole/.log + CSV"""
    r = requests.get(url, stream=True)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        desc=os.path.basename(dest),
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in r.iter_content(chunk_size=1024):
            size = f.write(data)
            bar.update(size)

    # MD5 berechnen
    with open(dest, "rb") as f:
        md5_hash = hashlib.md5(f.read()).hexdigest()

    # Hashvergleich
    if web_hash:
        hash_status = (
            "HASH stimmt"
            if md5_hash.lower() == web_hash.lower()
            else "HASH stimmt nicht"
        )
    else:
        web_hash = "N/A"
        hash_status = "HASH unbekannt"

    # Log: nur hier CSV schreiben
    message = f"Heruntergeladen: {dest}, MD5: {md5_hash}, {hash_status}"
    log_console_and_file(
        message,
        category=category,
        url=url,
        web_hash=web_hash,
        local_path=dest,
        local_md5=md5_hash,
        status=hash_status,
        write_csv=True,
    )


# --- Playwright-Teil ---
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # Vor dem Download: Download-Ordner leeren, falls Dateien vorhanden sind
    for filename in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Fehler beim Löschen von {file_path}: {e}")

    for category, url in PAGES.items():
        log_console_and_file(f"=== Kategorie: {category} ===", category=category)

        page.goto(url)
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(3000)

        anchors = page.query_selector_all("a")
        jdb_links = []

        for a in anchors:
            href = a.get_attribute("href")
            if href and href.lower().endswith(".jdb"):
                file_url = (
                    href
                    if href.startswith("http")
                    else page.url.rstrip("/") + "/" + href.lstrip("/")
                )

                # Übergeordneten Container suchen, der auch den Hash enthält
                parent_handle = a.evaluate_handle(
                    "node => node.closest('tr') || node.parentElement"
                )
                inner_text = (
                    parent_handle.evaluate("node => node.innerText")
                    if parent_handle
                    else ""
                )

                # MD5 aus Text extrahieren
                match = re.search(r"\b[a-fA-F0-9]{32}\b", inner_text)
                web_hash = match.group(0) if match else None

                jdb_links.append((file_url, web_hash))

        if not jdb_links:
            log_console_and_file(
                f"Keine .jdb-Dateien gefunden auf {category} ({url})", category=category
            )
            continue

        for link, web_hash in jdb_links:
            filename = os.path.join(DOWNLOAD_DIR, link.split("/")[-1])
            # Nur Konsole-Info, keine CSV-Zeile vor dem Download
            print(
                f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starte Download: {filename}"
            )
            download_file(link, filename, category=category, web_hash=web_hash)

    log_console_and_file("Alle Downloads abgeschlossen.")
    browser.close()


# Log-Datei im Standardprogramm öffnen (Windows)
os.startfile(log_file)
# Download-Ordner im Explorer öffnen (Windows)
os.startfile(DOWNLOAD_DIR)

# Logdateien ins Archiv kopieren
os.makedirs(ARCHIV_DIR, exist_ok=True)
shutil.copy2(csv_file, ARCHIV_DIR)
shutil.copy2(ndjson_file, ARCHIV_DIR)

# ...TSCHÜSS...
