from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from google.cloud import storage
import datetime
import json

app = Flask(__name__)

MAX_PAGES = 30
RAW_BUCKET = "nutraneo-brand-scrape-raw"


def clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return " ".join(soup.stripped_strings)


@app.get("/")
def health():
    return "ok", 200


@app.post("/scrape")
def scrape():
    data = request.get_json(silent=True) or {}
    start_url = data.get("website_url")

    if not start_url:
        return jsonify({"error": "website_url is required"}), 400

    parsed = urlparse(start_url)
    domain = parsed.netloc.replace("www.", "")
    if not domain:
        return jsonify({"error": "Invalid website_url"}), 400

    visited = set()
    queue = [start_url]
    pages = []

    headers = {"User-Agent": "nutraneo-site-scraper/1.0"}

    while queue and len(visited) < MAX_PAGES:
        url = queue.pop(0)
        if url in visited:
            continue

        try:
            resp = requests.get(url, headers=headers, timeout=12)
            ctype = resp.headers.get("Content-Type", "")

            if resp.status_code != 200 or "text/html" not in ctype:
                visited.add(url)
                continue

            visited.add(url)

            html = resp.text
            text = clean_text(html)

            pages.append({"url": url, "text": text})

            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                next_url = urljoin(url, a["href"])
                nparsed = urlparse(next_url)

                if nparsed.scheme not in ("http", "https"):
                    continue

                if nparsed.netloc.replace("www.", "") != domain:
                    continue

                normalized = next_url.split("#")[0].rstrip("/")
                if normalized and normalized not in visited:
                    queue.append(normalized)

        except Exception:
            visited.add(url)
            continue

    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    object_path = f"scrapes/{domain}/{timestamp}.json"

    payload = {
        "domain": domain,
        "started_url": start_url,
        "pages_scraped": len(pages),
        "max_pages": MAX_PAGES,
        "pages": pages,
        "created_utc": timestamp,
    }

    storage_client = storage.Client()
    bucket = storage_client.bucket(RAW_BUCKET)
    blob = bucket.blob(object_path)
    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False),
        content_type="application/json",
    )

    return jsonify(
        {
            "domain": domain,
            "pages_scraped": len(pages),
            "gcs_path": f"gs://{RAW_BUCKET}/{object_path}",
        }
    ), 200
