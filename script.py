import os
import json
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

# SETTINGS
START_ID = 150000
END_ID = 176100
MAX_THREADS = 40
OUTPUT_DIR = "downloaded_threads"
SESSION_COOKIE = "XCdBD4ZUOpLgqBx49JOyVvMnRKjKmHjvQu1%2FdSBTUmF6sxFW%2F27rnOU9vF3KXJaa5AfP18t0PyxnsHUrBcTilQzLFv2v8S2ghkbwN38QT7wmWLDILVm9edNipJBTRfbkUv6RCctpJlEseeYYaa19Umc3u6k2Yku2d2DzeYzPsxQruhmrxEwnIeMaJYMkPla72N0ttn4JxkM2HWxCqhH3ijT%2B5cpL8A5njHhIEVDsHC0iraGOoMaNOstomkMtzVEWRvNhoH6qPS6Nif%2Bwo3znbHEeeDCLYw%3D%3D--%2FDdTX2T4hUXznS5Z--QrhGyRjjugxsVnxovp6gmg%3D%3D"
DATE_LIMIT_START = datetime(2025, 1, 1)
DATE_LIMIT_END = datetime(2025, 4, 14)

# HEADERS
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Cookie": f"_forum_session={SESSION_COOKIE}",
}

# Ensure output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def is_within_date_range(created_at):
    try:
        post_date = datetime.strptime(created_at[:10], "%Y-%m-%d")
        return DATE_LIMIT_START <= post_date <= DATE_LIMIT_END
    except:
        return False

def download_topic(tid):
    url = f"https://discourse.onlinedegree.iitm.ac.in/t/{tid}.json"
    try:
        path = os.path.join(OUTPUT_DIR, f"{tid}.json")
        if os.path.exists(path):
            return f"⏭️ Skipped {tid}"

        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            created_at = data.get("created_at") or data.get("post_stream", {}).get("posts", [{}])[0].get("created_at", "")
            if is_within_date_range(created_at):
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                return f"✅ {tid}"
        return f"❌ {tid} ({response.status_code})"
    except Exception as e:
        return f"⚠️ {tid} (Exception: {str(e)})"

def parallel_scrape():
    print(f"🚀 Starting parallel scrape: {START_ID}–{END_ID} using {MAX_THREADS} threads")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(download_topic, tid): tid for tid in range(START_ID, END_ID + 1)}
        for f in tqdm(as_completed(futures), total=len(futures)):
            result = f.result()
            if result.startswith("✅"):
                tqdm.write(result)

if __name__ == "__main__":
    start = time.time()
    parallel_scrape()
    print(f"\n✅ Done in {int(time.time() - start)} seconds.")
