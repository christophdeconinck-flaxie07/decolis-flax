#!/usr/bin/env python3
"""
Hourly prefetch — only updates the 'today' period (since midnight Brussels).
Merges into existing historical.json so other periods stay untouched.
Runtime: ~10-30 seconds.
"""

import json
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

REGION_DEFS = [
    {"name": "76 - Seine-Maritime", "country": "FR", "bbox": [49.28, 0.07, 50.07, 1.78], "size": 30},
    {"name": "27 - Eure", "country": "FR", "bbox": [48.67, 0.30, 49.48, 1.80], "size": 20},
    {"name": "14 - Calvados", "country": "FR", "bbox": [48.76, -1.17, 49.40, 0.45], "size": 15},
    {"name": "Vexin Normand", "country": "FR", "bbox": [49.20, 1.30, 49.65, 2.05], "size": 12},
    {"name": "59 - Nord", "country": "FR", "bbox": [50.10, 2.55, 51.08, 4.23], "size": 20},
    {"name": "62 - Pas-de-Calais", "country": "FR", "bbox": [50.02, 1.55, 50.95, 3.20], "size": 20},
    {"name": "80 - Somme", "country": "FR", "bbox": [49.57, 1.38, 50.37, 3.18], "size": 15},
    {"name": "02 - Aisne", "country": "FR", "bbox": [48.83, 3.02, 49.96, 4.25], "size": 20},
    {"name": "60 - Oise", "country": "FR", "bbox": [49.10, 1.68, 49.78, 3.18], "size": 15},
    {"name": "77 - Seine-et-Marne", "country": "FR", "bbox": [48.12, 2.38, 49.13, 3.55], "size": 20},
    {"name": "95 - Val-d'Oise", "country": "FR", "bbox": [48.93, 1.62, 49.23, 2.60], "size": 8},
    {"name": "West Flanders (Westhoek/Lys/Polders)", "country": "BE", "bbox": [50.68, 2.55, 51.37, 3.42], "size": 15},
    {"name": "East Flanders (NL border + Oudenaarde)", "country": "BE", "bbox": [50.72, 3.43, 51.37, 4.23], "size": 15},
    {"name": "Flemish Brabant (Tienen/Hageland)", "country": "BE", "bbox": [50.70, 4.60, 50.95, 5.22], "size": 15},
    {"name": "Limburg (Haspengouw)", "country": "BE", "bbox": [50.70, 5.10, 51.00, 5.65], "size": 8},
    {"name": "Hainaut (Tournai/Ath/Mons)", "country": "BE", "bbox": [50.25, 3.25, 50.77, 4.43], "size": 15},
    {"name": "Walloon Brabant (Nivelles/Gembloux)", "country": "BE", "bbox": [50.45, 4.22, 50.78, 4.80], "size": 8},
    {"name": "Liège (Hesbaye plain)", "country": "BE", "bbox": [50.55, 5.05, 50.80, 5.50], "size": 15},
    {"name": "Namur (loam region)", "country": "BE", "bbox": [50.35, 4.55, 50.60, 5.15], "size": 15},
    {"name": "Condroz", "country": "BE", "bbox": [50.15, 4.75, 50.45, 5.45], "size": 15},
    {"name": "Zeeland", "country": "NL", "bbox": [51.22, 3.37, 51.67, 4.30], "size": 15},
    {"name": "Flevoland", "country": "NL", "bbox": [52.22, 5.15, 52.75, 5.90], "size": 8},
]

PARALLEL_WORKERS = 3
HTTP_TIMEOUT = 60
MAX_RETRIES = 3


def get_brussels_today_start_utc():
    """Returns UTC datetime corresponding to today 00:00 Brussels time."""
    now_utc = datetime.now(timezone.utc)
    month = now_utc.month
    is_summer = month in [4, 5, 6, 7, 8, 9]
    offset_hours = 2 if is_summer else 1
    local_now = now_utc + timedelta(hours=offset_hours)
    local_today_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = local_today_start - timedelta(hours=offset_hours)
    return today_start_utc


def generate_grid(bbox, n):
    south, west, north, east = bbox
    lat_span = north - south
    lon_span = east - west
    cols = max(2, round((n * lon_span / lat_span) ** 0.5))
    rows = max(2, round(n / cols))
    points = []
    for i in range(rows):
        for j in range(cols):
            lat = south + (i + 0.5) / rows * lat_span
            lon = west + (j + 0.5) / cols * lon_span
            points.append([round(lat, 4), round(lon, 4)])
    return points


def http_get_json(url):
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'flax-monitor-prefetch/2.0'})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429 and attempt < MAX_RETRIES:
                time.sleep(10 * (2 ** attempt))
            else:
                raise
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(3)
            else:
                raise
    raise last_err


def fetch_today_region_bulk(region):
    """BULK call for today's data (since midnight Brussels) of one region."""
    points = region['points']
    lats = ",".join(str(p[0]) for p in points)
    lons = ",".join(str(p[1]) for p in points)
    tz = urllib.parse.quote("Europe/Brussels")
    # past_days=1 gives us yesterday + today, which covers any timezone edge
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}"
           f"&hourly=precipitation,temperature_2m,et0_fao_evapotranspiration"
           f"&past_days=1&forecast_days=1&timezone={tz}")
    response = http_get_json(url)
    if isinstance(response, dict):
        response = [response]

    today_start = get_brussels_today_start_utc()
    now = datetime.now(timezone.utc)

    point_results = []
    for point_data in response:
        try:
            if 'hourly' not in point_data or 'precipitation' not in point_data['hourly']:
                point_results.append(None)
                continue
            precip, et0, t_sum, t_max, count = 0, 0, 0, -999, 0
            for i, t_str in enumerate(point_data['hourly']['time']):
                # Open-Meteo returns local time (Brussels) when timezone param is set
                # Convert local naive datetime to UTC for comparison
                t_local_naive = datetime.fromisoformat(t_str)
                # Apply Brussels offset based on month (matches our get_brussels_today_start logic)
                month_check = t_local_naive.month
                offset_h = 2 if month_check in [4, 5, 6, 7, 8, 9] else 1
                t_utc = t_local_naive.replace(tzinfo=timezone.utc) - timedelta(hours=offset_h)

                # Include hours from today_start up to now
                if today_start <= t_utc <= now:
                    p = point_data['hourly']['precipitation'][i]
                    e_arr = point_data['hourly'].get('et0_fao_evapotranspiration')
                    e = e_arr[i] if e_arr else None
                    temp_arr = point_data['hourly'].get('temperature_2m')
                    temp = temp_arr[i] if temp_arr else None
                    if p is not None: precip += p
                    if e is not None: et0 += e
                    if temp is not None:
                        t_sum += temp
                        count += 1
                        if temp > t_max: t_max = temp
            if count == 0:
                # Voor de allereerste run kort na middernacht is er nog geen data — return null
                point_results.append({"precip": 0, "et0": 0, "tempMean": None, "tempMax": None, "balance": 0})
            else:
                point_results.append({
                    "precip": precip, "et0": et0,
                    "tempMean": t_sum/count,
                    "tempMax": t_max if t_max != -999 else None,
                    "balance": precip - et0
                })
        except Exception as e:
            point_results.append({"_error": str(e)[:80]})
    return point_results


def safe_call(fn, *args):
    try:
        return fn(*args)
    except Exception:
        return None


def aggregate_region(points_data):
    if not points_data:
        return None
    valid = [p for p in points_data if p is not None and not (isinstance(p, dict) and "_error" in p)]
    if not valid:
        return None
    n = len(valid)

    def avg_or_none(key):
        vals = [v[key] for v in valid if v.get(key) is not None]
        return sum(vals)/len(vals) if vals else None

    return {
        "precip": sum(v["precip"] for v in valid) / n,
        "et0": sum(v["et0"] for v in valid) / n,
        "balance": sum(v["balance"] for v in valid) / n,
        "tempMean": avg_or_none("tempMean"),
        "tempMax": avg_or_none("tempMax"),
        "precipMin": min(v["precip"] for v in valid),
        "precipMax": max(v["precip"] for v in valid),
        "successCount": n,
        "totalCount": len(points_data)
    }


def main():
    start = time.time()
    print("=== Hourly Today Prefetch ===", flush=True)
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}", flush=True)
    today_start = get_brussels_today_start_utc()
    print(f"Today started at (UTC): {today_start.isoformat()}", flush=True)
    hours_since = (datetime.now(timezone.utc) - today_start).total_seconds() / 3600
    print(f"Hours since midnight Brussels: {hours_since:.1f}", flush=True)

    regions = []
    for r in REGION_DEFS:
        regions.append({**r, "points": generate_grid(r["bbox"], r["size"])})

    print(f"{len(regions)} regions, fetching today's data only", flush=True)

    hist_path = Path("data/historical.json")
    if not hist_path.exists():
        print("⚠️ data/historical.json not found - daily run must create it first", flush=True)
        sys.exit(0)

    with open(hist_path, "r", encoding="utf-8") as f:
        existing = json.load(f)

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        future_to_region = {
            ex.submit(safe_call, fetch_today_region_bulk, region): region
            for region in regions
        }
        results_by_region = {}
        for fut in as_completed(future_to_region):
            region = future_to_region[fut]
            results_by_region[region['name']] = fut.result()

    new_today = {}
    ok_count = 0
    for region in regions:
        agg = aggregate_region(results_by_region.get(region['name']))
        if agg:
            new_today[region['name']] = agg
            ok_count += 1
        else:
            new_today[region['name']] = {"error": "all points failed"}

    if "data" not in existing:
        existing["data"] = {}
    # We keep the JSON key as "24h" for backwards compatibility with the HTML
    # but the meaning is now "today since midnight Brussels"
    existing["data"]["24h"] = new_today
    existing["timestamp"] = datetime.now(timezone.utc).isoformat()

    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, separators=(',', ':'))

    print(f"\n✓ Updated 'today' data in historical.json ({ok_count}/{len(regions)} regions ok)", flush=True)
    print(f"Total time: {time.time()-start:.1f}s", flush=True)


if __name__ == "__main__":
    main()
