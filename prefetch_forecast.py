#!/usr/bin/env python3
"""
Forecast-only prefetch — runs 2x per day to catch both ECMWF model runs.
Updates forecast.json with fresh ECMWF + GFS 8-day forecast.
Runtime: ~80 seconds.
"""

import json
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

REGION_DEFS = [
    {"name": "76 - Seine-Maritime", "country": "FR", "bbox": [49.28, 0.07, 50.07, 1.78], "size": 30},
    {"name": "27 - Eure", "country": "FR", "bbox": [48.67, 0.30, 49.48, 1.80], "size": 25},
    {"name": "14 - Calvados", "country": "FR", "bbox": [48.76, -1.17, 49.40, 0.45], "size": 20},
    {"name": "Vexin Normand", "country": "FR", "bbox": [49.20, 1.30, 49.65, 2.05], "size": 12},
    {"name": "59 - Nord", "country": "FR", "bbox": [50.10, 2.55, 51.08, 4.23], "size": 20},
    {"name": "62 - Pas-de-Calais", "country": "FR", "bbox": [50.02, 1.55, 50.95, 3.20], "size": 20},
    {"name": "80 - Somme", "country": "FR", "bbox": [49.57, 1.38, 50.37, 3.18], "size": 20},
    {"name": "02 - Aisne", "country": "FR", "bbox": [48.83, 3.02, 49.96, 4.25], "size": 20},
    {"name": "60 - Oise", "country": "FR", "bbox": [49.10, 1.68, 49.78, 3.18], "size": 15},
    {"name": "77 - Seine-et-Marne", "country": "FR", "bbox": [48.12, 2.38, 49.13, 3.55], "size": 20},
    {"name": "95 - Val-d'Oise", "country": "FR", "bbox": [48.93, 1.62, 49.23, 2.60], "size": 8},
    {"name": "West Flanders (Westhoek/Lys/Polders)", "country": "BE", "bbox": [50.65, 2.55, 51.37, 3.45], "size": 35},
    {"name": "East Flanders (NL border + Oudenaarde)", "country": "BE", "bbox": [50.72, 3.43, 51.37, 4.30], "size": 18},
    {"name": "Flemish Brabant (Tienen/Hageland)", "country": "BE", "bbox": [50.70, 4.45, 50.95, 5.22], "size": 18},
    {"name": "Limburg (Haspengouw)", "country": "BE", "bbox": [50.70, 5.10, 51.20, 5.85], "size": 12},
    {"name": "Hainaut (Tournai/Ath/Mons)", "country": "BE", "bbox": [50.20, 3.25, 50.77, 4.43], "size": 25},
    {"name": "Walloon Brabant (Nivelles/Gembloux)", "country": "BE", "bbox": [50.45, 4.22, 50.78, 4.95], "size": 10},
    {"name": "Liège (Hesbaye plain)", "country": "BE", "bbox": [50.48, 5.00, 50.80, 5.55], "size": 20},
    {"name": "Namur (loam region)", "country": "BE", "bbox": [50.30, 4.55, 50.65, 5.15], "size": 20},
    {"name": "Condroz", "country": "BE", "bbox": [50.15, 4.30, 50.55, 5.45], "size": 15},
    {"name": "Entre-Sambre-et-Meuse", "country": "BE", "bbox": [49.90, 4.20, 50.30, 4.85], "size": 12},
    {"name": "Zeeland", "country": "NL", "bbox": [51.22, 3.37, 51.67, 4.30], "size": 15},
    {"name": "Flevoland", "country": "NL", "bbox": [52.22, 5.15, 52.75, 5.90], "size": 8},
]

PARALLEL_WORKERS = 3
HTTP_TIMEOUT = 60
MAX_RETRIES = 3


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


def fetch_forecast_region_bulk(region, model):
    """BULK forecast call: all region points in one request."""
    points = region['points']
    lats = ",".join(str(p[0]) for p in points)
    lons = ",".join(str(p[1]) for p in points)
    tz = urllib.parse.quote("Europe/Brussels")
    model_param = "ecmwf_ifs025" if model == "ecmwf" else "gfs_seamless"
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lats}&longitude={lons}"
           f"&daily=precipitation_sum,precipitation_probability_max,temperature_2m_max,temperature_2m_min"
           f"&models={model_param}&forecast_days=8&timezone={tz}")
    response = http_get_json(url)
    if isinstance(response, dict):
        response = [response]

    point_results = []
    for point_data in response:
        try:
            d = point_data.get('daily', {})
            point_results.append({
                "dates": d.get('time', []),
                "precip": d.get('precipitation_sum', []),
                "prob": d.get('precipitation_probability_max', []),
                "tmax": d.get('temperature_2m_max', []),
                "tmin": d.get('temperature_2m_min', [])
            })
        except Exception as e:
            point_results.append({"_error": str(e)[:80]})
    return point_results


def safe_call(fn, *args):
    try:
        return fn(*args)
    except Exception:
        return None


def aggregate_forecast_region(points_data):
    if not points_data:
        return None
    valid = [p for p in points_data if p and not (isinstance(p, dict) and "_error" in p) and p.get('dates')]
    if not valid:
        return None
    dates = valid[0]['dates']
    n_days = len(dates)
    out = {"dates": dates, "precip": [], "prob": [], "tmax": [], "tmin": []}
    for d in range(n_days):
        for key in ['precip', 'prob', 'tmax', 'tmin']:
            vals = [p[key][d] for p in valid if d < len(p[key]) and p[key][d] is not None]
            out[key].append(sum(vals)/len(vals) if vals else None)
    return out


def main():
    start = time.time()
    print("=== Forecast-Only Prefetch ===", flush=True)
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}", flush=True)

    regions = []
    for r in REGION_DEFS:
        regions.append({**r, "points": generate_grid(r["bbox"], r["size"])})

    print(f"{len(regions)} regions, fetching forecast only (ECMWF + GFS, 8 days)", flush=True)

    tasks = []
    for region in regions:
        tasks.append((region, "ecmwf"))
        tasks.append((region, "gfs"))

    forecast_data = {r['name']: {"ecmwf": None, "gfs": None} for r in regions}
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
        future_to_task = {
            ex.submit(safe_call, fetch_forecast_region_bulk, region, model): (region['name'], model)
            for (region, model) in tasks
        }
        for fut in as_completed(future_to_task):
            region_name, model = future_to_task[fut]
            forecast_data[region_name][model] = fut.result()

    forecast_per_region = {}
    forecast_dates = None
    for region in regions:
        ecmwf_agg = aggregate_forecast_region(forecast_data[region['name']]['ecmwf'])
        gfs_agg = aggregate_forecast_region(forecast_data[region['name']]['gfs'])
        if ecmwf_agg:
            forecast_per_region[region['name']] = {"ecmwf": ecmwf_agg, "gfs": gfs_agg}
            if not forecast_dates:
                forecast_dates = ecmwf_agg['dates']

    if not forecast_dates:
        print("⚠️ No forecast data retrieved", flush=True)
        sys.exit(1)

    forecast = {"dates": forecast_dates, "regions": forecast_per_region}
    timestamp = datetime.now(timezone.utc).isoformat()

    Path("data").mkdir(exist_ok=True)
    with open("data/forecast.json", "w", encoding="utf-8") as f:
        json.dump({"timestamp": timestamp, "data": forecast}, f, separators=(',', ':'))

    print(f"\n✓ Updated forecast.json ({len(forecast_per_region)}/{len(regions)} regions)", flush=True)
    print(f"Total time: {time.time()-start:.1f}s", flush=True)


if __name__ == "__main__":
    main()
