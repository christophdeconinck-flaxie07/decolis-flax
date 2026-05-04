#!/usr/bin/env python3
"""
Prefetch weather data for Flax Rainfall Monitor (BULK version).
Uses Open-Meteo's multi-coordinate endpoint: 1 call per region (instead of per point).
Target runtime: 2-4 minutes.
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

PARALLEL_WORKERS = 3  # Reduced to avoid Open-Meteo per-minute rate limit
HTTP_TIMEOUT = 60  # seconds - bulk responses are bigger
MAX_RETRIES = 3
PERIOD_PAUSE = 5  # seconds pause between historical periods to respect rate limits


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
                # Exponentiële backoff: 10s, 20s, 40s
                wait = 10 * (2 ** attempt)
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(3)
            else:
                raise
    raise last_err if last_err else Exception("Unknown error")


def fetch_historical_region_bulk(region, period):
    """
    BULK call: één request voor alle punten van een regio.
    Returns: list of point-results (one per point, in order).
    """
    points = region['points']
    lats = ",".join(str(p[0]) for p in points)
    lons = ",".join(str(p[1]) for p in points)
    tz = urllib.parse.quote("Europe/Brussels")
    base = "https://api.open-meteo.com/v1/forecast"

    if period == "24h":
        # past_days=1 covers today + yesterday (genoeg om today since midnight te dekken)
        url = (f"{base}?latitude={lats}&longitude={lons}"
               f"&hourly=precipitation,temperature_2m,et0_fao_evapotranspiration"
               f"&past_days=1&forecast_days=1&timezone={tz}")
    elif period == "yesterday":
        url = (f"{base}?latitude={lats}&longitude={lons}"
               f"&daily=precipitation_sum,temperature_2m_mean,temperature_2m_max,et0_fao_evapotranspiration"
               f"&past_days=1&forecast_days=0&timezone={tz}")
    else:
        days_map = {"3d": 3, "5d": 5, "7d": 7, "14d": 14, "30d": 30}
        if period in days_map:
            days = days_map[period]
            url = (f"{base}?latitude={lats}&longitude={lons}"
                   f"&daily=precipitation_sum,temperature_2m_mean,temperature_2m_max,et0_fao_evapotranspiration"
                   f"&past_days={days}&forecast_days=1&timezone={tz}")
        elif period == "season":
            year = datetime.now().year
            start = f"{year}-03-01"
            end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            url = (f"https://archive-api.open-meteo.com/v1/archive?latitude={lats}&longitude={lons}"
                   f"&daily=precipitation_sum,temperature_2m_mean,temperature_2m_max,et0_fao_evapotranspiration"
                   f"&start_date={start}&end_date={end}&timezone={tz}")
        else:
            return None

    response = http_get_json(url)

    # Bulk endpoint returns either a list (multi-coord) or a dict (single coord).
    # Normalize to list:
    if isinstance(response, dict):
        response = [response]

    # Extract per-point result
    point_results = []
    for point_data in response:
        try:
            point_results.append(extract_historical_from_response(point_data, period))
        except Exception as e:
            point_results.append({"_error": str(e)[:80]})
    return point_results


def extract_historical_from_response(data, period):
    """Extract aggregated stats for one point from API response."""
    if period == "24h":
        # NOTE: key blijft "24h" voor backward compat, betekenis = today since midnight Brussels
        if 'hourly' not in data or 'precipitation' not in data['hourly']:
            return None
        today_start = get_brussels_today_start_utc()
        now = datetime.now(timezone.utc)
        precip, et0, t_sum, t_max, count = 0, 0, 0, -999, 0
        for i, t_str in enumerate(data['hourly']['time']):
            # Open-Meteo returns local time (Brussels) when timezone param is set.
            # Convert local naive datetime to UTC for comparison.
            t_local_naive = datetime.fromisoformat(t_str)
            month_check = t_local_naive.month
            offset_h = 2 if month_check in [4, 5, 6, 7, 8, 9] else 1
            t_utc = t_local_naive.replace(tzinfo=timezone.utc) - timedelta(hours=offset_h)
            if today_start <= t_utc <= now:
                p = data['hourly']['precipitation'][i]
                e_arr = data['hourly'].get('et0_fao_evapotranspiration')
                e = e_arr[i] if e_arr else None
                temp_arr = data['hourly'].get('temperature_2m')
                temp = temp_arr[i] if temp_arr else None
                if p is not None: precip += p
                if e is not None: et0 += e
                if temp is not None:
                    t_sum += temp
                    count += 1
                    if temp > t_max: t_max = temp
        if count == 0:
            # Vroeg in de ochtend: nog geen data sinds middernacht — return zero
            return {"precip": 0, "et0": 0, "tempMean": None, "tempMax": None, "balance": 0}
        return {"precip": precip, "et0": et0,
                "tempMean": t_sum/count, "tempMax": t_max if t_max != -999 else None,
                "balance": precip - et0}

    if period == "yesterday":
        d = data.get('daily', {})
        if not d.get('precipitation_sum'):
            return None
        precip = d['precipitation_sum'][0] or 0
        et0 = (d.get('et0_fao_evapotranspiration', [0])[0]) or 0
        return {"precip": precip, "et0": et0,
                "tempMean": d.get('temperature_2m_mean', [None])[0],
                "tempMax": d.get('temperature_2m_max', [None])[0],
                "balance": precip - et0}

    # daily aggregates (3d, 5d, 7d, 14d, 30d, season)
    d = data.get('daily', {})
    if not d.get('precipitation_sum'):
        return None
    precip = sum(v for v in d['precipitation_sum'] if v is not None)
    et0_list = d.get('et0_fao_evapotranspiration', []) or []
    et0 = sum(v for v in et0_list if v is not None) if et0_list else 0
    t_mean_list = [v for v in (d.get('temperature_2m_mean') or []) if v is not None]
    t_max_list = [v for v in (d.get('temperature_2m_max') or []) if v is not None]
    return {
        "precip": precip, "et0": et0,
        "tempMean": sum(t_mean_list)/len(t_mean_list) if t_mean_list else None,
        "tempMax": max(t_max_list) if t_max_list else None,
        "balance": precip - et0
    }


def fetch_forecast_region_bulk(region, model):
    """BULK forecast call: alle punten van een regio in één request."""
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
    except Exception as e:
        return None  # marks the entire region as failed


def aggregate_region(points_data, period):
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
    start_total = time.time()
    print("=== Flax Rainfall Monitor Prefetch (BULK) ===", flush=True)
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}", flush=True)

    # Connectivity test
    print("\nTesting Open-Meteo connectivity...", flush=True)
    try:
        http_get_json("https://api.open-meteo.com/v1/forecast?latitude=50&longitude=4&hourly=precipitation&forecast_days=1")
        print("  ✓ API reachable", flush=True)
    except Exception as e:
        print(f"  ✗ API test failed: {e}", flush=True)
        sys.exit(1)

    regions = []
    for r in REGION_DEFS:
        regions.append({**r, "points": generate_grid(r["bbox"], r["size"])})

    total_points = sum(len(r["points"]) for r in regions)
    print(f"\n{len(regions)} regions, {total_points} measurement points", flush=True)
    print(f"Bulk strategy: 1 API call per region = {len(regions)} calls per period", flush=True)
    print(f"Total expected: {len(regions) * 8} historical calls + {len(regions) * 2} forecast calls = {len(regions) * 10} calls", flush=True)

    # ===== HISTORICAL =====
    historical = {}
    periods = ["24h", "yesterday", "3d", "5d", "7d", "14d", "30d", "season"]

    for period in periods:
        t0 = time.time()
        print(f"\n--- HISTORICAL: {period} ---", flush=True)

        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as ex:
            future_to_region = {
                ex.submit(safe_call, fetch_historical_region_bulk, region, period): region
                for region in regions
            }
            results_by_region = {}
            for fut in as_completed(future_to_region):
                region = future_to_region[fut]
                results_by_region[region['name']] = fut.result()

        period_results = {}
        ok_regions = 0
        for region in regions:
            agg = aggregate_region(results_by_region.get(region['name']), period)
            if agg:
                period_results[region['name']] = agg
                ok_regions += 1
            else:
                period_results[region['name']] = {"error": "all points failed"}
        historical[period] = period_results
        print(f"  → {period} done in {time.time()-t0:.1f}s ({ok_regions}/{len(regions)} regions ok)", flush=True)
        # Korte pauze om Open-Meteo per-minuut limiet te respecteren
        if period != periods[-1]:
            time.sleep(PERIOD_PAUSE)

    # ===== FORECAST =====
    print(f"\n--- FORECAST: ECMWF + GFS, 8 days ---", flush=True)
    t0 = time.time()

    # Build tasks: (region, model)
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
    print(f"  → forecast done in {time.time()-t0:.1f}s ({len(forecast_per_region)}/{len(regions)} regions ok)", flush=True)

    forecast = {"dates": forecast_dates, "regions": forecast_per_region} if forecast_dates else None

    # ===== WRITE =====
    Path("data").mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()

    with open("data/historical.json", "w", encoding="utf-8") as f:
        json.dump({"timestamp": timestamp, "data": historical}, f, separators=(',', ':'))
    print(f"\n✓ data/historical.json written", flush=True)

    if forecast:
        with open("data/forecast.json", "w", encoding="utf-8") as f:
            json.dump({"timestamp": timestamp, "data": forecast}, f, separators=(',', ':'))
        print(f"✓ data/forecast.json written", flush=True)

    print(f"\nTotal time: {time.time()-start_total:.1f}s", flush=True)


if __name__ == "__main__":
    main()
