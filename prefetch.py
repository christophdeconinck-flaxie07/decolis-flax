#!/usr/bin/env python3
"""
Prefetch weather data for Flax Rainfall Monitor.
Runs 4x per day via GitHub Actions, writes results to data/historical.json and data/forecast.json
"""

import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ============ REGIONS (mirror of HTML) ============
REGION_DEFS = [
    # France - Normandy
    {"name": "76 - Seine-Maritime", "country": "FR", "bbox": [49.28, 0.07, 50.07, 1.78], "size": 20},
    {"name": "27 - Eure", "country": "FR", "bbox": [48.67, 0.30, 49.48, 1.80], "size": 15},
    {"name": "14 - Calvados", "country": "FR", "bbox": [48.76, -1.17, 49.40, 0.45], "size": 15},
    # France - Hauts-de-France
    {"name": "59 - Nord", "country": "FR", "bbox": [50.10, 2.55, 51.08, 4.23], "size": 15},
    {"name": "62 - Pas-de-Calais", "country": "FR", "bbox": [50.02, 1.55, 50.95, 3.20], "size": 15},
    {"name": "80 - Somme", "country": "FR", "bbox": [49.57, 1.38, 50.37, 3.18], "size": 15},
    {"name": "02 - Aisne", "country": "FR", "bbox": [48.83, 3.02, 49.96, 4.25], "size": 15},
    {"name": "60 - Oise", "country": "FR", "bbox": [49.10, 1.68, 49.78, 3.18], "size": 15},
    # France - Île-de-France
    {"name": "77 - Seine-et-Marne", "country": "FR", "bbox": [48.12, 2.38, 49.13, 3.55], "size": 15},
    {"name": "95 - Val-d'Oise", "country": "FR", "bbox": [48.93, 1.62, 49.23, 2.60], "size": 8},
    # Belgium
    {"name": "West Flanders (Westhoek/Lys/Polders)", "country": "BE", "bbox": [50.68, 2.55, 51.37, 3.42], "size": 15},
    {"name": "East Flanders (NL border + Oudenaarde)", "country": "BE", "bbox": [50.72, 3.43, 51.37, 4.23], "size": 15},
    {"name": "Flemish Brabant (Tienen/Hageland)", "country": "BE", "bbox": [50.70, 4.60, 50.95, 5.22], "size": 15},
    {"name": "Limburg (Haspengouw)", "country": "BE", "bbox": [50.70, 5.10, 51.00, 5.65], "size": 8},
    {"name": "Hainaut (Tournai/Ath/Mons)", "country": "BE", "bbox": [50.25, 3.25, 50.77, 4.43], "size": 15},
    {"name": "Walloon Brabant (Nivelles/Gembloux)", "country": "BE", "bbox": [50.45, 4.22, 50.78, 4.80], "size": 8},
    {"name": "Liège (Hesbaye plain)", "country": "BE", "bbox": [50.55, 5.05, 50.80, 5.50], "size": 15},
    {"name": "Namur (loam region)", "country": "BE", "bbox": [50.35, 4.55, 50.60, 5.15], "size": 15},
    {"name": "Condroz", "country": "BE", "bbox": [50.15, 4.75, 50.45, 5.45], "size": 15},
    # Netherlands
    {"name": "Zeeland", "country": "NL", "bbox": [51.22, 3.37, 51.67, 4.30], "size": 15},
    {"name": "Flevoland", "country": "NL", "bbox": [52.22, 5.15, 52.75, 5.90], "size": 8},
]


def generate_grid(bbox, n):
    """Generate n points spread over bounding box."""
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


def http_get_json(url, max_retries=4):
    """GET JSON with retry on rate limit."""
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'flax-monitor-prefetch/1.0'})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                wait = 5 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise


def fetch_historical_point(lat, lon, period):
    """Fetch one historical period for one point."""
    tz = "Europe/Brussels"
    base = "https://api.open-meteo.com/v1/forecast"

    if period == "24h":
        url = f"{base}?latitude={lat}&longitude={lon}&hourly=precipitation,temperature_2m,et0_fao_evapotranspiration&past_days=2&forecast_days=1&timezone={urllib.parse.quote(tz)}"
        data = http_get_json(url)
        # Calculate last 24h from hourly
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)
        precip, et0, t_sum, t_max, count = 0, 0, 0, -999, 0
        for i, t_str in enumerate(data['hourly']['time']):
            t = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
            # Open-Meteo Brussels times need offset
            t_local = t  # treat as local — Brussels offset already applied
            if cutoff <= t_local <= now:
                p = data['hourly']['precipitation'][i]
                e = data['hourly'].get('et0_fao_evapotranspiration', [None]*len(data['hourly']['time']))[i]
                temp = data['hourly'].get('temperature_2m', [None]*len(data['hourly']['time']))[i]
                if p is not None: precip += p
                if e is not None: et0 += e
                if temp is not None:
                    t_sum += temp
                    count += 1
                    if temp > t_max: t_max = temp
        if count == 0:
            # Fallback: take all available hours from last 24
            return None
        return {
            "precip": precip,
            "et0": et0,
            "tempMean": t_sum / count if count else None,
            "tempMax": t_max if t_max != -999 else None,
            "balance": precip - et0
        }

    elif period == "yesterday":
        url = f"{base}?latitude={lat}&longitude={lon}&daily=precipitation_sum,temperature_2m_mean,temperature_2m_max,et0_fao_evapotranspiration&past_days=1&forecast_days=0&timezone={urllib.parse.quote(tz)}"
        data = http_get_json(url)
        d = data['daily']
        if not d['precipitation_sum']:
            return None
        precip = d['precipitation_sum'][0] or 0
        et0 = (d.get('et0_fao_evapotranspiration', [0])[0]) or 0
        t_mean = d.get('temperature_2m_mean', [None])[0]
        t_max = d.get('temperature_2m_max', [None])[0]
        return {"precip": precip, "et0": et0, "tempMean": t_mean, "tempMax": t_max, "balance": precip - et0}

    else:
        days_map = {"3d": 3, "5d": 5, "7d": 7, "14d": 14, "30d": 30}
        if period in days_map:
            days = days_map[period]
            url = f"{base}?latitude={lat}&longitude={lon}&daily=precipitation_sum,temperature_2m_mean,temperature_2m_max,et0_fao_evapotranspiration&past_days={days}&forecast_days=1&timezone={urllib.parse.quote(tz)}"
        elif period == "season":
            year = datetime.now().year
            start = f"{year}-03-01"
            end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&daily=precipitation_sum,temperature_2m_mean,temperature_2m_max,et0_fao_evapotranspiration&start_date={start}&end_date={end}&timezone={urllib.parse.quote(tz)}"
        else:
            return None

        data = http_get_json(url)
        d = data['daily']
        precip = sum(v for v in d['precipitation_sum'] if v is not None)
        et0_list = d.get('et0_fao_evapotranspiration', [])
        et0 = sum(v for v in et0_list if v is not None) if et0_list else 0
        t_mean_list = [v for v in d.get('temperature_2m_mean', []) if v is not None]
        t_max_list = [v for v in d.get('temperature_2m_max', []) if v is not None]
        return {
            "precip": precip,
            "et0": et0,
            "tempMean": sum(t_mean_list)/len(t_mean_list) if t_mean_list else None,
            "tempMax": max(t_max_list) if t_max_list else None,
            "balance": precip - et0
        }


def fetch_forecast_point(lat, lon, model):
    """Fetch 8-day forecast for one point with one model."""
    tz = "Europe/Brussels"
    model_param = "ecmwf_ifs025" if model == "ecmwf" else "gfs_seamless"
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           f"&daily=precipitation_sum,precipitation_probability_max,temperature_2m_max,temperature_2m_min"
           f"&models={model_param}&forecast_days=8&timezone={urllib.parse.quote(tz)}")
    data = http_get_json(url)
    d = data.get('daily', {})
    return {
        "dates": d.get('time', []),
        "precip": d.get('precipitation_sum', []),
        "prob": d.get('precipitation_probability_max', []),
        "tmax": d.get('temperature_2m_max', []),
        "tmin": d.get('temperature_2m_min', [])
    }


def aggregate_region(points_data, period):
    """Average results across points within a region."""
    valid = [p for p in points_data if p is not None]
    if not valid:
        return None

    n = len(valid)

    def avg_or_none(key):
        vals = [v[key] for v in valid if v.get(key) is not None]
        return sum(vals)/len(vals) if vals else None

    def safe_avg(key):
        return sum(v[key] for v in valid) / n

    return {
        "precip": safe_avg("precip"),
        "et0": safe_avg("et0"),
        "balance": safe_avg("balance"),
        "tempMean": avg_or_none("tempMean"),
        "tempMax": avg_or_none("tempMax"),
        "precipMin": min(v["precip"] for v in valid),
        "precipMax": max(v["precip"] for v in valid),
        "successCount": n,
        "totalCount": len(points_data)
    }


def aggregate_forecast_region(points_data):
    """Aggregate forecast points within a region per day."""
    valid = [p for p in points_data if p and p.get('dates')]
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
    print(f"=== Flax Rainfall Monitor Prefetch ===")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}")

    # Build full region list with points
    regions = []
    for r in REGION_DEFS:
        regions.append({
            **r,
            "points": generate_grid(r["bbox"], r["size"])
        })

    total_points = sum(len(r["points"]) for r in regions)
    print(f"{len(regions)} regions, {total_points} measurement points\n")

    # ===== HISTORICAL DATA =====
    historical = {}
    periods = ["24h", "yesterday", "3d", "5d", "7d", "14d", "30d", "season"]

    for period in periods:
        print(f"--- HISTORICAL: {period} ---")
        period_results = {}
        for region in regions:
            print(f"  {region['name']} ({len(region['points'])} pts)... ", end="", flush=True)
            point_results = []
            for lat, lon in region['points']:
                try:
                    point_results.append(fetch_historical_point(lat, lon, period))
                except Exception as e:
                    print(f"\n    Error: {e}")
                    point_results.append(None)
                time.sleep(0.05)  # mild rate limit
            agg = aggregate_region(point_results, period)
            if agg:
                period_results[region['name']] = agg
                print(f"OK ({agg['successCount']}/{agg['totalCount']}, {agg['precip']:.1f}mm)")
            else:
                period_results[region['name']] = {"error": "all points failed"}
                print("FAILED")
        historical[period] = period_results

    # ===== FORECAST =====
    print(f"\n--- FORECAST: ECMWF + GFS, 8 days ---")
    forecast_per_region = {}
    forecast_dates = None

    for region in regions:
        print(f"  {region['name']}... ", end="", flush=True)
        try:
            ecmwf_points = []
            gfs_points = []
            for lat, lon in region['points']:
                try:
                    ecmwf_points.append(fetch_forecast_point(lat, lon, "ecmwf"))
                except Exception:
                    ecmwf_points.append(None)
                time.sleep(0.05)
                try:
                    gfs_points.append(fetch_forecast_point(lat, lon, "gfs"))
                except Exception:
                    gfs_points.append(None)
                time.sleep(0.05)

            ecmwf_agg = aggregate_forecast_region(ecmwf_points)
            gfs_agg = aggregate_forecast_region(gfs_points)
            if ecmwf_agg:
                forecast_per_region[region['name']] = {"ecmwf": ecmwf_agg, "gfs": gfs_agg}
                if not forecast_dates:
                    forecast_dates = ecmwf_agg['dates']
                print("OK")
            else:
                print("FAILED")
        except Exception as e:
            print(f"ERROR: {e}")

    forecast = {"dates": forecast_dates, "regions": forecast_per_region} if forecast_dates else None

    # ===== WRITE OUTPUTS =====
    Path("data").mkdir(exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()

    with open("data/historical.json", "w", encoding="utf-8") as f:
        json.dump({"timestamp": timestamp, "data": historical}, f, separators=(',', ':'))
    print(f"\n✓ data/historical.json written")

    if forecast:
        with open("data/forecast.json", "w", encoding="utf-8") as f:
            json.dump({"timestamp": timestamp, "data": forecast}, f, separators=(',', ':'))
        print(f"✓ data/forecast.json written")
    else:
        print("✗ Forecast empty, not writing")

    print(f"\nFinished at: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
