# server/app.py
import os
import numpy as np
import pandas as pd
import traceback
from flask import Flask, render_template, jsonify, send_from_directory
from threading import Lock
from typing import Optional

# === IMPORT DATA UPDATER ===
try:
    from .data_updater import DataUpdater
except ImportError as e:
    print("[FATAL] Cannot import DataUpdater. Check server/data_updater.py")
    raise e

# === CONFIG: Absolute paths ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
STATIC_DIR = os.path.join(BASE_DIR, 'static')
TEMPLATES_DIR = os.path.join(BASE_DIR, 'templates')
CHAIN_FILE = os.path.join(BASE_DIR, 'Output', '2', 'options_chain_enhanced.xlsx')
IV_FILE = os.path.join(BASE_DIR, 'Output', '3', 'option_iv_history.xlsx')

print(f"[DEBUG] Base directory: {BASE_DIR}")
print(f"[DEBUG] Chain file: {CHAIN_FILE} → exists: {os.path.exists(CHAIN_FILE)}")
print(f"[DEBUG] IV file: {IV_FILE} → exists: {os.path.exists(IV_FILE)}")
print(f"[DEBUG] Static folder: {STATIC_DIR} → favicon: {os.path.exists(os.path.join(STATIC_DIR, 'favicon.ico'))}")

# === Flask App ===
app = Flask(
    __name__,
    template_folder=TEMPLATES_DIR,
    static_folder=STATIC_DIR
)

# === Global Updater & Cache ===
updater = DataUpdater(interval=300)  # هر 5 دقیقه
cache_lock = Lock()
cached_chain = None
cached_iv = None

def clean_for_json(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Replace NaN, inf with None for JSON"""
    if df is None or df.empty:
        return df
    return df.replace([np.nan, np.inf, -np.inf], None)

def read_excel_safely(filepath: str, **kwargs) -> Optional[pd.DataFrame]:
    """Safe Excel read with error handling"""
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return None
    try:
        print(f"[INFO] Reading file: {os.path.basename(filepath)}")
        return pd.read_excel(filepath, engine='openpyxl', **kwargs)
    except Exception as e:
        print(f"[ERROR] Failed to read file: {e}")
        traceback.print_exc()
        return None

@app.route('/')
def index():
    """Home page with status"""
    status = updater.get_status()
    return render_template('index_v2.html', **status)

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory(STATIC_DIR, filename)

@app.route('/api/status')
def api_status():
    """Return updater status"""
    return jsonify(updater.get_status())

@app.route('/api/chain')
def api_chain():
    """Return full option chain with cache"""
    global cached_chain
    print(f"[API] Request: /api/chain → file exists: {os.path.exists(CHAIN_FILE)}")

    # === CACHE CHECK ===
    if cached_chain is not None and os.path.exists(CHAIN_FILE):
        try:
            current_mtime = os.path.getmtime(CHAIN_FILE)
            if hasattr(cached_chain, 'mtime') and cached_chain.mtime == current_mtime:
                print(f"[CACHE HIT] Returning {len(cached_chain.data)} rows")
                return jsonify(cached_chain.data)
        except:
            cached_chain = None

    # === READ & PROCESS ===
    df = read_excel_safely(CHAIN_FILE)
    if df is None or df.empty:
        return jsonify({"error": "Chain data not available"}), 404

    try:
        # === ENSURE underlying_name ===
        if 'underlying_name' not in df.columns:
            print("[WARN] 'underlying_name' missing → extracting from 'ticker'")
            if 'ticker' in df.columns:
                df['underlying_name'] = df['ticker'].astype(str).str.extract(r'^([آ-ی]+)').fillna('نامشخص')
            else:
                df['underlying_name'] = 'نامشخص'

        # === SELECT COLUMNS ===
        desired_cols = [
            'underlying_name', 'ticker', 'type', 'strike', 'days_to_expiry',
            'market_price', 'last_price', 'theoretical_price', 'price_diff_pct',
            'iv', 'delta', 'gamma', 'theta_daily', 'vega_per_1pct', 'rho_per_1pct',
            'hv_30d', 'hv_90d', 'hv_252d',
            'volume', 'value', 'open_interest',
            'underlying_price', 'contract_isin'
        ]
        available_cols = [c for c in desired_cols if c in df.columns]
        df = df[available_cols].copy()

        # === CLEAN STRING COLUMNS ===
        str_cols = ['underlying_name', 'ticker', 'type', 'contract_isin']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', ''], '')

        # === CONVERT TO NUMERIC ===
        num_cols = [c for c in df.columns if c not in str_cols]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # === ROUNDING ===
        round_map = {
            'market_price': 0, 'last_price': 0, 'theoretical_price': 0,
            'price_diff_pct': 2, 'iv': 4, 'delta': 4, 'gamma': 6,
            'theta_daily': 2, 'vega_per_1pct': 4, 'rho_per_1pct': 4,
            'hv_30d': 4, 'hv_90d': 4, 'hv_252d': 4,
            'volume': 0, 'value': 0, 'open_interest': 0,
            'underlying_price': 0, 'strike': 0, 'days_to_expiry': 0
        }
        for col, dec in round_map.items():
            if col in df.columns:
                df[col] = df[col].round(dec)

        # === PREPARE JSON ===
        data = clean_for_json(df).to_dict(orient='records')

        # === UPDATE CACHE ===
        with cache_lock:
            cached_chain = type('Cache', (), {
                'data': data,
                'mtime': os.path.getmtime(CHAIN_FILE)
            })()

        print(f"[SUCCESS] Chain served: {len(data)} rows")
        return jsonify(data)

    except Exception as e:
        print(f"[ERROR] /api/chain failed: {e}")
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/iv/<option_isin>')
def api_iv(option_isin):
    """Return IV history for a specific option ISIN"""
    global cached_iv
    print(f"[API] Request: /api/iv/{option_isin}")

    # === LOAD OR REFRESH CACHE ===
    if cached_iv is None or not os.path.exists(IV_FILE):
        df = read_excel_safely(IV_FILE)
        if df is not None:
            with cache_lock:
                cached_iv = df.copy()
    else:
        df = cached_iv.copy()

    if df is None or df.empty:
        return jsonify({"error": "IV history not available"}), 404

    try:
        # === FILTER BY ISIN ===
        df_f = df[df['option_isin'].astype(str).str.strip() == str(option_isin).strip()].copy()
        if df_f.empty:
            return jsonify({"error": "Option ISIN not found"}), 404

        # === SORT BY DATE ===
        df_f['date_int'] = pd.to_numeric(df_f['date'].astype(str).str.replace('/', ''), errors='coerce')
        df_f = df_f.sort_values('date_int').drop(columns='date_int')

        # === SELECT COLUMNS ===
        cols = ['date', 'option_price', 'underlying_price', 'strike',
                'days_to_expiry', 'type', 'implied_volatility', 'bs_price']
        df_f = df_f[[c for c in cols if c in df_f.columns]]

        # === FORMAT IV ===
        if 'implied_volatility' in df_f.columns:
            df_f['implied_volatility'] = pd.to_numeric(df_f['implied_volatility'], errors='coerce').round(4)

        data = clean_for_json(df_f).to_dict(orient='records')
        print(f"[SUCCESS] IV history: {len(data)} records for {option_isin}")
        return jsonify(data)

    except Exception as e:
        print(f"[ERROR] /api/iv failed: {e}")
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/refresh')
def api_refresh():
    """Clear cache manually"""
    global cached_chain, cached_iv
    with cache_lock:
        cached_chain = None
        cached_iv = None
    print("[API] Cache cleared by /api/refresh")
    return jsonify({"message": "Cache cleared successfully"})

def start_server():
    """Start Flask + background updater"""
    print("=" * 80)
    print(" TSE Options Chain Analyzer - Server Starting")
    print(f" Local URL: http://127.0.0.1:5000")
    print(f" Favicon: /static/favicon.ico")
    print(f" Templates: {TEMPLATES_DIR}")
    print(f" Static: {STATIC_DIR}")
    print("=" * 80)

    # شروع به‌روزرسانی پس‌زمینه
    updater.start()

    try:
        app.run(
            host='127.0.0.1',
            port=5000,
            debug=False,
            use_reloader=False,
            threaded=True
        )
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Server interrupted by user")
    finally:
        print("[SHUTDOWN] Stopping background updater...")
        updater.stop()
        print("[SHUTDOWN] Server stopped")

if __name__ == '__main__':
    start_server()