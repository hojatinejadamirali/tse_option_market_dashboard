# Scripts/chain_calculator.py
import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import os
import hashlib

# ================== Settings ==================
INPUT_DIR = 'Output/1'
OUTPUT_DIR = 'Output/2'
os.makedirs(OUTPUT_DIR, exist_ok=True)

LIVE_FILE = f'{INPUT_DIR}/options_live.xlsx'
HISTORY_FILE = f'{INPUT_DIR}/underlying_history_252.xlsx'
UNDERLYING_LIVE_FILE = f'{INPUT_DIR}/underlying_live.xlsx'

OUTPUT_FILE = f'{OUTPUT_DIR}/options_chain_enhanced.xlsx'
HASH_FILE = OUTPUT_FILE + '.hash'

RISK_FREE_RATE = 0.36
MIN_IV = 0.01
MAX_IV = 5.0

# ================== Black-Scholes Functions ==================
def bs_price(S, K, T, r, sigma, option_type):
    """Black-Scholes price"""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    try:
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        if option_type == 'CALL':
            return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:
            return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    except:
        return 0.0

def implied_volatility(price, S, K, T, r, option_type):
    """Solve for IV"""
    if price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return np.nan
    try:
        return brentq(
            lambda sigma: bs_price(S, K, T, r, sigma, option_type) - price,
            MIN_IV, MAX_IV, maxiter=100
        )
    except:
        return np.nan

def calculate_greeks(S, K, T, r, sigma, option_type):
    """Delta, Gamma, Theta (daily), Vega (per 1%), Rho (per 1%)"""
    if T <= 0 or sigma <= MIN_IV or S <= 0 or K <= 0:
        return {k: np.nan for k in ['delta', 'gamma', 'theta', 'vega', 'rho']}
    try:
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        if option_type == 'CALL':
            delta = norm.cdf(d1)
            theta = -(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * norm.cdf(d2)
            rho = K * T * np.exp(-r * T) * norm.cdf(d2)
        else:
            delta = norm.cdf(d1) - 1
            theta = -(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * norm.cdf(-d2)
            rho = -K * T * np.exp(-r * T) * norm.cdf(-d2)

        gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
        vega = S * norm.pdf(d1) * np.sqrt(T)

        return {
            'delta': delta,
            'gamma': gamma,
            'theta': theta / 365,
            'vega': vega / 100,
            'rho': rho / 100
        }
    except:
        return {k: np.nan for k in ['delta', 'gamma', 'theta', 'vega', 'rho']}

# ================== Hash System ==================
def file_hash(filepath):
    """SHA256 hash of file"""
    if not os.path.exists(filepath):
        return None
    hash_sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def should_rebuild():
    """Check if rebuild is needed"""
    if not os.path.exists(OUTPUT_FILE):
        return True
    if not os.path.exists(LIVE_FILE):
        return False
    current_hash = file_hash(LIVE_FILE)
    if current_hash is None:
        return True
    if not os.path.exists(HASH_FILE):
        return True
    try:
        with open(HASH_FILE, 'r') as f:
            saved_hash = f.read().strip()
        return current_hash != saved_hash
    except:
        return True

def save_hash():
    h = file_hash(LIVE_FILE)
    if h:
        with open(HASH_FILE, 'w') as f:
            f.write(h)

# ================== Historical Volatility (FIXED: last + ISIN match) ==================
def calculate_hv(df_hist, u_isin):
    """HV 30, 90, 252 using 'last' column + robust ISIN matching"""
    u_isin_str = str(u_isin).strip()
    print(f"[HV] Calculating for underlying ISIN: {u_isin_str}")

    # فیلتر با تطابق دقیق ISIN
    hist = df_hist[df_hist['isin'].astype(str).str.strip() == u_isin_str].copy()
    print(f"[HV]   Found {len(hist)} historical rows")

    if len(hist) < 2:
        print(f"[HV]   Not enough data (<2 rows)")
        return np.nan, np.nan, np.nan, np.nan

    # تبدیل تاریخ به int و حذف تکراری
    hist['date_int'] = hist['date'].astype(str).str.replace('/', '').astype(int, errors='ignore')
    hist = hist.dropna(subset=['date_int'])
    hist['date_int'] = hist['date_int'].astype(int)
    hist = hist.sort_values('date_int').drop_duplicates('date_int')
    print(f"[HV]   After dedup: {len(hist)} rows")

    # استفاده از ستون 'last' به جای 'real_time_price'
    if 'last' not in hist.columns:
        print(f"[HV]   Missing 'last' column → available: {list(hist.columns)}")
        return np.nan, np.nan, np.nan, np.nan

    hist['price'] = pd.to_numeric(hist['last'], errors='coerce')
    hist = hist.dropna(subset=['price'])
    hist = hist[hist['price'] > 0]
    print(f"[HV]   Valid price rows: {len(hist)}")

    if len(hist) < 2:
        print(f"[HV]   Not enough valid prices")
        return np.nan, np.nan, np.nan, np.nan

    hist['ret'] = np.log(hist['price'] / hist['price'].shift(1))
    rets = hist['ret'].dropna()
    print(f"[HV]   Valid returns: {len(rets)}")

    if len(rets) == 0:
        print(f"[HV]   No valid returns")
        return np.nan, np.nan, np.nan, np.nan

    v30 = rets[-30:].std() * np.sqrt(252) if len(rets) >= 30 else np.nan
    v90 = rets[-90:].std() * np.sqrt(252) if len(rets) >= 90 else np.nan
    v252 = rets.std() * np.sqrt(252)

    print(f"[HV]   HV30: {v30:.4f}, HV90: {v90:.4f}, HV252: {v252:.4f}")

    valid_vols = [v for v in [v30, v90, v252] if pd.notna(v)]
    selected = np.nanmean(valid_vols) if valid_vols else 0.3
    print(f"[HV]   Selected HV: {selected:.4f}")

    return v30, v90, v252, selected

# ================== Main Build Function ==================
def build_chain():
    if not should_rebuild():
        print("Options chain: Already up to date")
        return pd.read_excel(OUTPUT_FILE)

    print("Building enhanced options chain...")

    # 1. خواندن داده‌ها
    if not os.path.exists(LIVE_FILE):
        print("Error: options_live.xlsx not found!")
        return pd.DataFrame()

    df_live = pd.read_excel(LIVE_FILE)
    print(f"Live options: {len(df_live)} rows")

    df_hist = pd.read_excel(HISTORY_FILE) if os.path.exists(HISTORY_FILE) else pd.DataFrame()
    print(f"Underlying history: {len(df_hist)} rows")

    df_underlying_live = pd.read_excel(UNDERLYING_LIVE_FILE) if os.path.exists(UNDERLYING_LIVE_FILE) else pd.DataFrame()

    # 2. HV برای هر underlying
    print("Calculating historical volatility...")
    hv_cache = {}
    unique_underlyings = df_live['underlying_isin'].dropna().unique()
    print(f"Unique underlying ISINs in live: {len(unique_underlyings)}")

    if not df_hist.empty:
        # نمایش ISINهای موجود در هیستوری
        hist_isins = df_hist['isin'].astype(str).str.strip().unique()
        print(f"Unique ISINs in history: {len(hist_isins)} → sample: {list(hist_isins)[:5]}")

        for u_isin in unique_underlyings:
            u_isin_str = str(u_isin).strip()
            if u_isin_str in ['nan', '']: 
                continue
            if u_isin_str not in hist_isins:
                print(f"[HV]   ISIN {u_isin_str} not in history → skipping")
                hv_cache[u_isin_str] = (np.nan, np.nan, np.nan, 0.3)
            else:
                hv_cache[u_isin_str] = calculate_hv(df_hist, u_isin_str)
    else:
        print("Warning: underlying_history_252.xlsx is empty!")

    # 3. قیمت underlying از underlying_live
    underlying_price_map = {}
    if not df_underlying_live.empty:
        for _, row in df_underlying_live.iterrows():
            isin = str(row['isin']).strip()
            if isin not in ['nan', '']:
                underlying_price_map[isin] = row['real_time_price']

    # 4. پردازش قراردادها
    print("Calculating IV, Greeks, and theoretical prices...")
    rows = []
    skipped = 0

    for idx, r in df_live.iterrows():
        try:
            u_isin = str(r['underlying_isin']).strip()
            if u_isin in ['nan', '']: 
                skipped += 1
                continue

            S = underlying_price_map.get(u_isin, r.get('underlying_real_time_price', 0))
            if pd.isna(S) or S <= 0:
                skipped += 1
                continue

            K = float(r['strike'])
            T = float(r['remained_day']) / 365.0
            price = float(r['real_time_price'])
            last = float(r['last']) if pd.notna(r['last']) else price
            typ = r['type']
            isin = str(r['isin_put'] if typ == 'PUT' else r['isin_call']).strip()

            if T <= 0 or price <= 0:
                skipped += 1
                continue

            # IV
            iv = implied_volatility(price, S, K, T, RISK_FREE_RATE, typ)
            hv_data = hv_cache.get(u_isin, (np.nan, np.nan, np.nan, 0.3))
            sigma = iv if pd.notna(iv) else hv_data[3]

            # قیمت تئوری
            theo = bs_price(S, K, T, RISK_FREE_RATE, sigma, typ)
            diff_pct = (last - theo) / theo if theo > 0 else np.nan

            # Greeks
            greeks = calculate_greeks(S, K, T, RISK_FREE_RATE, sigma, typ)

            rows.append({
                'contract_isin': isin,
                'ticker': str(r['ticker']),
                'name': str(r['name']),
                'type': typ,
                'strike': K,
                'days_to_expiry': int(r['remained_day']),
                'begin_date': str(r['begin_date']),
                'end_date': str(r['end_date']),
                'market_price': round(price, 0),
                'last_price': round(last, 0),
                'theoretical_price': round(theo, 2),
                'price_diff_pct': round(diff_pct, 4),
                'iv': round(iv, 4) if pd.notna(iv) else np.nan,
                'delta': round(greeks['delta'], 4),
                'gamma': round(greeks['gamma'], 4),
                'theta_daily': round(greeks['theta'], 4),
                'vega_per_1pct': round(greeks['vega'], 4),
                'rho_per_1pct': round(greeks['rho'], 4),
                'hv_30d': round(hv_data[0], 4) if pd.notna(hv_data[0]) else np.nan,
                'hv_90d': round(hv_data[1], 4) if pd.notna(hv_data[1]) else np.nan,
                'hv_252d': round(hv_data[2], 4),
                'hv_selected': round(hv_data[3], 4),
                'trade_count': int(r['count']) if pd.notna(r['count']) else 0,
                'volume': int(r['volume']) if pd.notna(r['volume']) else 0,
                'open_interest': int(r['open_interest']) if pd.notna(r['open_interest']) else 0,
                'underlying_price': round(S, 0),
                'underlying_isin': u_isin,
                'underlying_name': str(r.get('underlying_ticker', '')),
                'contract_size': int(r.get('contract_size', 1000))
            })
        except Exception as e:
            skipped += 1
            continue

    print(f"Processed: {len(rows)} | Skipped: {skipped}")

    if not rows:
        print("No valid contracts found!")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 5. مرتب‌سازی
    print("Sorting chains by expiry and strike...")
    df = df.sort_values(['days_to_expiry', 'type', 'strike'], ascending=[True, False, True]).reset_index(drop=True)

    # 6. ذخیره
    df.to_excel(OUTPUT_FILE, index=False)
    save_hash()
    print(f"Enhanced chain built: {len(df)} contracts | {df['days_to_expiry'].nunique()} expiries")
    print(f"Saved → {OUTPUT_FILE}")
    return df

# ================== Main ==================
def main():
    build_chain()

if __name__ == "__main__":
    main()