# Scripts/iv_calculator.py (نسخه دیباگ کامل)
import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import os

# ================== CONFIG ==================
INPUT_DIR = 'Output/1'
OUTPUT_DIR = 'Output/3'
os.makedirs(OUTPUT_DIR, exist_ok=True)

LIVE_OPTIONS = f'{INPUT_DIR}/options_live.xlsx'
HIST_OPTIONS = f'{INPUT_DIR}/options_history_252.xlsx'
HIST_UNDERLYING = f'{INPUT_DIR}/underlying_history_252.xlsx'
OUTPUT_FILE = f'{OUTPUT_DIR}/option_iv_history.xlsx'

RISK_FREE_RATE = 0.36
MIN_IV, MAX_IV = 1e-6, 5.0

# ================== BLACK-SCHOLES ==================
def bs_price(S, K, T, r, sigma, typ):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0: return 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2) if typ == 'CALL' else \
           K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def implied_volatility(price, S, K, T, r, typ):
    if price <= 0 or T <= 0 or S <= 0 or K <= 0: return np.nan
    try:
        return brentq(lambda sigma: bs_price(S, K, T, r, sigma, typ) - price, MIN_IV, MAX_IV, maxiter=100)
    except:
        return np.nan

# ================== DATE TO INT ==================
def jalali_to_int(s):
    if pd.isna(s): return np.nan
    s = str(s).replace('/', '').strip()
    return int(s) if len(s) == 8 else np.nan

# ================== UPDATE CHECK ==================
def should_rebuild():
    if not os.path.exists(OUTPUT_FILE): return True
    if not os.path.exists(HIST_OPTIONS): return True
    try:
        current = pd.read_excel(HIST_OPTIONS, usecols=['date']).apply(jalali_to_int, axis=1).max()
        saved = pd.read_excel(OUTPUT_FILE, usecols=['date']).apply(jalali_to_int, axis=1).max()
        print(f"[CHECK] Current max date: {current}, Saved max date: {saved}")
        return pd.isna(saved) or current > saved
    except Exception as e:
        print(f"[CHECK] Error: {e}")
        return True

# ================== MAIN (با دیباگ کامل) ==================
def build_iv_history():
    if not should_rebuild():
        print("IV history: Already up to date")
        return pd.read_excel(OUTPUT_FILE)

    print("=== STARTING IV HISTORY BUILD ===")

    # 1. بارگذاری
    print(f"Loading files...")
    print(f"   {LIVE_OPTIONS} → ", end="")
    df_live = pd.read_excel(LIVE_OPTIONS)
    print(f"{len(df_live)} rows")

    print(f"   {HIST_OPTIONS} → ", end="")
    df_hist_opt = pd.read_excel(HIST_OPTIONS)
    print(f"{len(df_hist_opt)} rows")

    print(f"   {HIST_UNDERLYING} → ", end="")
    df_hist_und = pd.read_excel(HIST_UNDERLYING)
    print(f"{len(df_hist_und)} rows")

    # 2. تبدیل تاریخ
    print("Converting dates to int...")
    df_live['end_date_int'] = df_live['end_date'].apply(jalali_to_int)
    print(f"   Live end_date_int: {df_live['end_date_int'].notna().sum()} valid")

    df_hist_opt['date_int'] = df_hist_opt['date'].apply(jalali_to_int)
    print(f"   Hist opt date_int: {df_hist_opt['date_int'].notna().sum()} valid")

    df_hist_und['date_int'] = df_hist_und['date'].apply(jalali_to_int)
    print(f"   Hist und date_int: {df_hist_und['date_int'].notna().sum()} valid")

    # 3. نقشه قراردادها
    print("Building contract map...")
    contract_map = {}
    isin_to_underlying = {}

    valid_live = 0
    for idx, row in df_live.iterrows():
        try:
            strike = float(row['strike'])
            end_int = row['end_date_int']
            if pd.isna(end_int): continue

            u_isin = str(row['underlying_isin']).strip()

            # PUT
            isin_put = str(row['isin_put']).strip()
            if isin_put not in ['nan', '', '<NA>']:
                contract_map[isin_put] = {'strike': strike, 'end_int': end_int, 'type': 'PUT'}
                isin_to_underlying[isin_put] = u_isin
                valid_live += 1

            # CALL
            isin_call = str(row['isin_call']).strip()
            if pd.notna(row['isin_call']) and isin_call not in ['nan', '', '<NA>']:
                contract_map[isin_call] = {'strike': strike, 'end_int': end_int, 'type': 'CALL'}
                isin_to_underlying[isin_call] = u_isin
                valid_live += 1
        except:
            continue

    print(f"   Mapped {len(contract_map)} unique option ISINs")
    print(f"   Total valid live entries: {valid_live}")

    # 4. فیلتر آپشن‌های تاریخچه
    print("Filtering historical options by contract map...")
    before_filter = len(df_hist_opt)
    df_hist_opt = df_hist_opt.dropna(subset=['source_isin', 'date_int'])
    df_hist_opt['source_isin'] = df_hist_opt['source_isin'].astype(str).str.strip()
    df_hist_opt = df_hist_opt[df_hist_opt['source_isin'].isin(contract_map.keys())]
    after_filter = len(df_hist_opt)
    print(f"   Before: {before_filter} → After: {after_filter} (matched)")

    if after_filter == 0:
        print("No matching option ISINs in history! Check 'source_isin' column.")
        return pd.DataFrame()

    # 5. ایندکس آندرلایینگ
    print("Indexing underlying history...")
    df_hist_und = df_hist_und.dropna(subset=['date_int', 'isin'])
    df_hist_und['isin'] = df_hist_und['isin'].astype(str).str.strip()
    df_hist_und.set_index(['date_int', 'isin'], inplace=True)
    print(f"   Indexed: {len(df_hist_und)} rows")

    # 6. محاسبه IV
    print("Calculating IV for each historical option...")
    records = []
    total = len(df_hist_opt)
    step = total // 10 if total > 0 else 1

    for idx, opt in df_hist_opt.iterrows():
        if idx % step == 0:
            print(f"   Progress: {idx}/{total} ({(idx/total)*100:.1f}%)")

        isin_opt = opt['source_isin']
        info = contract_map.get(isin_opt)
        if not info: continue

        date_int = opt['date_int']
        end_int = info['end_int']
        if end_int <= date_int: continue

        T = (end_int - date_int) / 365.0
        price = float(opt['last']) if pd.notna(opt['last']) else 0
        if price <= 0: continue

        u_isin = isin_to_underlying.get(isin_opt)
        if not u_isin: continue

        try:
            S_row = df_hist_und.loc[(date_int, u_isin)]
            S = float(S_row['last']) if pd.notna(S_row['last']) else 0
            if S <= 0: continue
        except:
            continue

        iv = implied_volatility(price, S, info['strike'], T, RISK_FREE_RATE, info['type'])
        bs = bs_price(S, info['strike'], T, RISK_FREE_RATE, iv, info['type']) if pd.notna(iv) else np.nan

        records.append({
            'option_isin': isin_opt,
            'date': opt['date'],
            'option_price': round(price, 2),
            'underlying_price': round(S, 2),
            'strike': info['strike'],
            'days_to_expiry': int(end_int - date_int),
            'type': info['type'],
            'implied_volatility': round(iv, 4) if pd.notna(iv) else np.nan,
            'bs_price': round(bs, 2) if pd.notna(bs) else np.nan
        })

    # 7. ذخیره
    print(f"Final records: {len(records)}")
    if not records:
        print("No IV data generated. Saving empty file.")
        pd.DataFrame().to_excel(OUTPUT_FILE, index=False)
        return pd.DataFrame()

    df_out = pd.DataFrame(records).sort_values(['option_isin', 'date']).reset_index(drop=True)
    df_out.to_excel(OUTPUT_FILE, index=False)
    print(f"SUCCESS: IV history saved → {OUTPUT_FILE}")
    print(f"   Rows: {len(df_out)} | Dates: {df_out['date'].nunique()}")
    return df_out

# ================== MAIN ==================
def main():
    build_iv_history()

if __name__ == "__main__":
    main()