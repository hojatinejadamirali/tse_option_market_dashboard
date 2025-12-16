# Scripts/data_fetcher.py
import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import logging
from typing import List, Optional

# ================== Settings ==================
UNDERLYING_ISIN = '0'  # '17914401175772326' → all contracts
OUTPUT_DIR = 'Output/1'
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_THREADS = 10          # کاهش یافت برای جلوگیری از بلاک شدن
REQUEST_DELAY = 0.15      # ~6-7 درخواست در ثانیه → امن
REQUEST_TIMEOUT = 10

OPTIONS_LIVE_FILE = f'{OUTPUT_DIR}/options_live.xlsx'
OPTIONS_HISTORY_FILE = f'{OUTPUT_DIR}/options_history_252.xlsx'
UNDERLYING_HISTORY_FILE = f'{OUTPUT_DIR}/underlying_history_252.xlsx'
UNDERLYING_LIVE_FILE = f'{OUTPUT_DIR}/underlying_live.xlsx'

REFRESH_INTERVAL = 3600   # 1 ساعت برای فایل‌های live

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ================== Date Helper ==================
def shamsi_to_int(date_str: str) -> Optional[int]:
    """Convert '14040807' or '1404/08/07' → 14040807 (int)"""
    try:
        d = str(date_str).replace('/', '').strip()
        if len(d) != 8:
            return None
        return int(d)
    except (ValueError, TypeError):
        return None

def int_to_shamsi(date_int: int) -> str:
    """14040807 → '14040807'"""
    return f"{date_int:08d}"

# ================== File Freshness Helper ==================
def is_file_fresh(file_path: str, interval: int = REFRESH_INTERVAL) -> bool:
    """Check if file is fresh (modified within the interval seconds)"""
    if not os.path.exists(file_path):
        return False
    age = time.time() - os.path.getmtime(file_path)
    return age < interval

# ================== 1. Live Options (همیشه آپدیت شود) ==================
def fetch_options_live(force_update: bool = False) -> pd.DataFrame:
    if not force_update and is_file_fresh(OPTIONS_LIVE_FILE):
        logger.info("1. Live options: Using fresh existing file")
        return pd.read_excel(OPTIONS_LIVE_FILE)

    url = "https://cdn.tsetmc.com/api/Instrument/GetInstrumentOptionMarketWatch/1"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()['instrumentOptMarketWatch']
    except requests.RequestException as e:
        logger.error(f"Error fetching live options: {e}")
        return pd.DataFrame()

    if UNDERLYING_ISIN != '0':
        data = [r for r in data if r['uaInsCode'] == UNDERLYING_ISIN]

    records = []
    for r in data:
        common = {
            'contract_size': r['contractSize'],
            'underlying_isin': r['uaInsCode'],
            'underlying_ticker': r.get('lval30_UA', '').strip(),
            'underlying_real_time_price': r['pDrCotVal_UA'],
            'underlying_yesterday_real_time_price': r['priceYesterday_UA'],
            'underlying_close': r['pClosing_UA'],
            'begin_date': r['beginDate'],
            'end_date': r['endDate'],
            'strike': r['strikePrice'],
            'remained_day': r['remainedDay']
        }

        # PUT
        records.append({
            'isin_put': r['insCode_P'],
            'isin_call': r.get('insCode_C'),
            'ticker': r['lVal18AFC_P'],
            'name': r['lVal30_P'],
            'count': r['zTotTran_P'],
            'volume': r['qTotTran5J_P'],
            'value': r['qTotCap_P'],
            'notional_value': r['notionalValue_P'],
            'real_time_price': r['pDrCotVal_P'],
            'yesterday_real_time_price': r['priceYesterday_P'],
            'open_interest': r['oP_P'],
            'last': r['pClosing_P'],
            'type': 'PUT',
            **common
        })

        # CALL
        if 'insCode_C' in r and pd.notna(r['insCode_C']):
            records.append({
                'isin_put': r['insCode_P'],
                'isin_call': r['insCode_C'],
                'ticker': r['lVal18AFC_C'],
                'name': r['lVal30_C'],
                'count': r['zTotTran_C'],
                'volume': r['qTotTran5J_C'],
                'value': r['qTotCap_C'],
                'notional_value': r['notionalValue_C'],
                'real_time_price': r['pDrCotVal_C'],
                'yesterday_real_time_price': r['priceYesterday_C'],
                'open_interest': r['oP_C'],
                'last': r['pClosing_C'],
                'type': 'CALL',
                **common
            })

    df = pd.DataFrame(records)
    if df.empty:
        logger.info("1. Live options: No contracts")
        return df

    df = df[[
        'isin_put', 'isin_call', 'contract_size', 'underlying_isin',
        'ticker', 'name', 'count', 'volume', 'value', 'notional_value',
        'real_time_price', 'yesterday_real_time_price', 'open_interest', 'last',
        'underlying_ticker', 'underlying_real_time_price', 'underlying_yesterday_real_time_price',
        'underlying_close', 'begin_date', 'end_date', 'strike', 'remained_day', 'type'
    ]]

    df.to_excel(OPTIONS_LIVE_FILE, index=False)
    logger.info(f"1. Live options: {len(df)} contracts")
    return df

# ================== Get Max Date from File ==================
def get_max_date(file_path: str, date_col: str) -> Optional[int]:
    """Get MAX(date) from file as int (14040807)"""
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_excel(file_path, usecols=[date_col], engine='openpyxl')
        df = df[df[date_col].notna()]
        df[date_col] = df[date_col].astype(str).str.replace('/', '').str.strip()
        df = df[df[date_col].str.len() == 8]
        df[date_col] = df[date_col].astype(int)
        return df[date_col].max() if not df.empty else None
    except Exception as e:
        logger.error(f"Error reading max date from {file_path}: {e}")
        return None

# ================== Generic History Fetcher (با منطق جدید) ==================
def fetch_history_generic(
    live_file: str,
    history_file: str,
    isin_cols: List[str] | str,
    hist_date_col: str = 'date',
    asset_type: str = 'options',
    force_update: bool = False
) -> pd.DataFrame:
    # 1. اگر فایل live وجود ندارد → نمی‌توان ادامه داد
    if not os.path.exists(live_file):
        logger.info(f"{asset_type.capitalize()} live file not found: {live_file}")
        return pd.DataFrame()

    # 2. اگر force_update یا فایل history وجود ندارد → کامل بگیر
    if force_update or not os.path.exists(history_file):
        logger.info(f"{asset_type.capitalize()} history: First time or forced → fetching full history")
        return _fetch_full_history(live_file, history_file, isin_cols, asset_type)

    # 3. بررسی نیاز به آپدیت بر اساس تاریخ
    max_hist = get_max_date(history_file, hist_date_col)
    today_int = shamsi_to_int(pd.Timestamp.now().strftime('%Y%m%d').replace('20', '14'))  # امروز شمسی
    if today_int is None:
        logger.error("Could not determine current Shamsi date")
        return pd.read_excel(history_file)

    if max_hist is None:
        max_hist = 0

    # اگر آخرین تاریخ history از امروز قدیمی‌تر است → نیاز به آپدیت
    if max_hist < today_int:
        days_missing = (today_int - max_hist) // 10000
        days_needed = max(1, days_missing + 10)  # +10 روز حاشیه
        logger.info(f"{asset_type.capitalize()} history: Missing {days_missing} days → fetching {days_needed} days")
        return _fetch_incremental_history(live_file, history_file, isin_cols, days_needed, asset_type, max_hist)
    else:
        logger.info(f"{asset_type.capitalize()} history: Up to date (max: {max_hist})")
        return pd.read_excel(history_file)

# ================== Full History (اولین بار) ==================
def _fetch_full_history(live_file: str, history_file: str, isin_cols: List[str] | str, asset_type: str) -> pd.DataFrame:
    df_live = pd.read_excel(live_file, usecols=isin_cols if isinstance(isin_cols, list) else [isin_cols], dtype=str)
    if isinstance(isin_cols, list):
        isin_list = []
        for col in isin_cols:
            isin_list.extend(df_live[col].dropna().astype(str).tolist())
        isin_list = list(set(isin_list))
    else:
        isin_list = df_live[isin_cols].dropna().astype(str).unique().tolist()

    all_data = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        futures = [ex.submit(_fetch_single_full, isin, asset_type) for isin in isin_list]
        for f in tqdm(as_completed(futures), total=len(isin_list), desc=f"{asset_type.capitalize()} full history", leave=False):
            df = f.result()
            if not df.empty:
                all_data.append(df)

    if not all_data:
        logger.info(f"{asset_type.capitalize()} full history: No data")
        return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)
    df = _clean_and_save_history(df, history_file, asset_type)
    return df

def _fetch_single_full(isin: str, asset_type: str) -> pd.DataFrame:
    url = f"https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{isin}/0"  # 0 = all
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get('closingPriceDaily', [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df['date_int'] = df['dEven'].apply(shamsi_to_int)
        df = df[df['date_int'].notna()].drop(columns=['date_int'])
        df = _rename_columns(df, isin, url, asset_type)
        time.sleep(REQUEST_DELAY)
        return df
    except requests.RequestException:
        time.sleep(REQUEST_DELAY)
        return pd.DataFrame()

# ================== Incremental History (آپدیت روزانه) ==================
def _fetch_incremental_history(
    live_file: str, history_file: str, isin_cols: List[str] | str,
    days_needed: int, asset_type: str, max_hist: int
) -> pd.DataFrame:
    df_live = pd.read_excel(live_file, usecols=isin_cols if isinstance(isin_cols, list) else [isin_cols], dtype=str)
    if isinstance(isin_cols, list):
        isin_list = []
        for col in isin_cols:
            isin_list.extend(df_live[col].dropna().astype(str).tolist())
        isin_list = list(set(isin_list))
    else:
        isin_list = df_live[isin_cols].dropna().astype(str).unique().tolist()

    all_data = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        futures = [ex.submit(_fetch_single_incremental, isin, days_needed, max_hist, asset_type) for isin in isin_list]
        for f in tqdm(as_completed(futures), total=len(isin_list), desc=f"{asset_type.capitalize()} incremental", leave=False):
            df = f.result()
            if not df.empty:
                all_data.append(df)

    if not all_data:
        logger.info(f"{asset_type.capitalize()} incremental: No new data")
        return pd.read_excel(history_file)

    new_df = pd.concat(all_data, ignore_index=True)
    old_df = pd.read_excel(history_file)
    df = pd.concat([old_df, new_df]).drop_duplicates(['isin', 'date', 'hour'], keep='last')
    df = df.sort_values(['isin', 'date', 'hour']).reset_index(drop=True)
    df.to_excel(history_file, index=False)
    logger.info(f"{asset_type.capitalize()} history: +{len(new_df)} rows → Total: {len(df)}")
    return df

def _fetch_single_incremental(isin: str, days_needed: int, max_hist: int, asset_type: str) -> pd.DataFrame:
    url = f"https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{isin}/{days_needed}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json().get('closingPriceDaily', [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df['date_int'] = df['dEven'].apply(shamsi_to_int)
        df = df[df['date_int'] > max_hist].drop(columns=['date_int'])
        if df.empty:
            return pd.DataFrame()
        df = _rename_columns(df, isin, url, asset_type)
        time.sleep(REQUEST_DELAY)
        return df
    except requests.RequestException:
        time.sleep(REQUEST_DELAY)
        return pd.DataFrame()

# ================== Common Rename & Clean ==================
def _rename_columns(df: pd.DataFrame, isin: str, url: str, asset_type: str) -> pd.DataFrame:
    df = df.rename(columns={
        'insCode': 'isin', 'dEven': 'date', 'hEven': 'hour',
        'pClosing': 'real_time_price', 'pDrCotVal': 'last',
        'priceChange': 'price_change', 'priceMin': 'low', 'priceMax': 'high',
        'priceYesterday': 'yesterday_price', 'priceFirst': 'open',
        'zTotTran': 'count', 'qTotTran5J': 'volume', 'qTotCap': 'value',
        'last': 'last_flag', 'iClose': 'i_close', 'yClose': 'y_close', 'id': 'id'
    })
    df = df[['isin', 'date', 'hour', 'real_time_price', 'last', 'price_change',
             'low', 'high', 'yesterday_price', 'open', 'count', 'volume', 'value',
             'last_flag', 'i_close', 'y_close', 'id']]
    df['source_isin'] = isin
    df['api_url'] = url
    if asset_type == 'underlying':
        df['asset_type'] = 'underlying'
    return df

def _clean_and_save_history(df: pd.DataFrame, history_file: str, asset_type: str) -> pd.DataFrame:
    df['date'] = df['date'].astype('Int64')
    df['hour'] = df['hour'].astype('Int64')
    non_num_cols = ['isin', 'source_isin', 'api_url', 'last_flag', 'i_close', 'y_close', 'id']
    if 'asset_type' in df.columns:
        non_num_cols.append('asset_type')
    num_cols = [c for c in df.columns if c not in non_num_cols]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
    df = df.sort_values(['isin', 'date', 'hour']).reset_index(drop=True)
    df.to_excel(history_file, index=False)
    logger.info(f"{asset_type.capitalize()} full history: {len(df)} rows saved")
    return df

# ================== 3. Options History ==================
def fetch_options_history(force_update: bool = False) -> pd.DataFrame:
    return fetch_history_generic(
        OPTIONS_LIVE_FILE,
        OPTIONS_HISTORY_FILE,
        isin_cols=['isin_put', 'isin_call'],
        asset_type='options',
        force_update=force_update
    )

# ================== 4. Underlying History ==================
def fetch_underlying_history(force_update: bool = False) -> pd.DataFrame:
    return fetch_history_generic(
        OPTIONS_LIVE_FILE,
        UNDERLYING_HISTORY_FILE,
        isin_cols='underlying_isin',
        asset_type='underlying',
        force_update=force_update
    )

# ================== 5. Live Underlying ==================
def fetch_underlying_live(force_update: bool = False) -> pd.DataFrame:
    if not force_update and is_file_fresh(UNDERLYING_LIVE_FILE):
        logger.info("4. Live underlying: Using fresh existing file")
        return pd.read_excel(UNDERLYING_LIVE_FILE)

    if not os.path.exists(OPTIONS_LIVE_FILE):
        logger.info("4. options_live.xlsx not found")
        return pd.DataFrame()

    df_live = pd.read_excel(OPTIONS_LIVE_FILE, usecols=['underlying_isin'], dtype=str)
    isin_list = df_live['underlying_isin'].dropna().astype(str).unique().tolist()

    def fetch_single(isin):
        url = f"https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/{isin}"
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json().get('closingPriceInfo')
            if not data:
                return None
            st = data.get('instrumentState', {})
            return {
                'isin': str(data.get('insCode', isin)),
                'source_isin': isin,
                'date': data.get('dEven', 0),
                'hour': data.get('hEven', 0),
                'real_time_price': data.get('pClosing', 0),
                'last': data.get('pDrCotVal', 0),
                'price_change': data.get('priceChange', 0),
                'low': data.get('priceMin', 0),
                'high': data.get('priceMax', 0),
                'yesterday_price': data.get('priceYesterday', 0),
                'open': data.get('priceFirst', 0),
                'count': data.get('zTotTran', 0),
                'volume': data.get('qTotTran5J', 0),
                'value': data.get('qTotCap', 0),
                'last_flag': bool(data.get('last', False)),
                'i_close': bool(data.get('iClose', False)),
                'y_close': bool(data.get('yClose', False)),
                'id': data.get('id', 0),
                'ticker': (st.get('lVal18AFC') or '').strip(),
                'name': (st.get('lVal30') or '').strip(),
                'status': (st.get('cEtavalTitle') or '').strip(),
                'api_url': url,
                'asset_type': 'underlying_live'
            }
        except requests.RequestException:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        futures = [ex.submit(fetch_single, i) for i in isin_list]
        for f in tqdm(as_completed(futures), total=len(isin_list), desc="4. Live underlying", leave=False):
            r = f.result()
            if r:
                results.append(r)
            time.sleep(REQUEST_DELAY)

    if not results:
        logger.info("4. Live underlying: 0 rows")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df[[
        'isin', 'source_isin', 'date', 'hour', 'real_time_price', 'last', 'price_change',
        'low', 'high', 'yesterday_price', 'open', 'count', 'volume', 'value',
        'last_flag', 'i_close', 'y_close', 'id', 'ticker', 'name', 'status', 'api_url', 'asset_type'
    ]]
    df['date'] = df['date'].astype('Int64')
    df['hour'] = df['hour'].astype('Int64')
    non_num_cols = ['isin', 'source_isin', 'api_url', 'asset_type', 'ticker', 'name', 'status', 'last_flag', 'i_close', 'y_close', 'id']
    num_cols = [c for c in df.columns if c not in non_num_cols]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
    df.to_excel(UNDERLYING_LIVE_FILE, index=False)
    logger.info(f"4. Live underlying: {len(df)} rows")
    return df

# ================== Main ==================
def main(force_update: bool = False):
    start = time.time()

    # 1. همیشه live options را آپدیت کن
    live = fetch_options_live(force_update=True)  # همیشه تازه
    if live.empty:
        logger.error("Failed to fetch live options. Cannot proceed.")
        return

    # 2. آپدیت history فقط اگر نیاز باشد
    fetch_options_history(force_update=force_update)
    fetch_underlying_history(force_update=force_update)
    fetch_underlying_live(force_update=force_update)

    logger.info(f"Done in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()