# Scripts/date_processor.py
import pandas as pd
import jdatetime
from datetime import datetime
import os

# ================== Settings ==================
OUTPUT_DIR = 'Output/1'
FILES = [
    f'{OUTPUT_DIR}/options_live.xlsx',
    f'{OUTPUT_DIR}/options_history_252.xlsx',
    f'{OUTPUT_DIR}/underlying_history_252.xlsx',
    f'{OUTPUT_DIR}/underlying_live.xlsx'
]

DATE_COLUMNS = {
    'options_live.xlsx': ['begin_date', 'end_date'],
    'options_history_252.xlsx': ['date'],
    'underlying_history_252.xlsx': ['date'],
    'underlying_live.xlsx': ['date']
}

ISIN_COLUMNS = ['isin', 'isin_put', 'isin_call', 'underlying_isin', 'source_isin']

# ================== Helper Functions ==================
def safe_int_convert(val):
    """تبدیل ایمن به عدد صحیح: str, float, int, pd.NA → int یا None"""
    if pd.isna(val) or val is None:
        return None
    try:
        return int(float(str(val).strip()))
    except:
        return None

def int_to_gregorian(date_int):
    """20250903 → datetime(2025, 9, 3)"""
    if not date_int or date_int < 10000000:
        return pd.NaT
    try:
        s = f"{date_int:08d}"
        return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except:
        return pd.NaT

def gregorian_to_jalali(dt):
    if pd.isna(dt):
        return pd.NaT
    try:
        jalali = jdatetime.datetime.fromgregorian(datetime=dt)
        return jalali.strftime('%Y/%m/%d')
    except:
        return pd.NaT

def get_latest_date(filepath: str, date_cols: list) -> int | None:
    if not os.path.exists(filepath):
        return None
    try:
        existing_cols = [col for col in date_cols if col in pd.read_excel(filepath, nrows=0).columns]
        if not existing_cols:
            return None
        df = pd.read_excel(filepath, usecols=existing_cols)
        all_dates = []
        for col in existing_cols:
            col_dates = df[col].apply(safe_int_convert).dropna()
            if not col_dates.empty:
                all_dates.append(col_dates)
        if not all_dates:
            return None
        combined = pd.concat([d for d in all_dates if len(d) > 0])
        return int(combined.max())
    except Exception as e:
        print(f"Error reading dates from {filepath}: {e}")
        return None

def should_process(filepath: str, flag_file: str, date_cols: list) -> bool:
    if not os.path.exists(filepath):
        return False
    current_date = get_latest_date(filepath, date_cols)
    if current_date is None:
        return True
    if not os.path.exists(flag_file):
        return True
    try:
        with open(flag_file, 'r') as f:
            last_processed = int(f.read().strip())
        needs_update = current_date > last_processed
        status = "Needs update" if needs_update else "Up to date"
        print(f" {os.path.basename(filepath)}: Latest = {current_date}, Processed = {last_processed} → {status}")
        return needs_update
    except:
        return True

def mark_processed(filepath: str, flag_file: str, date_cols: list):
    latest = get_latest_date(filepath, date_cols)
    if latest:
        with open(flag_file, 'w') as f:
            f.write(str(latest))

# ================== Main Processing ==================
def process_file(filepath: str, date_cols: list):
    filename = os.path.basename(filepath)
    flag_file = filepath + '.processed'

    if not should_process(filepath, flag_file, date_cols):
        print(f" {filename}: Already up to date")
        return

    print(f" Processing {filename}...")
    df = pd.read_excel(filepath)

    # 1. تمیز کردن ISIN
    for col in ISIN_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', '<NA>', '', 'NaN'], pd.NA)

    # 2. تبدیل تاریخ‌ها
    existing_date_cols = [col for col in date_cols if col in df.columns]
    for col in existing_date_cols:
        # مرحله 1: تبدیل ایمن به عدد صحیح
        df[col] = df[col].apply(safe_int_convert)
        # مرحله 2: تبدیل به datetime
        df[col] = df[col].apply(int_to_gregorian)
        # مرحله 3: تبدیل به شمسی
        df[col] = df[col].apply(gregorian_to_jalali)

    # ذخیره
    df.to_excel(filepath, index=False)
    mark_processed(filepath, flag_file, date_cols)
    print(f" {filename}: {len(df):,} rows processed. Jalali dates applied where possible.")

# ================== Main ==================
def main():
    print("Date & ISIN Processor Started")
    for file in FILES:
        if not os.path.exists(file):
            print(f" {os.path.basename(file)}: File not found, skipping")
            continue
        filename = os.path.basename(file)
        date_cols = DATE_COLUMNS.get(filename, [])
        if not date_cols:
            print(f" {filename}: No date columns defined, skipping")
            continue
        process_file(file, date_cols)
    print("Date & ISIN processing completed")

if __name__ == "__main__":
    main()