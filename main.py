import os
import sys
import requests
import pandas as pd
import datetime
import concurrent.futures
import re
import yfinance as yf
import gspread
import time
import random
import traceback
import gc
from oauth2client.service_account import ServiceAccountCredentials

YFINANCE_SUTUNLAR = ['DOLAR', 'EURO', 'GR ALTIN', 'GR GÜMÜŞ', 'NASDAQ', 'S&P 500']

def _backoff_bekle(deneme_no, taban=1.5, tavan=20.0):
    sure = min(tavan, taban * (2 ** deneme_no)) + random.uniform(0, 1.0)
    time.sleep(sure)

def get_all_bist_stocks():
    try:
        url = "https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/Tarihsel-Fiyat-Bilgileri.aspx"
        r = requests.get(url, timeout=10)
        matches = re.findall(r'<option value="([A-Z0-9]{4,5})">', r.text)
        if matches:
            return list(set([m for m in matches if not m.startswith('XU') and not m.startswith('XY') and m.isalpha()]))
    except Exception as e:
        print(f"[!] Halka arz kontrolü başarısız: {e}", flush=True)
    return []

def fetch_data_for_date_range(stock, start_date, end_date, max_deneme=5):
    url = f"https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/Common/Data.aspx/HisseTekil?hisse={stock}&startdate={start_date}&enddate={end_date}"
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(max_deneme):
        try:
            time.sleep(random.uniform(0.1, 0.4))
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if 'value' in data and data['value']:
                    df = pd.DataFrame(data['value'])
                    df['Tarih'] = pd.to_datetime(df['HGDG_TARIH'].str[:10], format='%d-%m-%Y', errors='coerce')
                    df['Hisse'] = stock
                    df['Kapanis'] = df['HGDG_KAPANIS']
                    return df[['Tarih', 'Hisse', 'Kapanis']]
                return pd.DataFrame()
            _backoff_bekle(attempt)
        except:
            _backoff_bekle(attempt)
    return pd.DataFrame()

def fetch_symbol_series(symbol, start_date, end_date, fallback_symbols=None, label="", max_deneme=5):
    denenecekler = [symbol] + (fallback_symbols or [])
    for sym in denenecekler:
        for attempt in range(max_deneme):
            try:
                df = yf.download(sym, start=start_date, end=end_date, progress=False, auto_adjust=False, threads=False)
                if df is not None and not df.empty and 'Close' in df.columns:
                    s = df['Close']
                    if isinstance(s, pd.DataFrame): s = s.iloc[:, 0]
                    s = s.dropna()
                    if not s.empty: return s
                _backoff_bekle(attempt, taban=2.0, tavan=30.0)
            except Exception as e:
                _backoff_bekle(attempt, taban=2.0, tavan=30.0)
    return pd.Series(dtype=float)

def fetch_frankfurter_fallback(start_date, end_date):
    try:
        url = f"https://api.frankfurter.app/{start_date}..{end_date}?from=USD&to=TRY,EUR"
        r = requests.get(url, timeout=15)
        if r.status_code != 200: return pd.DataFrame()
        data = r.json().get('rates', {})
        if not data: return pd.DataFrame()
        satirlar = []
        for tarih_str, kurlar in data.items():
            if kurlar.get('TRY'):
                satir = {'DOLAR': kurlar.get('TRY')}
                if kurlar.get('EUR') and kurlar.get('EUR') > 0: satir['EURO'] = kurlar.get('TRY') / kurlar.get('EUR')
                satirlar.append((pd.to_datetime(tarih_str), satir))
        if not satirlar: return pd.DataFrame()
        satirlar.sort(key=lambda x: x[0])
        return pd.DataFrame([s[1] for s in satirlar], index=[s[0] for s in satirlar])
    except: return pd.DataFrame()

def get_yfinance_incremental(start_date, end_date):
    ons_carpan = 31.1034768
    usd_try = fetch_symbol_series('TRY=X', start_date, end_date, label="USD/TRY")
    eur_try = fetch_symbol_series('EURTRY=X', start_date, end_date, label="EUR/TRY")
    gold_usd = fetch_symbol_series('GC=F', start_date, end_date, fallback_symbols=['XAUUSD=X'])
    silver_usd = fetch_symbol_series('SI=F', start_date, end_date, fallback_symbols=['XAGUSD=X'])
    nasdaq_usd = fetch_symbol_series('^IXIC', start_date, end_date)
    sp500_usd = fetch_symbol_series('^GSPC', start_date, end_date)

    if usd_try.empty or eur_try.empty:
        yedek = fetch_frankfurter_fallback(start_date, end_date)
        if not yedek.empty:
            if usd_try.empty and 'DOLAR' in yedek.columns: usd_try = yedek['DOLAR']
            if eur_try.empty and 'EURO' in yedek.columns: eur_try = yedek['EURO']

    ham_seriler = {'DOLAR': usd_try, 'EURO': eur_try, '_GOLD': gold_usd, '_SILVER': silver_usd, '_NASDAQ': nasdaq_usd, '_SP500': sp500_usd}
    dolu_seriler = {k: v for k, v in ham_seriler.items() if not v.empty}
    if not dolu_seriler: return pd.DataFrame()

    union_index = None
    for s in dolu_seriler.values(): union_index = s.index if union_index is None else union_index.union(s.index)

    combined = pd.DataFrame(index=union_index)
    for k, s in dolu_seriler.items(): combined[k] = s.reindex(union_index).ffill().bfill()
    combined = combined.sort_index()

    ekstra_df = pd.DataFrame(index=combined.index)
    if 'DOLAR' in combined.columns: ekstra_df['DOLAR'] = combined['DOLAR']
    if 'EURO' in combined.columns: ekstra_df['EURO'] = combined['EURO']
    if 'DOLAR' in combined.columns and '_GOLD' in combined.columns: ekstra_df['GR ALTIN'] = (combined['_GOLD'] * combined['DOLAR']) / ons_carpan
    if 'DOLAR' in combined.columns and '_SILVER' in combined.columns: ekstra_df['GR GÜMÜŞ'] = (combined['_SILVER'] * combined['DOLAR']) / ons_carpan
    if 'DOLAR' in combined.columns and '_NASDAQ' in combined.columns: ekstra_df['NASDAQ'] = combined['_NASDAQ'] * combined['DOLAR']
    if 'DOLAR' in combined.columns and '_SP500' in combined.columns: ekstra_df['S&P 500'] = combined['_SP500'] * combined['DOLAR']
    ekstra_df = ekstra_df.dropna(how='all')
    if not ekstra_df.empty: ekstra_df.index = pd.to_datetime(ekstra_df.index).tz_localize(None).normalize()
    return ekstra_df

def format_fiyat(val):
    if pd.isna(val) or val == "": return ""
    try: return "{:.3f}".format(float(val)).replace(".", ",")
    except ValueError: return ""

def coklu_tur_bist_onarimi(bist_pivot, max_tur=3):
    for tur in range(1, max_tur + 1):
        eksik_hisseler = bist_pivot.columns[bist_pivot.isna().any()].tolist()
        if not eksik_hisseler: break
        if tur > 1: time.sleep(3 + random.uniform(0, 2))
        for hisse in eksik_hisseler:
            for eksik_tarih in bist_pivot[bist_pivot[hisse].isna()].index:
                if eksik_tarih.weekday() >= 5: continue
                tarih_str_hedef = eksik_tarih.strftime('%d-%m-%Y')
                kurtarma_df = fetch_data_for_date_range(hisse, tarih_str_hedef, tarih_str_hedef)
                if not kurtarma_df.empty: bist_pivot.at[eksik_tarih, hisse] = kurtarma_df.iloc[0]['Kapanis']
    return bist_pivot

def baglanti_ve_bilgi_getir():
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('google_creds.json', scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key("1alSTQD8LsM7nMAXV58v3ij1Ke6H69g1pXwJsJtT9AJ4")
    sheet = ss.worksheet("Veritabanı")
    headers = [h.strip() for h in sheet.row_values(1) if h.strip()]
    tarih_sutunu = sheet.col_values(1)
    last_valid_row_idx = 1
    last_date_str = ""
    tarih_to_row = {}
    for i, val in enumerate(tarih_sutunu):
        if i == 0: continue
        temiz_val = val.strip()
        if re.match(r'\d{2}\.\d{2}\.\d{4}', temiz_val):
            row_no = i + 1
            tarih_to_row[temiz_val] = row_no
            last_valid_row_idx = row_no
            last_date_str = temiz_val
    return sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str

def guncelleme_motoru():
    try:
        sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
        if not last_date_str: return

        # --- GÜN İÇİ SIFIRLAMA MANTIĞI EKLENDİ ---
        last_date = datetime.datetime.strptime(last_date_str, "%d.%m.%Y")
        today = datetime.datetime.now()
        
        if last_date.date() == today.date():
            sheet.delete_row(last_valid_row_idx) # Bugünün eksik verisini içeren son satırı sil
            time.sleep(2)
            # Satır silindiği için veritabanı bilgilerini tazeleyerek yeniden alıyoruz
            sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
            if not last_date_str: return
            last_date = datetime.datetime.strptime(last_date_str, "%d.%m.%Y")
        # -----------------------------------------

        aktif_hisseler = get_all_bist_stocks()
        if aktif_hisseler:
            yeni_hisseler = [h for h in aktif_hisseler if h not in headers]
            if yeni_hisseler:
                headers.extend(yeni_hisseler)
                sheet.update(values=[headers], range_name='A1', value_input_option='USER_ENTERED')
                time.sleep(2)

        row_to_tarih = {v: k for k, v in tarih_to_row.items()}
        yf_col_indices = {ad: headers.index(ad) + 1 for ad in YFINANCE_SUTUNLAR if ad in headers}
        yf_last_valid_date = {}

        for ad, idx in yf_col_indices.items():
            col_values = sheet.col_values(idx)
            son_dolu_satir = None
            for i in range(len(col_values) - 1, 0, -1):
                if col_values[i].strip() != "":
                    son_dolu_satir = i + 1
                    break
            if son_dolu_satir and son_dolu_satir in row_to_tarih: yf_last_valid_date[ad] = datetime.datetime.strptime(row_to_tarih[son_dolu_satir], "%d.%m.%Y")
            else: yf_last_valid_date[ad] = last_date - datetime.timedelta(days=365)

        en_geri_kalan_tarih = min(yf_last_valid_date.values()) if yf_last_valid_date else last_date
        yf_gap_var = en_geri_kalan_tarih < last_date
        if last_date.date() >= today.date() and not yf_gap_var: return

        start_fetch = (last_date + datetime.timedelta(days=1)).strftime("%d-%m-%Y")
        end_fetch = today.strftime("%d-%m-%Y")
        bist_pivot = pd.DataFrame()

        if last_date.date() < today.date():
            stocks = [h for h in headers if h not in (['Tarih'] + YFINANCE_SUTUNLAR) and h]
            all_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_data_for_date_range, stock, start_fetch, end_fetch, 5): stock for stock in stocks}
                for future in concurrent.futures.as_completed(futures):
                    df = future.result()
                    if not df.empty: all_data.append(df)
            if all_data:
                master_df = pd.concat(all_data, ignore_index=True)
                bist_pivot = master_df.pivot_table(index='Tarih', columns='Hisse', values='Kapanis', aggfunc='first')
                bist_pivot.index = pd.to_datetime(bist_pivot.index).tz_localize(None).normalize()
                bist_pivot = coklu_tur_bist_onarimi(bist_pivot, max_tur=3)

        yf_start = (en_geri_kalan_tarih + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        yf_end = (today + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        yfinance_kuru = get_yfinance_incremental(yf_start, yf_end)
        hedef_tarih = pd.to_datetime(last_date_str, format="%d.%m.%Y")

        if not yfinance_kuru.empty:
            yf_gap_df = yfinance_kuru[yfinance_kuru.index <= hedef_tarih]
            if not yf_gap_df.empty:
                gap_updates = []
                for tarih_idx, row_data in yf_gap_df.iterrows():
                    row_no = tarih_to_row.get(tarih_idx.strftime('%d.%m.%Y'))
                    if not row_no: continue
                    for col_name in YFINANCE_SUTUNLAR:
                        if col_name in row_data.index and col_name in yf_col_indices:
                            formatted_val = format_fiyat(row_data[col_name])
                            if formatted_val != "": gap_updates.append(gspread.Cell(row=row_no, col=yf_col_indices[col_name], value=formatted_val))
                if gap_updates: sheet.update_cells(gap_updates, value_input_option='USER_ENTERED')
            yf_new_df = yfinance_kuru[yfinance_kuru.index > hedef_tarih]
        else: yf_new_df = pd.DataFrame()

        final_df = bist_pivot.join(yf_new_df, how='outer') if not bist_pivot.empty or not yf_new_df.empty else pd.DataFrame()
        if final_df.empty: return
        final_df = final_df.sort_index()[final_df.index > hedef_tarih]
        if final_df.empty: return

        final_df.index = final_df.index.strftime('%d.%m.%Y')
        final_df.index.name = 'Tarih'
        final_df.reset_index(inplace=True)

        for col in headers:
            if col not in final_df.columns: final_df[col] = ""
        final_df = final_df[headers]
        for col in final_df.columns:
            if col != 'Tarih': final_df[col] = final_df[col].apply(format_fiyat)
        final_df = final_df.fillna("")

        yeni_satirlar = final_df.values.tolist()
        if yeni_satirlar:
            start_row = last_valid_row_idx + 1
            if sheet.row_count < start_row + len(yeni_satirlar): sheet.add_rows((start_row + len(yeni_satirlar)) - sheet.row_count + 50)
            sheet.update(values=yeni_satirlar, range_name=f"A{start_row}", value_input_option='USER_ENTERED')

    except Exception as e: print(traceback.format_exc(), flush=True)

def bakim_motoru(taranacak_gun=15):
    try:
        sheet, headers, tarih_to_row, last_valid_row_idx, _ = baglanti_ve_bilgi_getir()
        baslangic_satiri = max(2, last_valid_row_idx - taranacak_gun)
        son_satirlar = sheet.get_all_values()[baslangic_satiri - 1:last_valid_row_idx]
        eksik_guncellemeler = []
        for satir_verisi in son_satirlar:
            satir_tarihi_str = satir_verisi[0].strip()
            if not re.match(r'\d{2}\.\d{2}\.\d{4}', satir_tarihi_str): continue
            satir_dt = datetime.datetime.strptime(satir_tarihi_str, "%d.%m.%Y")
            if satir_dt.weekday() >= 5: continue
            gercek_satir_no = tarih_to_row[satir_tarihi_str]
            for col_idx in range(1, len(headers)):
                hisse_adi = headers[col_idx]
                if hisse_adi in YFINANCE_SUTUNLAR or not hisse_adi: continue
                if (satir_verisi[col_idx].strip() if col_idx < len(satir_verisi) else "") == "":
                    tarih_api = satir_dt.strftime('%d-%m-%Y')
                    kurtarma_df = fetch_data_for_date_range(hisse_adi, tarih_api, tarih_api)
                    if not kurtarma_df.empty:
                        yfiyat = format_fiyat(kurtarma_df.iloc[0]['Kapanis'])
                        if yfiyat != "": eksik_guncellemeler.append(gspread.Cell(row=gercek_satir_no, col=col_idx + 1, value=yfiyat))
        if eksik_guncellemeler: sheet.update_cells(eksik_guncellemeler, value_input_option='USER_ENTERED')
    except Exception as e: print(traceback.format_exc(), flush=True)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "guncelle": guncelleme_motoru()
        elif sys.argv[1] == "bakim": bakim_motoru(15)
