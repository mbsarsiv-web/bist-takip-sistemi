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
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

YFINANCE_SUTUNLAR = ['DOLAR', 'EURO', 'GR ALTIN', 'GR GÜMÜŞ', 'NASDAQ', 'S&P 500']

# ======================================================================
# HAFTALIK GEÇMİŞ YENİLEME AYARLARI
# ======================================================================
# Günlük motor ile AYNI alan kullanılır (tutarlılık için). Alan adını değiştirmek gerekirse yalnızca burası değişir.
FIYAT_ALANI = "HGDG_KAPANIS"
# Teşhis amaçlı karşılaştırılan ikinci alan (log'da iki alanın farklı olup olmadığı raporlanır, tabloya yazılmaz)
ALT_FIYAT_ALANI = "HG_KAPANIS"

HAFTALIK_PARCA_YIL = 5               # Geçmiş, kaç yıllık parçalar hâlinde çekilir
HAFTALIK_TOLERANS_MUTLAK = 0.0015    # Yuvarlama farkı toleransı (TL)
HAFTALIK_TOLERANS_ORAN = 0.003       # Yuvarlama farkı toleransı (oran, %0,3)
HAFTALIK_MIN_KAPSAMA = 0.8           # Yeni veri, eski kayıtların en az %80'ini kapsamalı; yoksa hisse atlanır
HAFTALIK_MAX_YAZILACAK_HISSE = 350   # Tek çalışmada en fazla bu kadar hissenin sütunu yeniden yazılır (güvenlik tavanı)
HAFTALIK_YAZMA_GRUBU = 15            # Tek yazma isteğinde en fazla bu kadar sütun
HAFTALIK_OKUMA_GRUBU = 40            # Tek okuma/çekme turunda en fazla bu kadar hisse
GUNLUK_SAYFA_ADI = "Düzeltme Günlüğü"


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


# ======================================================================
# HAFTALIK GEÇMİŞ YENİLEME MOTORU
# Amaç: Bölünme (bedelsiz/bedelli) gibi işlemlerde İş Yatırım geçmiş fiyatları yeniden hesaplar.
# Bu motor her hissenin tüm geçmişini İş Yatırım'dan baştan çeker, tablodakiyle karşılaştırır
# ve yalnızca farklı çıkan hissenin sütununu yeniden yazar. Fark yoksa hiçbir şey yazılmaz.
# ======================================================================

def hassas_yuvarla(val):
    v = float(val)
    return round(v, 4) if abs(v) < 1 else round(v, 3)

def saklanan_sayi(v):
    """Tablodan okunan hücre değerini pozitif sayıya çevirir; sayı değilse None döner."""
    if isinstance(v, bool): return None
    if isinstance(v, (int, float)):
        return float(v) if v > 0 else None
    if isinstance(v, str):
        s = v.strip()
        if s == "": return None
        try:
            x = float(s.replace(",", "."))
            return x if x > 0 else None
        except ValueError:
            return None
    return None

def haftalik_parcalar(ilk_tarih, son_tarih):
    parcalar = []
    bas = ilk_tarih
    while bas <= son_tarih:
        bit = min(datetime.date(bas.year + HAFTALIK_PARCA_YIL - 1, 12, 31), son_tarih)
        parcalar.append((bas.strftime('%d-%m-%Y'), bit.strftime('%d-%m-%Y')))
        bas = datetime.date(bit.year + 1, 1, 1)
    return parcalar

def haftalik_parca_cek(stock, bas_str, bit_str, max_deneme=5):
    """Dönüş: (satir_listesi, basarili_mi). Başarısız istek ile boş yanıt birbirinden ayrılır."""
    url = f"https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/Common/Data.aspx/HisseTekil?hisse={stock}&startdate={bas_str}&enddate={bit_str}"
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(max_deneme):
        try:
            time.sleep(random.uniform(0.1, 0.4))
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                satirlar = data.get('value') if isinstance(data, dict) else None
                return (satirlar if satirlar else []), True
            _backoff_bekle(attempt)
        except Exception:
            _backoff_bekle(attempt)
    return [], False

def haftalik_hisse_gecmisi(stock, parcalar):
    """Dönüş: (fiyat_sozlugu, alt_alan_sozlugu) ya da herhangi bir parça alınamazsa (None, None)."""
    fiyat = {}
    alt = {}
    for bas_str, bit_str in parcalar:
        satirlar, ok = haftalik_parca_cek(stock, bas_str, bit_str)
        if not ok: return None, None
        for s in satirlar:
            try:
                tarih = datetime.datetime.strptime(str(s.get('HGDG_TARIH'))[:10], '%d-%m-%Y').date()
            except Exception:
                continue
            v = s.get(FIYAT_ALANI)
            try:
                v = float(v)
            except (TypeError, ValueError):
                v = None
            if v is not None and v > 0: fiyat[tarih] = v
            a = s.get(ALT_FIYAT_ALANI)
            try:
                a = float(a)
            except (TypeError, ValueError):
                a = None
            if a is not None and a > 0: alt[tarih] = a
    return fiyat, alt

def _tolerans(x):
    return max(HAFTALIK_TOLERANS_MUTLAK, HAFTALIK_TOLERANS_ORAN * abs(x))

def haftalik_karsilastir(tarihler, eski, ham, yeni):
    """
    tarihler : satır sırasına göre tarih listesi (geçersiz satır için None)
    eski     : tablodaki sayısal fiyatlar (yoksa None)
    ham      : tablodaki ham hücre değerleri (değişmeyen satırlar aynen geri yazılır)
    yeni     : İş Yatırım'dan gelen {tarih: fiyat}
    """
    n = len(tarihler)
    if not yeni: return {'durum': 'degismedi'}

    eski_dolu = [i for i in range(n) if tarihler[i] is not None and eski[i] is not None]
    ortak = [i for i in eski_dolu if tarihler[i] in yeni]
    yeni_ilk = min(yeni.keys())

    if eski_dolu:
        if not ortak:
            return {'durum': 'atla', 'neden': 'tablo ile ortak tarih yok'}
        kapsanacak = [i for i in eski_dolu if tarihler[i] >= yeni_ilk]
        if kapsanacak and len(ortak) < HAFTALIK_MIN_KAPSAMA * len(kapsanacak):
            return {'durum': 'atla', 'neden': f'eksik veri geldi ({len(ortak)}/{len(kapsanacak)} gün)'}
        capa = max(ortak, key=lambda i: tarihler[i])
        capa_yeni = yeni[tarihler[capa]]
        if abs(eski[capa] - capa_yeni) > _tolerans(capa_yeni):
            return {'durum': 'atla', 'neden': f'en son ortak gün uyuşmuyor ({tarihler[capa].strftime("%d.%m.%Y")}: tablo={eski[capa]} yeni={capa_yeni})'}

    farkli = [i for i in ortak if abs(eski[i] - yeni[tarihler[i]]) > _tolerans(yeni[tarihler[i]])]
    doldurulacak = [i for i in range(n) if tarihler[i] is not None and eski[i] is None and tarihler[i] in yeni and tarihler[i].weekday() < 5]

    if not farkli and not doldurulacak:
        return {'durum': 'degismedi'}

    kolon = []
    oran = None
    tahmini = 0
    for i in range(n):
        d = tarihler[i]
        if d is not None and d in yeni:
            y = yeni[d]
            if eski[i] is not None and eski[i] > 0: oran = y / eski[i]
            kolon.append([hassas_yuvarla(y)])
        elif d is not None and eski[i] is not None and farkli and oran is not None and d >= yeni_ilk:
            # İş Yatırım'da o gün yok ama tabloda var: önceki ortak günün oranıyla düzeltilir
            kolon.append([hassas_yuvarla(eski[i] * oran)])
            tahmini += 1
        else:
            kolon.append([ham[i]])

    ayrinti = f"{len(farkli)} gün farklı, {len(doldurulacak)} boş gün dolduruldu"
    if farkli:
        ilk = min(farkli, key=lambda i: tarihler[i])
        yv = yeni[tarihler[ilk]]
        ayrinti += f"; ilk fark {tarihler[ilk].strftime('%d.%m.%Y')}: tablo={eski[ilk]} -> yeni={round(yv, 4)} (oran {round(yv / eski[ilk], 4)})"
        kapsanmayan = len([i for i in eski_dolu if tarihler[i] < yeni_ilk])
        if kapsanmayan: ayrinti += f"; UYARI: {kapsanmayan} eski gün İş Yatırım verisinin dışında kaldı, düzeltilemedi"
    if tahmini: ayrinti += f"; {tahmini} gün oranla düzeltildi"
    return {'durum': 'guncelle', 'kolon': kolon, 'ayrinti': ayrinti, 'farkli': len(farkli), 'doldurulan': len(doldurulacak)}

def haftalik_teshis(tarihler, yeni, alt):
    """HGDG_KAPANIS ile HG_KAPANIS arasında fark var mı? (yalnızca log için)"""
    if not alt or not yeni: return None
    farklar = [(d, yeni[d], alt[d]) for d in sorted(yeni.keys()) if d in alt and abs(yeni[d] - alt[d]) > _tolerans(yeni[d])]
    if not farklar: return None
    d0, a0, b0 = farklar[0]
    return f"{len(farklar)} gün farklı; ilk: {d0.strftime('%d.%m.%Y')} {FIYAT_ALANI}={a0} {ALT_FIYAT_ALANI}={b0}"

def haftalik_yaz(sheet, guncellemeler):
    for i in range(0, len(guncellemeler), HAFTALIK_YAZMA_GRUBU):
        parca = guncellemeler[i:i + HAFTALIK_YAZMA_GRUBU]
        data = [{'range': g['aralik'], 'values': g['degerler']} for g in parca]
        yazildi = False
        for deneme in range(5):
            try:
                sheet.batch_update(data, value_input_option='RAW')
                yazildi = True
                break
            except Exception as e:
                print(f"[!] Yazma hatası (deneme {deneme + 1}): {e}", flush=True)
                _backoff_bekle(deneme, taban=5.0, tavan=60.0)
        if not yazildi:
            raise RuntimeError("Sütunlar tabloya yazılamadı, işlem durduruldu.")
        time.sleep(2)

def haftalik_gunluk_yaz(sheet, satirlar):
    if not satirlar: return
    try:
        ss = sheet.spreadsheet
        try:
            ws = ss.worksheet(GUNLUK_SAYFA_ADI)
        except gspread.exceptions.WorksheetNotFound:
            ws = ss.add_worksheet(title=GUNLUK_SAYFA_ADI, rows=2000, cols=4)
            ws.update(values=[["ZAMAN", "HİSSE", "DURUM", "AYRINTI"]], range_name='A1')
        ws.append_rows(satirlar, value_input_option='RAW')
    except Exception as e:
        print(f"[!] Günlük sayfasına yazılamadı: {e}", flush=True)

def haftalik_motoru():
    baslangic = time.time()
    try:
        sheet, headers, tarih_to_row, son_satir, son_tarih_str = baglanti_ve_bilgi_getir()
        if not son_tarih_str:
            print("[!] Tabloda geçerli tarih bulunamadı, işlem iptal.", flush=True)
            return

        satir_tarihi = {}
        for tstr, row in tarih_to_row.items():
            satir_tarihi[row] = datetime.datetime.strptime(tstr, "%d.%m.%Y").date()
        tarihler = [satir_tarihi.get(r) for r in range(2, son_satir + 1)]
        n = len(tarihler)
        ilk_tarih = min(satir_tarihi.values())
        bugun = datetime.datetime.now().date()
        parcalar = haftalik_parcalar(ilk_tarih, bugun)

        hisseler = []
        for idx, ad in enumerate(headers):
            if idx == 0 or not ad or ad in YFINANCE_SUTUNLAR: continue
            hisseler.append((ad, idx + 1))

        print(f"[HAFTALIK] {len(hisseler)} hisse, {n} satır, {ilk_tarih} → {bugun}, {len(parcalar)} parça/hisse", flush=True)

        zaman = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        sayac = {'degismedi': 0, 'guncelle': 0, 'atla': 0, 'cekilemedi': 0}
        gunluk = []
        teshis_adet = 0
        teshis_ornek = []
        tavan_asildi = False

        for g in range(0, len(hisseler), HAFTALIK_OKUMA_GRUBU):
            grup = hisseler[g:g + HAFTALIK_OKUMA_GRUBU]
            c1 = min(c for _, c in grup)
            c2 = max(c for _, c in grup)

            aralik = f"{rowcol_to_a1(2, c1)}:{rowcol_to_a1(son_satir, c2)}"
            blok = None
            for deneme in range(5):
                try:
                    blok = sheet.get(aralik, value_render_option='UNFORMATTED_VALUE')
                    break
                except Exception as e:
                    print(f"[!] Okuma hatası (deneme {deneme + 1}): {e}", flush=True)
                    _backoff_bekle(deneme, taban=5.0, tavan=60.0)
            if blok is None:
                raise RuntimeError(f"Tablo okunamadı: {aralik}")
            time.sleep(1.5)

            tum_gecmis = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(haftalik_hisse_gecmisi, ad, parcalar): ad for ad, _ in grup}
                for future in concurrent.futures.as_completed(futures):
                    ad = futures[future]
                    try:
                        tum_gecmis[ad] = future.result()
                    except Exception:
                        tum_gecmis[ad] = (None, None)

            guncellemeler = []
            for ad, col in grup:
                j = col - c1
                ham = []
                eski = []
                for r in range(n):
                    satir = blok[r] if r < len(blok) else []
                    v = satir[j] if j < len(satir) else ""
                    ham.append(v)
                    eski.append(saklanan_sayi(v))

                yeni, alt = tum_gecmis.get(ad, (None, None))
                if yeni is None:
                    sayac['cekilemedi'] += 1
                    gunluk.append([zaman, ad, "ATLANDI", "İş Yatırım'dan veri alınamadı"])
                    continue

                t = haftalik_teshis(tarihler, yeni, alt)
                if t:
                    teshis_adet += 1
                    if len(teshis_ornek) < 5: teshis_ornek.append(f"{ad}: {t}")

                sonuc = haftalik_karsilastir(tarihler, eski, ham, yeni)
                sayac[sonuc['durum']] += 1

                if sonuc['durum'] == 'atla':
                    gunluk.append([zaman, ad, "ATLANDI", sonuc['neden']])
                elif sonuc['durum'] == 'guncelle':
                    if sayac['guncelle'] > HAFTALIK_MAX_YAZILACAK_HISSE:
                        tavan_asildi = True
                        sayac['guncelle'] -= 1
                        gunluk.append([zaman, ad, "ERTELENDI", "Tek çalışmadaki güvenlik tavanı aşıldı, gelecek hafta yenilenecek"])
                        continue
                    a1 = f"{rowcol_to_a1(2, col)}:{rowcol_to_a1(son_satir, col)}"
                    guncellemeler.append({'aralik': a1, 'degerler': sonuc['kolon']})
                    gunluk.append([zaman, ad, "GÜNCELLENDİ", sonuc['ayrinti']])
                    print(f"[GÜNCELLENDİ] {ad}: {sonuc['ayrinti']}", flush=True)

            if guncellemeler:
                haftalik_yaz(sheet, guncellemeler)

            del blok, tum_gecmis
            gc.collect()
            print(f"[HAFTALIK] {min(g + HAFTALIK_OKUMA_GRUBU, len(hisseler))}/{len(hisseler)} hisse tarandı", flush=True)

        haftalik_gunluk_yaz(sheet, gunluk)

        sure = int(time.time() - baslangic)
        print("=" * 60, flush=True)
        print(f"[HAFTALIK ÖZET] Değişmeyen: {sayac['degismedi']} | Güncellenen: {sayac['guncelle']} | Atlanan: {sayac['atla']} | Alınamayan: {sayac['cekilemedi']} | Süre: {sure // 60} dk {sure % 60} sn", flush=True)
        if tavan_asildi:
            print(f"[UYARI] Güvenlik tavanı ({HAFTALIK_MAX_YAZILACAK_HISSE}) aşıldı, kalan hisseler gelecek haftaya ertelendi.", flush=True)
        if teshis_adet:
            print(f"[ALAN TEŞHİSİ] {teshis_adet} hissede {FIYAT_ALANI} ile {ALT_FIYAT_ALANI} farklı. Örnekler:", flush=True)
            for o in teshis_ornek: print("   " + o, flush=True)
        else:
            print(f"[ALAN TEŞHİSİ] {FIYAT_ALANI} ile {ALT_FIYAT_ALANI} hiçbir hissede farklı değil.", flush=True)
        print("=" * 60, flush=True)

    except Exception:
        print(traceback.format_exc(), flush=True)
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "guncelle": guncelleme_motoru()
        elif sys.argv[1] == "bakim": bakim_motoru(15)
        elif sys.argv[1] == "haftalik": haftalik_motoru()
