import os
import sys
import json
import math
import requests
import pandas as pd
import datetime
import concurrent.futures
import re
import bisect
import yfinance as yf
import gspread
import time
import random
import traceback
import gc
import threading
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

YFINANCE_SUTUNLAR = ['DOLAR', 'EURO', 'GR ALTIN', 'GR GÜMÜŞ', 'NASDAQ', 'S&P 500']

# ======================================================================
# GÜNCELLEME KAYITLARI (web uygulamasındaki "GÜNCELLEMELER" sekmesi)
# Her motor çalışmasının sonucu, tablodaki "Güncelleme Kayıtları" sayfasına tek satır olarak yazılır.
# Sütunlar: ZAMAN (TR) | MOTOR | DURUM | İŞLENEN TARİH | DOLULUK | SÜRE | AYRINTI
# Sayfa yoksa otomatik oluşturulur. Haftalık motor, sayfayı son KAYIT_MAKS_SATIR satırla sınırlar.
# AYRINTI sütununda, okunabilir metnin ardından " ## " ayracıyla web uygulamasının okuduğu ayrıntı (JSON) bulunabilir.
# ======================================================================
KAYIT_SAYFA_ADI = "Güncelleme Kayıtları"
KAYIT_BASLIKLAR = ["ZAMAN (TR)", "MOTOR", "DURUM", "İŞLENEN TARİH", "DOLULUK", "SÜRE", "AYRINTI"]
KAYIT_MAKS_SATIR = 1500
KAYIT_AYRINTI_MAKS = 3000             # AYRINTI hücresinin en fazla karakter sayısı (ayrıntı JSON'u kesilmesin diye geniş tutulur)
TR_SAAT_DILIMI = datetime.timezone(datetime.timedelta(hours=3))   # Türkiye: sabit UTC+3
AYRINTI_AYRACI = " ## "

_SS_ONBELLEK = {}
_HATA_SAYACI = {'isyatirim': 0}   # İş Yatırım'dan, tüm denemelere rağmen alınamayan istek sayısı
_HATA_KILIDI = threading.Lock()

# ======================================================================
# HAFTALIK GEÇMİŞ YENİLEME AYARLARI
# ======================================================================
# Günlük motor ile AYNI alan kullanılır (tutarlılık için). Alan adını değiştirmek gerekirse yalnızca burası değişir.
FIYAT_ALANI = "HGDG_KAPANIS"
# Teşhis amaçlı karşılaştırılan ikinci alan (ham kapanış)
ALT_FIYAT_ALANI = "HG_KAPANIS"

# Haftalık motorun tabloya yazacağı geçmiş fiyat serisinin yöntemi.
# Kod değiştirmeden seçilir: GitHub > Settings > Secrets and variables > Actions > Variables > FIYAT_YONTEMI
# (workflow'u elle çalıştırırken "yontem" listesinden de seçilebilir). Değer yoksa ISYATIRIM kullanılır.
#   "ISYATIRIM" : İş Yatırım'ın düzeltilmiş kapanışı (HGDG_KAPANIS).
#   "SERMAYE"   : Ham kapanış (HG_KAPANIS) x (o günkü SERMAYE / en güncel SERMAYE).
#   "HIBRIT"    : İş Yatırım düzeltmesi korunur; sermaye artışıyla eşleşen BEDELSİZ olaylarında
#                 İş Yatırım'ın çarpanı yerine kesin sermaye oranı kullanılır. Bedelli/diğer olaylara dokunulmaz.
# Hangisinin seçileceğine 'finnet' motorunun raporu karar verir.
_GECERLI_YONTEMLER = ("ISYATIRIM", "SERMAYE", "HIBRIT")
FIYAT_YONTEMI = os.environ.get("FIYAT_YONTEMI", "ISYATIRIM").strip().upper()
if FIYAT_YONTEMI not in _GECERLI_YONTEMLER:
    print(f"[!] Geçersiz FIYAT_YONTEMI '{FIYAT_YONTEMI}', ISYATIRIM kullanılıyor.", flush=True)
    FIYAT_YONTEMI = "ISYATIRIM"

HAFTALIK_PARCA_YIL = 5               # Geçmiş, kaç yıllık parçalar hâlinde çekilir
HAFTALIK_TOLERANS_MUTLAK = 0.0001    # Eşleşme/çapa kontrolü toleransı (TL)
HAFTALIK_TOLERANS_ORAN = 0.0005      # Eşleşme/çapa kontrolü toleransı (oran, %0,05)
# Değişiklik tespiti için çok daha hassas tolerans: eski sürümlerden kalan yuvarlanmış değerler de düzeltilir.
# (Tabloya 6 ondalık yazıldığı için yuvarlama hatası en fazla 5e-7'dir; bu tolerans onun üzerindedir.)
HAFTALIK_TOLERANS_HASSAS_MUTLAK = 0.000002
HAFTALIK_TOLERANS_HASSAS_ORAN = 0.00002
HAFTALIK_MIN_KAPSAMA = 0.8           # Yeni veri, eski kayıtların en az %80'ini kapsamalı; yoksa hisse atlanır
HAFTALIK_MAX_YAZILACAK_HISSE = 350   # Tek çalışmada en fazla bu kadar hissenin sütunu yeniden yazılır (güvenlik tavanı)
HAFTALIK_YAZMA_GRUBU = 15            # Tek yazma isteğinde en fazla bu kadar sütun
HAFTALIK_OKUMA_GRUBU = 40            # Tek okuma/çekme turunda en fazla bu kadar hisse
GUNLUK_SAYFA_ADI = "Düzeltme Günlüğü"

HIBRIT_PENCERE_GUN = 45              # Fiyat düzeltme olayı ile sermaye değişimi arasında izin verilen en fazla gün farkı

# Günlük motorda sondaki "tarihi var ama hiçbir hisse fiyatı yok" satırlarını temizlerken en fazla kaç satıra bakılır
BOS_SATIR_TARAMA_ADEDI = 10

# Bakım raporunda (web uygulamasında gösterilen ayrıntıda) listelenecek en fazla hisse adedi
BAKIM_RAPOR_BOS_HISSE_MAKS = 40      # "Taranan günlerin hiçbirinde fiyatı olmayan" hisse adı sayısı
BAKIM_RAPOR_KISMI_HISSE_MAKS = 15    # "Bazı günleri eksik kalan" hisse adı sayısı
BAKIM_RAPOR_TARIH_MAKS = 4           # Her hisse / kur sütunu için gösterilecek en fazla gün sayısı

# ======================================================================
# HIZLI KONTROL (YOKLAMA) AYARLARI
# 'kontrol' komutu, yeni günün kapanış verisi İş Yatırım'da yayınlandı mı diye bu likit hisselere bakar.
# Yayınlandıysa normal 'guncelle' motoru çalışır, yayınlanmadıysa saniyeler içinde çıkar.
# ======================================================================
BIST_YOKLAMA_HISSELERI = ["THYAO", "GARAN", "AKBNK", "ASELS", "EREGL", "BIMAS", "KCHOL", "SISE", "TUPRS", "ISCTR"]
BIST_YOKLAMA_ESIK_ORANI = 0.7        # Yoklama hisselerinin en az bu oranında yeni gün verisi varsa "yayınlandı" sayılır

# Seçilen yöntem uygulanamadığında (örn. SERMAYE verisi yok) İş Yatırım serisine dönülen hisseler
_YONTEM_GERI_DONUS = []


def _backoff_bekle(deneme_no, taban=1.5, tavan=20.0):
    sure = min(tavan, taban * (2 ** deneme_no)) + random.uniform(0, 1.0)
    time.sleep(sure)

# ======================================================================
# GÜNCELLEME KAYITLARI YARDIMCILARI
# ======================================================================

def _ss_olustur():
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('google_creds.json', scope)
    client = gspread.authorize(creds)
    return client.open_by_key("1alSTQD8LsM7nMAXV58v3ij1Ke6H69g1pXwJsJtT9AJ4")

def sure_metni(saniye):
    saniye = int(round(saniye))
    if saniye >= 60:
        return f"{saniye // 60} dk {saniye % 60} sn"
    return f"{saniye} sn"

def _tarih_ozeti(tarihler):
    """['19.11.2026', '20.11.2026'] -> '19.11.2026, 20.11.2026' (3'ten fazlaysa 'ilk – son (N gün)')"""
    tarihler = [t for t in tarihler if t]
    if not tarihler: return "-"
    if len(tarihler) <= 3: return ", ".join(tarihler)
    return f"{tarihler[0]} – {tarihler[-1]} ({len(tarihler)} gün)"

def _ad_ozeti(adlar, maks=6):
    adlar = list(adlar)
    if len(adlar) <= maks: return ", ".join(adlar)
    return ", ".join(adlar[:maks]) + f" +{len(adlar) - maks}"

def _tarihleri_sirala(tarih_kumesi):
    def anahtar(t):
        try: return datetime.datetime.strptime(t, "%d.%m.%Y")
        except Exception: return datetime.datetime.min
    return sorted(tarih_kumesi, key=anahtar)

def _json_ekle(metin, sozluk):
    """Okunabilir metnin sonuna, web uygulamasının okuyacağı ayrıntıyı (JSON) ekler."""
    return metin + AYRINTI_AYRACI + json.dumps(sozluk, ensure_ascii=False, separators=(',', ':'))

def _kayit_sayfasi_getir(yeni_baglanti=False):
    ss = None if yeni_baglanti else _SS_ONBELLEK.get('ss')
    if ss is None:
        ss = _ss_olustur()
        _SS_ONBELLEK['ss'] = ss
    try:
        ws = ss.worksheet(KAYIT_SAYFA_ADI)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=KAYIT_SAYFA_ADI, rows=KAYIT_MAKS_SATIR + 200, cols=len(KAYIT_BASLIKLAR))
        ws.update(values=[KAYIT_BASLIKLAR], range_name='A1', value_input_option='RAW')
    return ws

def kayit_yaz(motor, durum, islenen="-", doluluk="-", sure_sn=None, ayrinti=""):
    """Motor sonucunu 'Güncelleme Kayıtları' sayfasına tek satır olarak ekler. Hata verirse ana işlemi bozmaz."""
    satir = [
        datetime.datetime.now(TR_SAAT_DILIMI).strftime("%d.%m.%Y %H:%M"),
        motor,
        durum,
        islenen or "-",
        doluluk or "-",
        sure_metni(sure_sn) if sure_sn is not None else "-",
        str(ayrinti)[:KAYIT_AYRINTI_MAKS],
    ]
    for deneme in range(2):
        try:
            ws = _kayit_sayfasi_getir(yeni_baglanti=(deneme > 0))
            ws.append_rows([satir], value_input_option='RAW')
            print(f"[KAYIT] {motor} | {durum} | {satir[3]} | {satir[4]} | {satir[5]} | {satir[6].split(AYRINTI_AYRACI)[0]}", flush=True)
            return True
        except Exception as e:
            print(f"[!] Güncelleme kaydı yazılamadı (deneme {deneme + 1}): {e}", flush=True)
            time.sleep(3)
    return False

def kayit_budama(max_satir=KAYIT_MAKS_SATIR):
    """Kayıt sayfasında en yeni max_satir satırı bırakır, en eskileri siler."""
    try:
        ws = _kayit_sayfasi_getir()
        veri_satiri = len(ws.col_values(1)) - 1
        fazla = veri_satiri - max_satir
        if fazla > 0:
            ws.delete_rows(2, fazla + 1)
            print(f"[KAYIT] Eski {fazla} kayıt silindi (en fazla {max_satir} satır tutulur).", flush=True)
    except Exception as e:
        print(f"[!] Kayıt budama atlandı: {e}", flush=True)

def _sonucu_kaydet(motor, sonuc, baslangic):
    kayit_yaz(motor, sonuc['durum'], sonuc['tarih'], sonuc['doluluk'], time.time() - baslangic, sonuc['ayrinti'])

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
    # Tüm denemeler başarısız oldu (boş yanıt değil, gerçek bağlantı/sunucu hatası)
    with _HATA_KILIDI:
        _HATA_SAYACI['isyatirim'] += 1
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
    try:
        # Finnet gibi platformlarla yüzdelik getirilerin kuruşu kuruşuna tutması için
        # yüksek ondalık hassasiyeti korunur. Sondaki gereksiz sıfırlar atılır.
        formatted = "{:.6f}".format(float(val)).rstrip('0')
        if formatted.endswith('.'):
            formatted = formatted[:-1]
        return formatted.replace(".", ",")
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
    ss = _ss_olustur()
    _SS_ONBELLEK['ss'] = ss
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

def bos_son_satirlari_temizle(sheet, headers, last_valid_row_idx, max_satir=BOS_SATIR_TARAMA_ADEDI):
    """Tablonun sonunda, tarihi olduğu halde HİÇBİR hisse fiyatı bulunmayan satırları siler.
    Böyle bir satır kalırsa günlük motor 'bu tarih işlendi' sanır ve fiyatı bir daha çekmez;
    uygulama da son fiyat yerine eski fiyatı gösterir. Satır silinince motor o tarihi yeniden çeker.
    Dönüş: satır silindiyse True."""
    try:
        if last_valid_row_idx < 2: return False
        bist_idx = [i for i, ad in enumerate(headers) if i > 0 and ad and ad not in YFINANCE_SUTUNLAR]
        if not bist_idx: return False

        ilk = max(2, last_valid_row_idx - max_satir + 1)
        bitis_hucre = rowcol_to_a1(last_valid_row_idx, len(headers))
        blok = sheet.get(f"A{ilk}:{bitis_hucre}", value_render_option='UNFORMATTED_VALUE')

        silinecek_ilk = None
        for r in range(last_valid_row_idx, ilk - 1, -1):
            k = r - ilk
            satir = blok[k] if k < len(blok) else []
            fiyat_var = False
            for i in bist_idx:
                if i < len(satir) and saklanan_sayi(satir[i]) is not None:
                    fiyat_var = True
                    break
            if fiyat_var: break
            silinecek_ilk = r

        if silinecek_ilk is None: return False

        print(f"[TEMİZLİK] Fiyatsız son satırlar siliniyor: {silinecek_ilk}-{last_valid_row_idx} (yeniden çekilecek)", flush=True)
        sheet.delete_rows(silinecek_ilk, last_valid_row_idx)
        return True
    except Exception as e:
        print(f"[!] Boş satır temizliği atlandı: {e}", flush=True)
        return False

def _guncelleme_calistir():
    """Günlük güncelleme mantığı. Dönüş: {'durum', 'tarih', 'doluluk', 'ayrinti'} (kayıt sayfası için)."""
    sonuc = {'durum': 'HATA', 'tarih': '-', 'doluluk': '-', 'ayrinti': ''}
    notlar = []
    isy_hata = False
    hafta_sonu = False   # Son tarih ile bugün arasında hiç hafta içi gün yoksa True olur

    def veri_yok():
        if isy_hata:
            sonuc['durum'] = 'HATA'
            sonuc['ayrinti'] = f"İş Yatırım yanıt vermedi ({_HATA_SAYACI['isyatirim']} istek alınamadı)"
        elif hafta_sonu:
            sonuc['durum'] = 'YENİ VERİ YOK'
            sonuc['ayrinti'] = " | ".join(["Hafta sonu: yeni işlem günü yok"] + notlar)
        else:
            sonuc['durum'] = 'VERİ YOK'
            sonuc['ayrinti'] = " | ".join(["Kapanış verisi henüz yayınlanmamış"] + notlar)
        return sonuc

    try:
        with _HATA_KILIDI:
            _HATA_SAYACI['isyatirim'] = 0

        sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
        if not last_date_str:
            sonuc['ayrinti'] = "Tabloda geçerli tarih bulunamadı"
            return sonuc

        # --- SONDAKİ FİYATSIZ SATIRLARI TEMİZLE ---
        if bos_son_satirlari_temizle(sheet, headers, last_valid_row_idx):
            notlar.append("Fiyatsız son satırlar temizlendi")
            time.sleep(2)
            sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
            if not last_date_str:
                sonuc['ayrinti'] = "Tabloda geçerli tarih bulunamadı"
                return sonuc
        # ------------------------------------------

        # --- GÜN İÇİ SIFIRLAMA MANTIĞI ---
        last_date = datetime.datetime.strptime(last_date_str, "%d.%m.%Y")
        today = datetime.datetime.now()
        yenileme = False

        if last_date.date() == today.date():
            sheet.delete_rows(last_valid_row_idx) # Bugünün eksik verisini içeren son satırı sil
            yenileme = True
            time.sleep(2)
            # Satır silindiği için veritabanı bilgilerini tazeleyerek yeniden alıyoruz
            sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
            if not last_date_str:
                sonuc['ayrinti'] = "Tabloda geçerli tarih bulunamadı"
                return sonuc
            last_date = datetime.datetime.strptime(last_date_str, "%d.%m.%Y")
        # -----------------------------------------

        yeni_hisseler = []
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
        if last_date.date() >= today.date() and not yf_gap_var:
            sonuc['durum'] = 'YENİ VERİ YOK'
            sonuc['ayrinti'] = " | ".join(["Tablo zaten güncel"] + notlar)
            return sonuc

        start_fetch = (last_date + datetime.timedelta(days=1)).strftime("%d-%m-%Y")
        end_fetch = today.strftime("%d-%m-%Y")
        bist_pivot = pd.DataFrame()

        # Son tarih ile bugün arasında hiç hafta içi gün yoksa (hafta sonu), İş Yatırım'a boşuna sorgu atılmaz
        islem_gunu_var = False
        kontrol_gunu = last_date + datetime.timedelta(days=1)
        while kontrol_gunu.date() <= today.date():
            if kontrol_gunu.weekday() < 5:
                islem_gunu_var = True
                break
            kontrol_gunu += datetime.timedelta(days=1)
        if last_date.date() < today.date() and not islem_gunu_var:
            hafta_sonu = True

        if last_date.date() < today.date() and islem_gunu_var:
            stocks = [h for h in headers if h not in (['Tarih'] + YFINANCE_SUTUNLAR) and h]
            all_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_data_for_date_range, stock, start_fetch, end_fetch, 5): stock for stock in stocks}
                for future in concurrent.futures.as_completed(futures):
                    df = future.result()
                    if not df.empty: all_data.append(df)
            if not all_data and stocks and _HATA_SAYACI['isyatirim'] >= max(1, len(stocks) // 2):
                isy_hata = True
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
                if gap_updates:
                    sheet.update_cells(gap_updates, value_input_option='USER_ENTERED')
                    notlar.append(f"Kur/altın/endeks boşluğu dolduruldu ({len(gap_updates)} hücre)")
            yf_new_df = yfinance_kuru[yfinance_kuru.index > hedef_tarih]
        else: yf_new_df = pd.DataFrame()

        final_df = bist_pivot.join(yf_new_df, how='outer') if not bist_pivot.empty or not yf_new_df.empty else pd.DataFrame()
        if final_df.empty: return veri_yok()
        final_df = final_df.sort_index()[final_df.index > hedef_tarih]
        if final_df.empty: return veri_yok()

        final_df.index = final_df.index.strftime('%d.%m.%Y')
        final_df.index.name = 'Tarih'
        final_df.reset_index(inplace=True)

        for col in headers:
            if col not in final_df.columns: final_df[col] = ""
        final_df = final_df[headers]
        for col in final_df.columns:
            if col != 'Tarih': final_df[col] = final_df[col].apply(format_fiyat)
        final_df = final_df.fillna("")

        # --- HİÇBİR HİSSE FİYATI OLMAYAN SATIRLARI YAZMA ---
        # Sadece tarih (ya da yalnızca kur/altın) içeren satır yazılırsa o gün "işlendi" sanılır ve hisse fiyatları bir daha çekilmez.
        bist_kolonlar = list(dict.fromkeys([c for c in headers if c and c != 'Tarih' and c not in YFINANCE_SUTUNLAR]))
        if bist_kolonlar:
            dolu_mask = (final_df[bist_kolonlar] != "").any(axis=1)
            atilan = final_df.loc[~dolu_mask, 'Tarih'].tolist()
            if atilan:
                print(f"[ATLANDI] Hisse fiyatı henüz yayınlanmamış, yazılmayan tarihler: {', '.join(atilan)} (sonraki çalışmada tekrar denenecek)", flush=True)
                notlar.append(f"Fiyatı yayınlanmayan tarihler yazılmadı: {', '.join(atilan)}")
            final_df = final_df[dolu_mask]
        if final_df.empty: return veri_yok()
        # -----------------------------------------------------

        yeni_satirlar = final_df.values.tolist()
        if yeni_satirlar:
            start_row = last_valid_row_idx + 1
            if sheet.row_count < start_row + len(yeni_satirlar): sheet.add_rows((start_row + len(yeni_satirlar)) - sheet.row_count + 50)
            sheet.update(values=yeni_satirlar, range_name=f"A{start_row}", value_input_option='USER_ENTERED')

            toplam_hisse = len(bist_kolonlar)
            dolu_adet = int((final_df.iloc[-1][bist_kolonlar] != "").sum()) if bist_kolonlar else 0
            tr_saat = datetime.datetime.now(TR_SAAT_DILIMI).hour
            if yenileme:
                ana = "ABD kapanışı / gün içi kesinleştirme turu" if tr_saat < 6 else "Gün içi satır yeniden yazıldı"
            else:
                ana = "Yeni kapanış verisi eklendi"
            ayr = [ana]
            if yeni_hisseler: ayr.append(f"Yeni halka arz ({len(yeni_hisseler)}): {_ad_ozeti(yeni_hisseler, 5)}")
            ayr.extend(notlar)

            sonuc['durum'] = 'YENİLENDİ' if yenileme else 'YAZILDI'
            sonuc['tarih'] = _tarih_ozeti(final_df['Tarih'].tolist())
            sonuc['doluluk'] = f"{dolu_adet} / {toplam_hisse}"
            sonuc['ayrinti'] = " | ".join(ayr)
        return sonuc

    except Exception as e:
        print(traceback.format_exc(), flush=True)
        sonuc['durum'] = 'HATA'
        sonuc['ayrinti'] = f"{type(e).__name__}: {str(e)[:200]}"
        return sonuc

def guncelleme_motoru():
    baslangic = time.time()
    sonuc = _guncelleme_calistir()
    _sonucu_kaydet("GÜNCELLE", sonuc, baslangic)

# ======================================================================
# HIZLI KONTROL (YOKLAMA) MOTORU
# Kullanım: python main.py kontrol
# Yeni günün kapanış verisi İş Yatırım'da yayınlandıysa normal 'guncelle' motorunu çalıştırır.
# Yayınlanmadıysa (ya da tabloda o günün satırı zaten varsa) hiçbir şey yazmadan çıkar ve kayıt da bırakmaz.
# Bu sayede gece boyunca sık aralıklarla çalıştırılabilir; veri yayınlanır yayınlanmaz tabloya girer.
# ======================================================================

def yeni_bist_verisi_var_mi(son_tarih, bugun, esik_orani=BIST_YOKLAMA_ESIK_ORANI):
    """Yoklama hisselerinin yeterli bölümünde, tablodaki son tarihten SONRAKİ bir güne ait geçerli kapanış var mı?"""
    bas_str = (son_tarih + datetime.timedelta(days=1)).strftime('%d-%m-%Y')
    bit_str = bugun.strftime('%d-%m-%Y')
    dolu = 0
    for hisse in BIST_YOKLAMA_HISSELERI:
        df = fetch_data_for_date_range(hisse, bas_str, bit_str, 2)
        if df.empty: continue
        kap = pd.to_numeric(df['Kapanis'], errors='coerce')
        gecerli = df[df['Tarih'].notna() & (kap > 0)]
        if not gecerli.empty and (gecerli['Tarih'].dt.date > son_tarih).any():
            dolu += 1
    gerekli = max(1, int(len(BIST_YOKLAMA_HISSELERI) * esik_orani))
    print(f"[KONTROL] Yoklama: {dolu}/{len(BIST_YOKLAMA_HISSELERI)} hissede yeni gün verisi var (gereken: {gerekli})", flush=True)
    return dolu >= gerekli

def hizli_kontrol_motoru():
    baslangic = time.time()
    try:
        sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
        if not last_date_str:
            print("[KONTROL] Tabloda geçerli tarih bulunamadı.", flush=True)
            return

        # Sondaki fiyatsız satırlar varsa önce temizle (günlük motorla aynı mantık)
        if bos_son_satirlari_temizle(sheet, headers, last_valid_row_idx):
            time.sleep(2)
            sheet, headers, tarih_to_row, last_valid_row_idx, last_date_str = baglanti_ve_bilgi_getir()
            if not last_date_str: return

        son_tarih = datetime.datetime.strptime(last_date_str, "%d.%m.%Y").date()
        bugun = datetime.datetime.now().date()

        if son_tarih >= bugun:
            print(f"[KONTROL] Tabloda {last_date_str} satırı zaten var, yapılacak bir şey yok.", flush=True)
            return

        if not yeni_bist_verisi_var_mi(son_tarih, bugun):
            print(f"[KONTROL] {last_date_str} sonrası için kapanış verisi henüz yayınlanmamış, sonraki yoklamada tekrar bakılacak.", flush=True)
            return

        print("[KONTROL] Yeni kapanış verisi yayınlanmış, güncelleme motoru başlatılıyor.", flush=True)
        sonuc = _guncelleme_calistir()
        _sonucu_kaydet("KONTROL", sonuc, baslangic)
    except Exception as e:
        print(traceback.format_exc(), flush=True)
        kayit_yaz("KONTROL", "HATA", "-", "-", time.time() - baslangic, f"{type(e).__name__}: {str(e)[:200]}")

# ======================================================================
# BAKIM (EKSİK TAMAMLAMA) MOTORU
# Son 'taranacak_gun' satırdaki işlem günlerini tarar:
#   1) Hisse sütunlarındaki boş hücreleri İş Yatırım'dan doldurmaya çalışır.
#   2) DOLAR / EURO / GR ALTIN / GR GÜMÜŞ / NASDAQ / S&P 500 sütunlarında boş hücre var mı kontrol eder (raporlar).
# Sonucu, hangi hücrelerin dolmadığı DAHİL, kayıt sayfasına yazar (web uygulamasında "EKSİK TAMAMLAMA" satırı).
# ======================================================================

def bakim_motoru(taranacak_gun=15):
    baslangic = time.time()
    try:
        with _HATA_KILIDI:
            _HATA_SAYACI['isyatirim'] = 0

        sheet, headers, tarih_to_row, last_valid_row_idx, _ = baglanti_ve_bilgi_getir()
        baslangic_satiri = max(2, last_valid_row_idx - taranacak_gun)
        son_satirlar = sheet.get_all_values()[baslangic_satiri - 1:last_valid_row_idx]

        def hucre(satir, idx):
            return satir[idx].strip() if idx < len(satir) else ""

        # Taranacak işlem günleri (yalnızca hafta içi, tarihi geçerli satırlar)
        gun_satirlari = []   # (tarih_str, tarih_dt, satir_no, satir_verisi)
        for satir_verisi in son_satirlar:
            satir_tarihi_str = hucre(satir_verisi, 0)
            if not re.match(r'\d{2}\.\d{2}\.\d{4}', satir_tarihi_str): continue
            satir_dt = datetime.datetime.strptime(satir_tarihi_str[:10], "%d.%m.%Y")
            if satir_dt.weekday() >= 5: continue
            gercek_satir_no = tarih_to_row.get(satir_tarihi_str)
            if gercek_satir_no is None: continue
            gun_satirlari.append((satir_tarihi_str, satir_dt, gercek_satir_no, satir_verisi))

        gun_adedi = len(gun_satirlari)
        ilk_gun = gun_satirlari[0][0] if gun_satirlari else "-"
        son_gun = gun_satirlari[-1][0] if gun_satirlari else "-"

        hisse_kolonlari = [(i, headers[i]) for i in range(1, len(headers)) if headers[i] and headers[i] not in YFINANCE_SUTUNLAR]

        # --- 1) KUR / ALTIN / GÜMÜŞ / ENDEKS SÜTUNLARI: boş hücre var mı? ---
        kur_eksik = {}   # sütun adı -> [boş günler]
        for ad in YFINANCE_SUTUNLAR:
            if ad not in headers: continue
            idx = headers.index(ad)
            bos_gunler = [t for t, _, _, s in gun_satirlari if hucre(s, idx) == ""]
            if bos_gunler: kur_eksik[ad] = bos_gunler

        # --- 2) HİSSE SÜTUNLARI: boş hücreleri doldurmayı dene ---
        eksik_guncellemeler = []
        eksik_toplam = 0
        eksik_tarihleri = set()
        doldurulan_tarihleri = set()
        doldurulan_adlar = []
        hisse_dolu_gun = {}      # hisse -> doldurmadan önce fiyatı olan gün sayısı
        hisse_doldurulan = {}    # hisse -> bu çalışmada doldurulan hücre sayısı
        hisse_kalan = {}         # hisse -> doldurulamayan günler

        for t_str, satir_dt, gercek_satir_no, satir_verisi in gun_satirlari:
            tarih_api = satir_dt.strftime('%d-%m-%Y')
            for col_idx, hisse_adi in hisse_kolonlari:
                if hucre(satir_verisi, col_idx) != "":
                    hisse_dolu_gun[hisse_adi] = hisse_dolu_gun.get(hisse_adi, 0) + 1
                    continue
                eksik_toplam += 1
                eksik_tarihleri.add(t_str)
                yfiyat = ""
                kurtarma_df = fetch_data_for_date_range(hisse_adi, tarih_api, tarih_api)
                if not kurtarma_df.empty:
                    yfiyat = format_fiyat(kurtarma_df.iloc[0]['Kapanis'])
                if yfiyat != "":
                    eksik_guncellemeler.append(gspread.Cell(row=gercek_satir_no, col=col_idx + 1, value=yfiyat))
                    doldurulan_tarihleri.add(t_str)
                    hisse_doldurulan[hisse_adi] = hisse_doldurulan.get(hisse_adi, 0) + 1
                    if hisse_adi not in doldurulan_adlar: doldurulan_adlar.append(hisse_adi)
                else:
                    hisse_kalan.setdefault(hisse_adi, []).append(t_str)

        if eksik_guncellemeler: sheet.update_cells(eksik_guncellemeler, value_input_option='USER_ENTERED')

        doldurulan = len(eksik_guncellemeler)
        kalan = eksik_toplam - doldurulan

        # Doldurulamayanları ikiye ayır:
        #   tamamen_bos : taranan günlerin hiçbirinde fiyatı olmayan hisse
        #   kismi       : başka günlerde fiyatı var ama bazı günleri doldurulamayan hisse (asıl dikkat edilecekler)
        tamamen_bos = []
        kismi = {}
        for hisse_adi, gunler in hisse_kalan.items():
            if hisse_dolu_gun.get(hisse_adi, 0) == 0 and hisse_doldurulan.get(hisse_adi, 0) == 0:
                tamamen_bos.append(hisse_adi)
            else:
                kismi[hisse_adi] = gunler
        tamamen_bos.sort()
        kismi_sirali = sorted(kismi.items(), key=lambda kv: (-len(kv[1]), kv[0]))

        def kisa_gun(t):
            return t[:5]   # gg.aa

        with _HATA_KILIDI:
            ag_hatasi = _HATA_SAYACI['isyatirim']

        rapor = {
            "n": gun_adedi, "d1": ilk_gun, "d2": son_gun, "hs": len(hisse_kolonlari),
            "top": eksik_toplam, "ok": doldurulan,
            "ka": {ad: [len(ts), [kisa_gun(t) for t in ts[:BAKIM_RAPOR_TARIH_MAKS]]] for ad, ts in kur_eksik.items()},
            "bosN": len(tamamen_bos), "bos": tamamen_bos[:BAKIM_RAPOR_BOS_HISSE_MAKS],
            "kisN": len(kismi_sirali),
            "kis": {h: [len(ts), [kisa_gun(t) for t in ts[:BAKIM_RAPOR_TARIH_MAKS]]] for h, ts in kismi_sirali[:BAKIM_RAPOR_KISMI_HISSE_MAKS]},
            "ag": ag_hatasi
        }

        if eksik_toplam == 0:
            durum = "TEMİZ"
            tarih_metni = f"{ilk_gun} – {son_gun}" if gun_adedi else f"Son {taranacak_gun} gün"
            doluluk = "0 eksik"
            ayrinti = f"Son {taranacak_gun} günde eksik BIST hücresi bulunmadı"
        elif kalan == 0:
            durum = "ONARILDI"
            tarih_metni = _tarih_ozeti(_tarihleri_sirala(doldurulan_tarihleri))
            doluluk = f"+{doldurulan} hücre"
            ayrinti = f"Eksik {doldurulan} BIST hücresi tamamlandı ({_ad_ozeti(doldurulan_adlar)})"
        elif doldurulan > 0:
            durum = "KISMEN"
            tarih_metni = _tarih_ozeti(_tarihleri_sirala(eksik_tarihleri))
            doluluk = f"+{doldurulan} / {eksik_toplam}"
            ayrinti = f"{doldurulan} hücre tamamlandı ({_ad_ozeti(doldurulan_adlar)}), {kalan} hücre İş Yatırım'da bulunamadı"
        else:
            durum = "KALAN EKSİK"
            tarih_metni = _tarih_ozeti(_tarihleri_sirala(eksik_tarihleri))
            doluluk = f"{eksik_toplam} eksik"
            ayrinti = f"{eksik_toplam} hücre İş Yatırım'da bulunamadı (işlem görmeyen hisseler olabilir)"

        # Kur/altın/endeks sütunlarında boş hücre varsa bu, hisse eksiğinden daha önemlidir; durum buna göre işaretlenir
        if kur_eksik:
            durum = "KUR EKSİK"
            ayrinti = f"Kur/altın/endeks sütunlarında boş hücre var: {_ad_ozeti(kur_eksik.keys(), 6)} | " + ayrinti

        kayit_yaz("BAKIM", durum, tarih_metni, doluluk, time.time() - baslangic, _json_ekle(ayrinti, rapor))
    except Exception as e:
        print(traceback.format_exc(), flush=True)
        kayit_yaz("BAKIM", "HATA", "-", "-", time.time() - baslangic, f"{type(e).__name__}: {str(e)[:200]}")


# ======================================================================
# HAFTALIK GEÇMİŞ YENİLEME MOTORU
# ======================================================================

def hassas_yuvarla(val):
    # Geçmiş kurumsal işlemlerin (bölünme vb.) getiriye etkisini 
    # hassas biçimde yansıtmak için yuvarlama yapılmadan döndürülür.
    return float(val)

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

def _tarih_coz(satir):
    try:
        return datetime.datetime.strptime(str(satir.get('HGDG_TARIH'))[:10], '%d-%m-%Y').date()
    except Exception:
        return None

def _pozitif_float(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None

def seri_cikar(satirlar, alan):
    """İş Yatırım satırlarından {tarih: değer} sözlüğü çıkarır (yalnızca pozitif değerler)."""
    seri = {}
    for s in satirlar:
        d = _tarih_coz(s)
        if d is None: continue
        v = _pozitif_float(s.get(alan))
        if v is not None: seri[d] = v
    return seri

def sermaye_duzeltmeli_seri(satirlar):
    """Ham kapanış x (o günkü sermaye / en güncel sermaye). SERMAYE ya da ham veri yoksa None döner."""
    ham = seri_cikar(satirlar, 'HG_KAPANIS')
    ser = seri_cikar(satirlar, 'SERMAYE')
    if not ham or not ser: return None
    ser_gunler = sorted(ser)
    ham_gunler = sorted(ham)

    def sermaye_o_gun(d):
        k = bisect.bisect_right(ser_gunler, d) - 1
        if k < 0: k = 0
        return ser[ser_gunler[k]]

    ref = sermaye_o_gun(ham_gunler[-1])
    if ref <= 0: return None
    return {d: ham[d] * sermaye_o_gun(d) / ref for d in ham_gunler}

def hibrit_olay_analizi(satirlar):
    """İş Yatırım'ın fiyat düzeltme olaylarını SERMAYE değişimleriyle eşleştirir.
    Çarpan = olay gününden ÖNCEKİ fiyatlara uygulanan düzeltme çarpanıdır (0,5 = eski fiyatlar yarıya indirilmiş).
    Bir fiyat olayının çarpanı, yakın tarihli bir sermaye artışının (1 / sermaye oranı) değerine yeterince yakınsa
    olay BEDELSİZ sayılır ve kesin sermaye oranı kullanılır. Yakın değilse (örn. bedelli) olay DIGER sayılır.
    Dönüş: (duz, ham, ser, eslesmeler) ya da veri eksikse None.
    eslesmeler: [(tarih, is_carpani, sermaye_carpani_veya_None, sinif)]"""
    duz = seri_cikar(satirlar, FIYAT_ALANI)
    ham = seri_cikar(satirlar, ALT_FIYAT_ALANI)
    ser = seri_cikar(satirlar, 'SERMAYE')
    if not duz or not ham or not ser: return None

    fiyat_olaylari = _olaylari_bul(duz, ham)
    sermaye_olaylari = _sermaye_olaylari(ser)
    kullanilan = set()
    eslesmeler = []

    for d, onceki, f_eski, f_yeni, oran in fiyat_olaylari:
        c_is = 1.0 / oran
        gurultu = 0.005 / ham[d] + 0.005 / ham[onceki]
        tol = min(0.10, max(0.01, 3.0 * gurultu))
        en_iyi = None
        for k, (sd, s_eski, s_yeni, s_oran) in enumerate(sermaye_olaylari):
            if k in kullanilan: continue
            if abs((sd - d).days) > HIBRIT_PENCERE_GUN: continue
            c_s = s_eski / s_yeni
            sapma = abs(c_is / c_s - 1.0)
            if sapma <= tol and (en_iyi is None or sapma < en_iyi[0]):
                en_iyi = (sapma, k, c_s)
        if en_iyi is not None:
            kullanilan.add(en_iyi[1])
            eslesmeler.append((d, c_is, en_iyi[2], 'BEDELSIZ'))
        else:
            eslesmeler.append((d, c_is, None, 'DIGER'))

    return duz, ham, ser, eslesmeler

def hibrit_duzeltmeli_seri(satirlar):
    """İş Yatırım düzeltilmiş serisi + bedelsiz olaylarında kesin oran düzeltmesi. Veri eksikse None."""
    sonuc = hibrit_olay_analizi(satirlar)
    if sonuc is None: return None
    duz, ham, ser, eslesmeler = sonuc
    duzeltmeler = sorted((d, c_s / c_is) for d, c_is, c_s, sinif in eslesmeler if sinif == 'BEDELSIZ')
    if not duzeltmeler: return dict(duz)

    gunler = sorted(duz)
    seri = {}
    idx = len(duzeltmeler) - 1
    kum = 1.0
    for g in reversed(gunler):
        # Olay gününden ÖNCEKİ günler etkilenir: tarihi g'den büyük tüm olayların düzeltmesi uygulanır
        while idx >= 0 and duzeltmeler[idx][0] > g:
            kum *= duzeltmeler[idx][1]
            idx -= 1
        seri[g] = duz[g] * kum
    return seri

def haftalik_hisse_satirlari(stock, parcalar):
    """Tüm parçaları birleştirir. Herhangi bir parça alınamazsa None döner."""
    tum = []
    for bas_str, bit_str in parcalar:
        satirlar, ok = haftalik_parca_cek(stock, bas_str, bit_str)
        if not ok: return None
        tum.extend(satirlar)
    return tum

def haftalik_hisse_gecmisi(stock, parcalar):
    """Dönüş: (fiyat_sozlugu, alt_alan_sozlugu) ya da herhangi bir parça alınamazsa (None, None)."""
    satirlar = haftalik_hisse_satirlari(stock, parcalar)
    if satirlar is None: return None, None
    fiyat = seri_cikar(satirlar, FIYAT_ALANI)
    alt = seri_cikar(satirlar, ALT_FIYAT_ALANI)
    if FIYAT_YONTEMI == "SERMAYE":
        sdz = sermaye_duzeltmeli_seri(satirlar)
        if sdz: fiyat = sdz
        else: _YONTEM_GERI_DONUS.append(stock)
    elif FIYAT_YONTEMI == "HIBRIT":
        hdz = hibrit_duzeltmeli_seri(satirlar)
        if hdz: fiyat = hdz
        else: _YONTEM_GERI_DONUS.append(stock)
    return fiyat, alt

def _tolerans(x):
    return max(HAFTALIK_TOLERANS_MUTLAK, HAFTALIK_TOLERANS_ORAN * abs(x))

def _tolerans_hassas(x):
    return max(HAFTALIK_TOLERANS_HASSAS_MUTLAK, HAFTALIK_TOLERANS_HASSAS_ORAN * abs(x))

def haftalik_karsilastir(tarihler, eski, ham, yeni):
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

    farkli = [i for i in ortak if abs(eski[i] - yeni[tarihler[i]]) > _tolerans_hassas(yeni[tarihler[i]])]
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
            kayit_yaz("HAFTALIK", "HATA", "-", "-", time.time() - baslangic, "Tabloda geçerli tarih bulunamadı")
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

        print(f"[HAFTALIK] Fiyat yöntemi: {FIYAT_YONTEMI}", flush=True)
        print(f"[HAFTALIK] {len(hisseler)} hisse, {n} satır, {ilk_tarih} → {bugun}, {len(parcalar)} parça/hisse", flush=True)

        zaman = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        sayac = {'degismedi': 0, 'guncelle': 0, 'atla': 0, 'cekilemedi': 0}
        gunluk = []
        guncellenen_adlar = []
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
                    guncellenen_adlar.append(ad)
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
        print(f"[HAFTALIK ÖZET] Yöntem: {FIYAT_YONTEMI} | Değişmeyen: {sayac['degismedi']} | Güncellenen: {sayac['guncelle']} | Atlanan: {sayac['atla']} | Alınamayan: {sayac['cekilemedi']} | Süre: {sure // 60} dk {sure % 60} sn", flush=True)
        if _YONTEM_GERI_DONUS:
            print(f"[UYARI] {len(_YONTEM_GERI_DONUS)} hissede {FIYAT_YONTEMI} yöntemi uygulanamadı (SERMAYE verisi yok), İş Yatırım serisi kullanıldı. Örnek: {', '.join(sorted(set(_YONTEM_GERI_DONUS))[:10])}", flush=True)
        if tavan_asildi:
            print(f"[UYARI] Güvenlik tavanı ({HAFTALIK_MAX_YAZILACAK_HISSE}) aşıldı, kalan hisseler gelecek haftaya ertelendi (haftalik motoru elle tekrar çalıştırarak hemen tamamlayabilirsiniz).", flush=True)
        if teshis_adet:
            print(f"[ALAN TEŞHİSİ] {teshis_adet} hissede {FIYAT_ALANI} ile {ALT_FIYAT_ALANI} farklı. Örnekler:", flush=True)
            for o in teshis_ornek: print("   " + o, flush=True)
        else:
            print(f"[ALAN TEŞHİSİ] {FIYAT_ALANI} ile {ALT_FIYAT_ALANI} hiçbir hissede farklı değil.", flush=True)
        print("=" * 60, flush=True)

        # --- GÜNCELLEME KAYDI ---
        toplam_hisse = len(hisseler)
        if toplam_hisse and sayac['cekilemedi'] >= max(1, toplam_hisse // 2):
            h_durum = "HATA"
            h_ayrinti = f"İş Yatırım'dan {sayac['cekilemedi']} hissenin verisi alınamadı"
        elif sayac['guncelle'] > 0:
            h_durum = "GÜNCELLENDİ"
            h_ayrinti = f"{sayac['guncelle']} sütun yenilendi ({_ad_ozeti(guncellenen_adlar)})"
        else:
            h_durum = "DEĞİŞMEDİ"
            h_ayrinti = "Geçmiş fiyatlarda değişiklik yok"
        ekler = []
        if sayac['atla']: ekler.append(f"{sayac['atla']} hisse atlandı")
        if sayac['cekilemedi'] and h_durum != "HATA": ekler.append(f"{sayac['cekilemedi']} hisse alınamadı")
        if tavan_asildi: ekler.append("güvenlik tavanı aşıldı, kalanlar gelecek haftaya ertelendi")
        if _YONTEM_GERI_DONUS: ekler.append(f"{len(set(_YONTEM_GERI_DONUS))} hissede {FIYAT_YONTEMI} yerine İş Yatırım serisi kullanıldı")
        if ekler: h_ayrinti += " | " + ", ".join(ekler)
        kayit_yaz("HAFTALIK", h_durum, f"{ilk_tarih.year}–{bugun.year}", f"{sayac['guncelle']} hisse", time.time() - baslangic, h_ayrinti)
        kayit_budama()

    except Exception as e:
        print(traceback.format_exc(), flush=True)
        kayit_yaz("HAFTALIK", "HATA", "-", "-", time.time() - baslangic, f"{type(e).__name__}: {str(e)[:200]}")
        sys.exit(1)


# ======================================================================
# TEŞHİS VE ANOMALİ ARAÇLARI
# ======================================================================

ANOMALI_ALT = 0.89                 # Önceki güne göre bu orandan düşük = şüpheli
ANOMALI_UST = 1.12                 # Önceki güne göre bu orandan yüksek = şüpheli
ANOMALI_MAX_GUN_FARKI = 7          # İki fiyat arası en fazla bu kadar gün ise karşılaştırılır
ANOMALI_MAX_SATIR = 6000           # Rapor sayfasına yazılacak en fazla satır
ANOMALI_SAYFA_ADI = "Anomali Raporu"
ANOMALI_DUSUK_FIYAT = 0.05         # Her iki fiyattan biri bunun altındaysa "DÜŞÜK FİYAT" sınıfına alınır
ANOMALI_YIL_SINIRI = 20            # Uygulamadaki en uzun getiri sütunu 20 yıllık; rapor bu kadar yıla odaklanır

def _gun_coz(metin):
    return datetime.datetime.strptime(str(metin).strip(), "%d-%m-%Y").date()

def _seri_getir(satirlar, alan):
    seri = {}
    for s in satirlar:
        try:
            d = datetime.datetime.strptime(str(s.get('HGDG_TARIH'))[:10], '%d-%m-%Y').date()
            v = float(s.get(alan))
        except (TypeError, ValueError):
            continue
        if v > 0: seri[d] = v
    return seri

def _o_gun_veya_oncesi(seri, d):
    adaylar = [x for x in seri if x <= d]
    if not adaylar: return None
    k = max(adaylar)
    return k, seri[k]

def _kesin_oncesi(seri, d):
    adaylar = [x for x in seri if x < d]
    if not adaylar: return None
    k = max(adaylar)
    return k, seri[k]

def _getiri_satiri(etiket, baz, son):
    if baz is None or son is None or baz[1] <= 0:
        return f"  {etiket:<62}: hesaplanamadı"
    g = (son[1] / baz[1] - 1) * 100
    return f"  {etiket:<62}: %{g:,.2f}   [{baz[0].strftime('%d.%m.%Y')} {baz[1]:.4f} -> {son[0].strftime('%d.%m.%Y')} {son[1]:.4f}]"

def _fmt(v):
    return f"{v:.4f}" if v is not None else "-"

def _olaylari_bul(duz, ham):
    ortak = sorted(d for d in duz if d in ham)
    olaylar = []
    onceki = None
    for d in ortak:
        if onceki is not None:
            f_yeni = duz[d] / ham[d]
            f_eski = duz[onceki] / ham[onceki]
            ruido = 0.005 / ham[d] + 0.005 / ham[onceki]
            esik = max(0.003, 2.0 * ruido)
            oran = f_yeni / f_eski
            if abs(oran - 1) > esik:
                olaylar.append((d, onceki, f_eski, f_yeni, oran))
        onceki = d
    return olaylar

def _hisse_listesi_coz(metin):
    """'AKSA:2904.75,FROTO' -> [('AKSA', 2904.75), ('FROTO', None)]"""
    sonuc = []
    for parca in str(metin).replace(";", ",").split(","):
        parca = parca.strip()
        if not parca: continue
        kod, _, ref = parca.partition(":")
        kod = kod.strip().upper()
        referans = None
        if ref.strip():
            try:
                referans = float(ref.strip().replace("%", "").replace(",", "."))
            except ValueError:
                referans = None
        if kod: sonuc.append((kod, referans))
    return sonuc

def teshis_motoru(hisse_metni, bas_str, bit_str):
    try:
        bas = _gun_coz(bas_str)
        bit = _gun_coz(bit_str)
        hisseler = [kod for kod, _ in _hisse_listesi_coz(hisse_metni)]
        sheet, headers, tarih_to_row, son_satir, son_tarih_str = baglanti_ve_bilgi_getir()
        satir_tarihi = {row: datetime.datetime.strptime(t, "%d.%m.%Y").date() for t, row in tarih_to_row.items()}
        bugun = datetime.datetime.now().date()

        print(f"[TEŞHİS] Tablodaki son tarih: {son_tarih_str} | Fiyat alanı: {FIYAT_ALANI} (düzeltilmiş) / {ALT_FIYAT_ALANI} (ham) | Yöntem: {FIYAT_YONTEMI}", flush=True)

        for hisse in hisseler:
            print("=" * 100, flush=True)
            print(f"{hisse} | {bas.strftime('%d.%m.%Y')} -> {bit.strftime('%d.%m.%Y')}", flush=True)
            if hisse not in headers:
                print("  Tabloda bu başlık bulunamadı.", flush=True)
                continue

            col = headers.index(hisse) + 1
            blok = sheet.get(f"{rowcol_to_a1(2, col)}:{rowcol_to_a1(son_satir, col)}", value_render_option='UNFORMATTED_VALUE')
            tablo = {}
            for r in range(2, son_satir + 1):
                d = satir_tarihi.get(r)
                if d is None: continue
                satir = blok[r - 2] if (r - 2) < len(blok) else []
                v = saklanan_sayi(satir[0]) if satir else None
                if v is not None: tablo[d] = v

            ilk_t = min(tablo) if tablo else datetime.date(2000, 1, 4)
            parcalar = haftalik_parcalar(min(ilk_t, bas), max(bit, bugun))
            duz, ham = haftalik_hisse_gecmisi(hisse, parcalar)
            if duz is None:
                print("  İş Yatırım'dan veri alınamadı.", flush=True)
                continue

            def pencere(merkez_bas, merkez_bit):
                gunler = sorted(set(d for d in list(duz) + list(ham) + list(tablo) if merkez_bas <= d <= merkez_bit))
                print(f"  {'TARİH':<12}{'TABLO':>14}{'İŞ YAT. DÜZELTİLMİŞ':>22}{'İŞ YAT. HAM':>16}", flush=True)
                for d in gunler:
                    print(f"  {d.strftime('%d.%m.%Y'):<12}{_fmt(tablo.get(d)):>14}{_fmt(duz.get(d)):>22}{_fmt(ham.get(d)):>16}", flush=True)

            print("\n  --- BAŞLANGIÇ TARİHİ ÇEVRESİ ---", flush=True)
            pencere(bas - datetime.timedelta(days=8), bas + datetime.timedelta(days=4))
            print("\n  --- BİTİŞ TARİHİ ÇEVRESİ ---", flush=True)
            pencere(bit - datetime.timedelta(days=6), bit)

            print("\n  --- GETİRİ KARŞILAŞTIRMASI ---", flush=True)
            print("  (A) Başlangıç günü kapanışı baz alınır  |  (B) Başlangıçtan ÖNCEKİ son kapanış baz alınır", flush=True)
            for ad, seri in [("TABLO (uygulamanın kullandığı veri)", tablo),
                             ("SEÇİLİ YÖNTEM SERİSİ (" + FIYAT_YONTEMI + ")", duz),
                             ("İŞ YATIRIM HAM (" + ALT_FIYAT_ALANI + ")", ham)]:
                son = _o_gun_veya_oncesi(seri, bit)
                print(_getiri_satiri(ad + " (A)", _o_gun_veya_oncesi(seri, bas), son), flush=True)
                print(_getiri_satiri(ad + " (B)", _kesin_oncesi(seri, bas), son), flush=True)

            b_duz = _o_gun_veya_oncesi(duz, bas)
            b_ham = _o_gun_veya_oncesi(ham, bas)
            e_duz = _o_gun_veya_oncesi(duz, bit)
            e_ham = _o_gun_veya_oncesi(ham, bit)
            if b_duz and b_ham and e_duz and e_ham:
                print(f"\n  Düzeltme katsayısı (düzeltilmiş/ham): başlangıçta {b_duz[1] / b_ham[1]:.4f} | bitişte {e_duz[1] / e_ham[1]:.4f}", flush=True)

            kontrol = []
            for etiket, d in [("başlangıç", _o_gun_veya_oncesi(tablo, bas)), ("bitiş", _o_gun_veya_oncesi(tablo, bit))]:
                if d is None: continue
                karsilik = duz.get(d[0])
                if karsilik is None:
                    kontrol.append(f"{etiket} ({d[0].strftime('%d.%m.%Y')}): İş Yatırım'da bu gün yok")
                elif abs(d[1] - karsilik) > _tolerans(karsilik):
                    kontrol.append(f"{etiket} ({d[0].strftime('%d.%m.%Y')}): TABLO={d[1]} İŞ YATIRIM={karsilik}  -> FARKLI")
                else:
                    kontrol.append(f"{etiket} ({d[0].strftime('%d.%m.%Y')}): tablo = İş Yatırım ({karsilik})")
            print("  Tablo / İş Yatırım uyumu: " + " | ".join(kontrol), flush=True)

            olaylar = _olaylari_bul(duz, ham)
            print(f"\n  --- İŞ YATIRIM'IN UYGULADIĞI DÜZELTME OLAYLARI (toplam {len(olaylar)}) ---", flush=True)
            print("  'Çarpan': o tarihten ÖNCEKİ tüm fiyatlara uygulanan düzeltme çarpanı (0,75 = eski fiyatlar %25 düşürülmüş)", flush=True)
            print(f"  {'TARİH':<12}{'ÇARPAN':>10}{'KÜMÜLATİF KATSAYI (önce → sonra)':>36}{'HAM FİYAT (önceki gün → gün)':>34}", flush=True)
            toplam_carpan = 1.0
            for d, onceki, f_eski, f_yeni, oran in olaylar[-60:]:
                carpan = 1.0 / oran
                print(f"  {d.strftime('%d.%m.%Y'):<12}{carpan:>10.4f}{f'{f_eski:.4f} -> {f_yeni:.4f}':>36}{f'{ham[onceki]:.2f} -> {ham[d]:.2f}':>34}", flush=True)
            for d, onceki, f_eski, f_yeni, oran in olaylar:
                toplam_carpan *= 1.0 / oran
            if olaylar:
                print(f"  Tüm olayların çarpımı: {toplam_carpan:.4f}", flush=True)

        print("=" * 100, flush=True)
    except Exception:
        print(traceback.format_exc(), flush=True)
        sys.exit(1)


# ======================================================================
# FINNET KARŞILAŞTIRMA MOTORU
# Kullanım: python main.py finnet "AKSA:2904.75,FROTO:1587.29" 13-11-2019 13-11-2026
# 'KOD:değer' -> değer, Finnet'teki "% Değ. Kapanış" rakamıdır. Değer yazılmazsa yalnızca seriler yazdırılır.
# Getiri bazı Finnet ile aynıdır: başlangıç tarihinden ÖNCEKİ son kapanış.
# Aday yöntemler: İş Yatırım düzeltilmiş, sermaye düzeltmeli, hibrit. Finnet'e en yakın olan raporlanır.
# ======================================================================

def _getiri_hesapla(seri, bas, bit):
    if not seri: return None
    baz = _kesin_oncesi(seri, bas)
    son = _o_gun_veya_oncesi(seri, bit)
    if baz is None or son is None or baz[1] <= 0: return None
    return (son[1] / baz[1] - 1) * 100, baz, son

def _sermaye_olaylari(ser):
    gunler = sorted(ser)
    olaylar = []
    for onceki, d in zip(gunler, gunler[1:]):
        oran = ser[d] / ser[onceki]
        if abs(oran - 1) > 0.0001:
            olaylar.append((d, ser[onceki], ser[d], oran))
    return olaylar

def finnet_motoru(hisse_metni, bas_str, bit_str):
    try:
        bas = _gun_coz(bas_str)
        bit = _gun_coz(bit_str)
        hisseler = _hisse_listesi_coz(hisse_metni)
        sheet, headers, tarih_to_row, son_satir, son_tarih_str = baglanti_ve_bilgi_getir()
        satir_tarihi = {row: datetime.datetime.strptime(t, "%d.%m.%Y").date() for t, row in tarih_to_row.items()}
        bugun = datetime.datetime.now().date()

        print(f"[FINNET] Tablodaki son tarih: {son_tarih_str} | {bas.strftime('%d.%m.%Y')} -> {bit.strftime('%d.%m.%Y')} | Baz: başlangıçtan ÖNCEKİ son kapanış", flush=True)

        galibiyet = {}
        for hisse, referans in hisseler:
            print("=" * 110, flush=True)
            ref_metin = f"%{referans:,.2f}" if referans is not None else "verilmedi"
            print(f"{hisse} | Finnet referans getirisi: {ref_metin}", flush=True)
            if hisse not in headers:
                print("  Tabloda bu başlık bulunamadı.", flush=True)
                continue

            col = headers.index(hisse) + 1
            blok = sheet.get(f"{rowcol_to_a1(2, col)}:{rowcol_to_a1(son_satir, col)}", value_render_option='UNFORMATTED_VALUE')
            tablo = {}
            for r in range(2, son_satir + 1):
                d = satir_tarihi.get(r)
                if d is None: continue
                satir = blok[r - 2] if (r - 2) < len(blok) else []
                v = saklanan_sayi(satir[0]) if satir else None
                if v is not None: tablo[d] = v

            ilk_t = min(tablo) if tablo else datetime.date(2000, 1, 4)
            parcalar = haftalik_parcalar(min(ilk_t, bas), max(bit, bugun))
            satirlar = haftalik_hisse_satirlari(hisse, parcalar)
            if satirlar is None:
                print("  İş Yatırım'dan veri alınamadı.", flush=True)
                continue

            duz = seri_cikar(satirlar, FIYAT_ALANI)
            ham = seri_cikar(satirlar, ALT_FIYAT_ALANI)
            ser = seri_cikar(satirlar, 'SERMAYE')
            sdz = sermaye_duzeltmeli_seri(satirlar)
            hib = hibrit_olay_analizi(satirlar)
            hdz = hibrit_duzeltmeli_seri(satirlar)
            if not ser:
                print("  UYARI: İş Yatırım yanıtında SERMAYE alanı yok/boş; SERMAYE ve HİBRİT yöntemleri hesaplanamaz.", flush=True)

            # (kısa ad, uzun ad, seri, yöntem adayı mı)
            adaylar = [
                ("TABLO", "TABLO (uygulamanın kullandığı veri)", tablo, False),
                ("DÜZ", "İŞ YATIRIM DÜZELTİLMİŞ (" + FIYAT_ALANI + ")", duz, True),
                ("SER", "SERMAYE DÜZELTMELİ (ham x sermaye oranı)", sdz or {}, True),
                ("HİB", "HİBRİT (İş Yatırım + kesin bedelsiz oranı)", hdz or {}, True),
                ("HAM", "İŞ YATIRIM HAM (" + ALT_FIYAT_ALANI + ")", ham, False),
            ]

            print("\n  --- GETİRİ KARŞILAŞTIRMASI (baz = başlangıçtan önceki son kapanış) ---", flush=True)
            en_yakin = None
            for kisa, ad, seri, aday in adaylar:
                sonuc = _getiri_hesapla(seri, bas, bit)
                if sonuc is None:
                    print(f"  {ad:<52}: hesaplanamadı", flush=True)
                    continue
                g, baz, son = sonuc
                fark_metin = ""
                if referans is not None:
                    fark = g - referans
                    fark_metin = f"   Finnet farkı: {fark:+,.2f} puan"
                    if aday and (en_yakin is None or abs(fark) < en_yakin[1]):
                        en_yakin = (ad, abs(fark))
                print(f"  {ad:<52}: %{g:>10,.2f}{fark_metin}   [{baz[0].strftime('%d.%m.%Y')} {baz[1]:.4f} -> {son[0].strftime('%d.%m.%Y')} {son[1]:.4f}]", flush=True)

            son_duz = _o_gun_veya_oncesi(duz, bit)
            if referans is not None and son_duz:
                ima = son_duz[1] / (1 + referans / 100.0)
                parcalar_metin = []
                for kisa, ad, seri, aday in adaylar:
                    b = _kesin_oncesi(seri, bas) if seri else None
                    parcalar_metin.append(f"{kisa}={_fmt(b[1]) if b else '-'}")
                print(f"\n  Finnet'in ima ettiği önceki kapanış ≈ {ima:.4f}  |  bizim bazlar: " + " | ".join(parcalar_metin), flush=True)

            if en_yakin is not None:
                print(f"  EN YAKIN YÖNTEM: {en_yakin[0]}  (fark {en_yakin[1]:,.2f} puan)", flush=True)
                galibiyet[en_yakin[0]] = galibiyet.get(en_yakin[0], 0) + 1

            esik_tarih = bas - datetime.timedelta(days=10)
            sermaye_olaylari = [o for o in _sermaye_olaylari(ser) if o[0] >= esik_tarih] if ser else []
            print(f"\n  --- BAŞLANGIÇTAN SONRAKİ SERMAYE DEĞİŞİMLERİ (toplam {len(sermaye_olaylari)}) ---", flush=True)
            for d, eski_s, yeni_s, oran in sermaye_olaylari[-40:]:
                print(f"  {d.strftime('%d.%m.%Y'):<12} sermaye {eski_s:,.0f} -> {yeni_s:,.0f}   oran {oran:.4f}  (sermaye düzeltme çarpanı {1.0 / oran:.4f})", flush=True)

            duz_olaylari = [o for o in _olaylari_bul(duz, ham) if o[0] >= esik_tarih] if (duz and ham) else []
            print(f"\n  --- BAŞLANGIÇTAN SONRAKİ İŞ YATIRIM DÜZELTME OLAYLARI (toplam {len(duz_olaylari)}) ---", flush=True)
            for d, onceki, f_eski, f_yeni, oran in duz_olaylari[-40:]:
                print(f"  {d.strftime('%d.%m.%Y'):<12} çarpan {1.0 / oran:.4f}   ham {ham[onceki]:.2f} -> {ham[d]:.2f}", flush=True)

            if hib:
                hib_olaylari = [e for e in hib[3] if e[0] >= esik_tarih]
                print(f"\n  --- HİBRİT EŞLEŞTİRME: İŞ YATIRIM OLAYI ↔ SERMAYE DEĞİŞİMİ (toplam {len(hib_olaylari)}) ---", flush=True)
                for d, c_is, c_s, sinif in hib_olaylari[-40:]:
                    c_s_metin = f"{c_s:.4f}" if c_s is not None else "-"
                    print(f"  {d.strftime('%d.%m.%Y'):<12} İş Yatırım çarpanı {c_is:.4f} | sermaye çarpanı {c_s_metin} | {sinif}", flush=True)

        print("=" * 110, flush=True)
        if galibiyet:
            print("[FINNET ÖZET] Finnet'e en yakın çıkan yöntem sayıları:", flush=True)
            for ad, adet in sorted(galibiyet.items(), key=lambda kv: -kv[1]):
                print(f"   {adet} hisse: {ad}", flush=True)
            print("   Kazanan yöntemi uygulamak için GitHub > Settings > Secrets and variables > Actions > Variables bölümünde", flush=True)
            print("   FIYAT_YONTEMI değişkenini ISYATIRIM / SERMAYE / HIBRIT olarak ayarlayıp 'haftalik' motorunu çalıştırın.", flush=True)
        print("=" * 110, flush=True)
    except Exception:
        print(traceback.format_exc(), flush=True)
        sys.exit(1)

def anomali_motoru():
    baslangic = time.time()
    try:
        sheet, headers, tarih_to_row, son_satir, son_tarih_str = baglanti_ve_bilgi_getir()
        if not son_tarih_str:
            print("[!] Tabloda geçerli tarih bulunamadı.", flush=True)
            return

        satir_tarihi = {row: datetime.datetime.strptime(t, "%d.%m.%Y").date() for t, row in tarih_to_row.items()}
        tarihler = [satir_tarihi.get(r) for r in range(2, son_satir + 1)]
        n = len(tarihler)

        hisseler = []
        for idx, ad in enumerate(headers):
            if idx == 0 or not ad or ad in YFINANCE_SUTUNLAR: continue
            hisseler.append((ad, idx + 1))

        bulunanlar = []
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

            for ad, col in grup:
                j = col - c1
                seri = []
                for r in range(n):
                    d = tarihler[r]
                    if d is None: continue
                    satir = blok[r] if r < len(blok) else []
                    v = saklanan_sayi(satir[j]) if j < len(satir) else None
                    if v is not None: seri.append((d, v))

                atla = False
                for k in range(1, len(seri)):
                    if atla:
                        atla = False
                        continue
                    ot, ov = seri[k - 1]
                    t, v = seri[k]
                    gun_farki = (t - ot).days
                    oran = v / ov
                    if gun_farki <= ANOMALI_MAX_GUN_FARKI and (oran < ANOMALI_ALT or oran > ANOMALI_UST):
                        tur = "KALICI KAYMA"
                        if k + 1 < len(seri):
                            t2, v2 = seri[k + 1]
                            geri = v2 / ov
                            if (t2 - t).days <= ANOMALI_MAX_GUN_FARKI and ANOMALI_ALT <= geri <= ANOMALI_UST:
                                tur = "TEK GÜNLÜK SAPMA"
                                atla = True
                        if min(ov, v) < ANOMALI_DUSUK_FIYAT:
                            tur = "DÜŞÜK FİYAT"
                        bulunanlar.append((ad, ot, ov, t, v, oran, gun_farki, tur))
            del blok
            gc.collect()
            print(f"[ANOMALİ] {min(g + HAFTALIK_OKUMA_GRUBU, len(hisseler))}/{len(hisseler)} sütun tarandı", flush=True)

        kalici = [x for x in bulunanlar if x[7] == "KALICI KAYMA"]
        tek_gun = [x for x in bulunanlar if x[7] == "TEK GÜNLÜK SAPMA"]
        dusuk = [x for x in bulunanlar if x[7] == "DÜŞÜK FİYAT"]
        kalici.sort(key=lambda x: x[3], reverse=True)
        tek_gun.sort(key=lambda x: x[3], reverse=True)
        dusuk.sort(key=lambda x: x[3], reverse=True)
        siralanmis = kalici + tek_gun + dusuk

        satirlar = [["HİSSE", "ÖNCEKİ TARİH", "ÖNCEKİ FİYAT", "TARİH", "FİYAT", "ORAN", "GÜN FARKI", "TÜR", "YORUM"]]
        for ad, ot, ov, t, v, oran, gf, tur in siralanmis[:ANOMALI_MAX_SATIR]:
            if tur == "DÜŞÜK FİYAT":
                yorum = "Fiyat çok düşük (<0,05 TL): yuvarlama veya fiyat adımı kaynaklı gürültü olabilir"
            elif tur == "TEK GÜNLÜK SAPMA":
                yorum = "Tek günlük hatalı fiyat olabilir (ertesi gün eski seviyeye dönüyor)"
            elif oran < ANOMALI_ALT:
                yorum = "Kalıcı düşüş: düzeltilmemiş bölünme veya kaynak verisi hatası olabilir"
            else:
                yorum = "Kalıcı yükseliş: ters bölünme veya kaynak verisi hatası olabilir"
            satirlar.append([ad, ot.strftime("%d.%m.%Y"), round(ov, 4), t.strftime("%d.%m.%Y"), round(v, 4), round(oran, 4), gf, tur, yorum])

        try:
            ss = sheet.spreadsheet
            try:
                ws = ss.worksheet(ANOMALI_SAYFA_ADI)
                ws.clear()
            except gspread.exceptions.WorksheetNotFound:
                ws = ss.add_worksheet(title=ANOMALI_SAYFA_ADI, rows=max(len(satirlar) + 20, 100), cols=9)
            if ws.row_count < len(satirlar) + 5 or ws.col_count < 9:
                ws.resize(rows=max(ws.row_count, len(satirlar) + 20), cols=max(ws.col_count, 9))
            ws.update(values=satirlar, range_name='A1', value_input_option='RAW')
        except Exception as e:
            print(f"[!] Anomali Raporu sayfasına yazılamadı: {e}", flush=True)

        sure = int(time.time() - baslangic)
        yil_siniri = datetime.datetime.now().year - ANOMALI_YIL_SINIRI
        print("=" * 70, flush=True)
        print(f"[ANOMALİ ÖZET] Toplam: {len(bulunanlar)} | KALICI KAYMA: {len(kalici)} | TEK GÜNLÜK SAPMA: {len(tek_gun)} | DÜŞÜK FİYAT: {len(dusuk)} | Etkilenen hisse: {len(set(x[0] for x in bulunanlar))} | Süre: {sure // 60} dk {sure % 60} sn", flush=True)
        print(f"[NORMAL FİYATLI KALICI KAYMALAR] {len(kalici)} olay, {len(set(x[0] for x in kalici))} hisse (gerçek kurumsal işlem veya kaynak hatası adayları)", flush=True)

        yil_sayac = {}
        for x in kalici:
            yil_sayac[x[3].year] = yil_sayac.get(x[3].year, 0) + 1
        print("[KALICI KAYMALARIN YILLARA DAĞILIMI] " + " | ".join(f"{y}:{yil_sayac[y]}" for y in sorted(yil_sayac)), flush=True)

        son20 = [x for x in kalici if x[3].year >= yil_siniri]
        print(f"[SON 20 YILDAKİ KALICI KAYMA] {len(son20)} olay, {len(set(x[0] for x in son20))} hisse (15 ve 20 yıllık getirileri etkileyebilir)", flush=True)

        hisse_sayac = {}
        for x in kalici:
            hisse_sayac[x[0]] = hisse_sayac.get(x[0], 0) + 1
        en_cok = sorted(hisse_sayac.items(), key=lambda kv: -kv[1])[:10]
        print("[EN ÇOK KALICI KAYMASI OLAN 10 HİSSE] " + " | ".join(f"{a}:{b}" for a, b in en_cok), flush=True)

        print("[SON 20 YILDAKİ EN YENİ 15 KALICI KAYMA]", flush=True)
        for ad, ot, ov, t, v, oran, gf, tur in son20[:15]:
            print(f"   {ad}: {ot.strftime('%d.%m.%Y')} {round(ov, 4)} -> {t.strftime('%d.%m.%Y')} {round(v, 4)} (oran {round(oran, 4)})", flush=True)
        print("=" * 70, flush=True)

        # --- GÜNCELLEME KAYDI ---
        etkilenen = len(set(x[0] for x in bulunanlar))
        bugun_tarih = datetime.datetime.now().date()
        kalici_30 = len([x for x in kalici if (bugun_tarih - x[3]).days <= 30])
        kalici_365 = len([x for x in kalici if (bugun_tarih - x[3]).days <= 365])
        rapor = {
            "sut": len(hisseler), "top": len(bulunanlar), "kal": len(kalici), "tek": len(tek_gun),
            "dus": len(dusuk), "his": etkilenen, "k30": kalici_30, "k365": kalici_365
        }
        # Son 20 yıl (15 ve 20 yıllık getiri sütunlarını etkileyebilecek olaylar)
        son20_liste = [x for x in kalici if (bugun_tarih - x[3]).days <= 365 * ANOMALI_YIL_SINIRI + 5]
        hisse20 = {}
        for x in son20_liste:
            hisse20[x[0]] = hisse20.get(x[0], 0) + 1
        en_cok20 = sorted(hisse20.items(), key=lambda kv: (-kv[1], kv[0]))[:8]
        buyuk20 = sorted(son20_liste, key=lambda x: abs(math.log(x[5])), reverse=True)[:5]
        rapor["k20"] = len(son20_liste)
        rapor["h20"] = len(hisse20)
        rapor["en"] = [[a, b] for a, b in en_cok20]
        rapor["buy"] = [[x[0], x[3].strftime("%d.%m.%Y"), round(x[5], 3)] for x in buyuk20]
        print(f"[SON 20 YIL] Kalıcı kayma: {len(son20_liste)} olay, {len(hisse20)} hisse | En çok olan: " + ", ".join(f"{a}:{b}" for a, b in en_cok20), flush=True)
        print("[SON 20 YIL: EN BÜYÜK 5 SIÇRAMA] " + " | ".join(f"{x[0]} {x[3].strftime('%d.%m.%Y')} oran {round(x[5], 3)}" for x in buyuk20), flush=True)
        if kalici:
            en_yeni = kalici[0]
            rapor["son"] = [en_yeni[0], en_yeni[3].strftime("%d.%m.%Y"), round(en_yeni[5], 4)]
        ayrinti_metni = f"{len(hisseler)} sütun tarandı | Kalıcı kayma {len(kalici)}, tek günlük sapma {len(tek_gun)}, düşük fiyat {len(dusuk)} ({etkilenen} hisse etkilendi)"
        kayit_yaz(
            "ANOMALİ", "TARANDI", "-", f"{len(kalici)} kalıcı kayma", time.time() - baslangic,
            _json_ekle(ayrinti_metni, rapor)
        )

    except Exception as e:
        print(traceback.format_exc(), flush=True)
        kayit_yaz("ANOMALİ", "HATA", "-", "-", time.time() - baslangic, f"{type(e).__name__}: {str(e)[:200]}")
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "guncelle": guncelleme_motoru()
        elif sys.argv[1] == "kontrol": hizli_kontrol_motoru()
        elif sys.argv[1] == "bakim": bakim_motoru(15)
        elif sys.argv[1] == "haftalik": haftalik_motoru()
        elif sys.argv[1] == "anomali": anomali_motoru()
        elif sys.argv[1] == "teshis":
            teshis_motoru(
                sys.argv[2] if len(sys.argv) > 2 else "DOAS,AYGAZ,FROTO",
                sys.argv[3] if len(sys.argv) > 3 else "13-11-2019",
                sys.argv[4] if len(sys.argv) > 4 else datetime.datetime.now().strftime("%d-%m-%Y")
            )
        elif sys.argv[1] == "finnet":
            finnet_motoru(
                sys.argv[2] if len(sys.argv) > 2 else "AKSA:2904.75,FROTO:1587.29,DOAS:2972.26,ISMEN:4891.56,EGPRO:2914.88,TUPRS:2889.05",
                sys.argv[3] if len(sys.argv) > 3 else "13-11-2019",
                sys.argv[4] if len(sys.argv) > 4 else datetime.datetime.now().strftime("%d-%m-%Y")
            )
