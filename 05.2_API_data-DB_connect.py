import requests
import mariadb
import sys
from datetime import datetime

API_URL = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"
params = {
    'inf': 'SFC',
    'stn': '',
    'tm': '202211300900',
    'help': '0',
    'authKey': '9LD69So4RI6w-vUqOCSOQQ'
}

try:
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    text = response.text
except Exception as e:
    print(f"API 요청 실패: {e}")
    sys.exit(1)

lines = text.strip().split('\n')
data_lines = [line for line in lines if line and not line.startswith('#')]

try:
    conn = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="localhost",
        port=3310,
        database="cp_data"
    )
    cursor = conn.cursor()
except mariadb.Error as e:
    print(f"DB 연결 실패: {e}")
    sys.exit(1)

for line in data_lines:
    parts = line.split()
    if len(parts) < 15:
        print(f"컬럼 수 부족: {line}")
        continue

    stn_id = parts[0]
    lon = parts[1]
    lat = parts[2]
    stn_sp = parts[3]
    ht = parts[4]
    ht_pa = parts[5]
    ht_ta = parts[6]
    ht_wd = parts[7]
    ht_rn = parts[8]
    stn_ad = parts[9]
    stn_ko = parts[10]
    stn_en = parts[11]
    fct_id = parts[12]
    law_id = parts[13]
    basin = parts[14]

    org_addr = line
    create_dt = datetime.now().strftime('%Y%m%d%H%M%S')

    try:
        cursor.execute("""
            INSERT INTO tb_weather_tcn (
                STN_ID, LON, LAT, STN_SP, HT, HT_PA, HT_TA, HT_WD,
                HT_RN, STN_AD, STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN,
                org_addr, create_dt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stn_id, lon, lat, stn_sp, ht, ht_pa, ht_ta, ht_wd,
            ht_rn, stn_ad, stn_ko, stn_en, fct_id, law_id, basin,
            org_addr, create_dt
        ))
    except mariadb.Error as e:
        print(f"DB 적재 오류: {e} | 데이터: {line}")

conn.commit()
cursor.close()
conn.close()

print("데이터 적재 완료!")

