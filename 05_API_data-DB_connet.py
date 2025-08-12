import requests
import mariadb
from datetime import datetime
import sys

try:
    conn_tar = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="localhost",
        port=3310,
        database="cp_data"
    )
    cursor = conn_tar.cursor()
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# API 요청
url = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min?tm2=202302010900&stn=0&disp=0&help=1&authKey=9LD69So4RI6w-vUqOCSOQQ"  # 실제 URL로 변경
params = {
    'tm1': '202508120600',
    'tm2': '202508120700',
    'stn': '0',
    'disp': '1',
    'help': '2',
    'authKey': '9LD69So4RI6w-vUqOCSOQQ'
}

response = requests.get(url, params=params)

if response.status_code == 200:
    lines = response.text.strip().split('\n')

    for line in lines:
        if line.startswith('#') or not line.strip():
            continue

        values = line.strip().split(',')

        if values[-1] == '=':
            values = values[:-1]

        if len(values) < 18:
            print(f"Skipping line due to unexpected column count: {values}")
            continue

        (
            yyyymmddhhmi, stn, wd1, ws1, wds, wss,
            wd10, ws10, ta, re, rn_15m, rn_60m, rn_12h,
            rn_day, hm, pa, ps, td
        ) = values[:18]

        org_data = line.strip()
        update_dt = datetime.now().strftime('%Y%m%d%H%M%S')

        try:
            cursor.execute("""
                INSERT INTO tb_weather_aws1 (
                    yyyymmddhhmi, stn, wd1, ws1, wds, wss, wd10, ws10,
                    ta, re, rn_15m, rn_60m, rn_12h, rn_day, hm, pa, ps, td,
                    org_data, update_dt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                yyyymmddhhmi, stn, wd1, ws1, wds, wss, wd10, ws10,
                ta, re, rn_15m, rn_60m, rn_12h, rn_day, hm, pa, ps, td,
                org_data, update_dt
            ))
        except mariadb.Error as e:
            print(f"Insert error: {e} | Data: {values}")

    conn_tar.commit()
    cursor.close()
    conn_tar.close()
    print("데이터 적재 완료")
else:
    print(f"API 요청 실패: {response.status_code}")
