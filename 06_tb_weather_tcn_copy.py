import mariadb
import sys

# 원격 MariaDB 연결 정보
SRC_DB_CONFIG = {
    'host': '192.168.14.38',
    'user': 'lguplus7',
    'password': 'lg7p@ssw0rd~!',
    'database': 'cp_data',
    'port': 3310
}

# 내 MariaDB 연결 정보 (예: cp_data DB에 접속)
DST_DB_CONFIG = {
    'host': 'localhost',
    'user': 'lguplus7',
    'password': 'lg7p@ssw0rd~!',
    'database': 'cp_data',
    'port': 3310
}

def sync_tb_weather_tcn():
    try:
        # 원격 DB 연결
        src_conn = mariadb.connect(**SRC_DB_CONFIG)
        src_cur = src_conn.cursor()

    except mariadb.Error as e:
        print(f"Error connecting to source MariaDB: {e}")
        sys.exit(1)

    try:
        # 내 DB 연결
        dst_conn = mariadb.connect(**DST_DB_CONFIG)
        dst_cur = dst_conn.cursor()

    except mariadb.Error as e:
        print(f"Error connecting to target MariaDB: {e}")
        sys.exit(1)

    try:
        print("원격 DB에서 데이터 조회 중...")
        src_cur.execute("""
            SELECT seq_no, STN_ID, LON, LAT, STN_SP, HT, HT_WD, LAU_ID, STN_AD,
                   STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN, addr1, addr2, addr3,
                   org_addr, create_dt
            FROM tb_weather_tcn
        """)
        rows = src_cur.fetchall()
        print(f"총 {len(rows)} 건 데이터 조회됨")

        print("내 DB 테이블 데이터 삭제 중...")
        dst_cur.execute("DELETE FROM tb_weather_tcn")

        print("내 DB에 데이터 삽입 중...")
        insert_sql = """
            INSERT INTO tb_weather_tcn (
                seq_no, STN_ID, LON, LAT, STN_SP, HT, HT_WD, LAU_ID, STN_AD,
                STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN, addr1, addr2, addr3,
                org_addr, create_dt
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?
            )
        """
        dst_cur.executemany(insert_sql, rows)
        dst_conn.commit()

        print("동기화 완료!")

    except mariadb.Error as e:
        print(f"MariaDB 오류 발생: {e}")
        sys.exit(1)

    finally:
        src_cur.close()
        src_conn.close()
        dst_cur.close()
        dst_conn.close()

if __name__ == "__main__":
    sync_tb_weather_tcn()
