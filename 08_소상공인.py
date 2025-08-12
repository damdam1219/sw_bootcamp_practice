import os
import pandas as pd
import mariadb
import sys
import math

try:
    conn = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="localhost",
        port=3310,
        database="cp_data"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cur = conn.cursor()

folder_path = r'c:\data\소상공인상권정보'
file_list = os.listdir(folder_path)
csv_files = [f for f in file_list if f.endswith('.csv')]

cols = [f'col{i}' for i in range(1, 40)]
placeholders = ', '.join(['%s'] * len(cols))
insert_sql = f"""
    INSERT INTO tb_smb_ods ({', '.join(cols)}) VALUES ({placeholders})
"""

batch_size = 1000

# 숫자형 컬럼 인덱스 (0부터 시작) 실제 숫자형 컬럼 번호로 수정 필요
num_cols = [22, 23, 37, 38]

for csv_file in csv_files:
    full_path = os.path.join(folder_path, csv_file)
    print(f"Processing {full_path}")

    try:
        # 모든 컬럼을 문자열로 읽어온다 (일단 다 str로 읽기)
        df = pd.read_csv(full_path, encoding='utf-8', dtype=str)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        continue

    if len(df.columns) != len(cols):
        print(f"Warning: {csv_file} 컬럼 수({len(df.columns)})가 예상과 다릅니다({len(cols)}).")

    df.columns = cols[:len(df.columns)]

    # 숫자형 컬럼은 숫자로 변환, 변환 불가하면 0으로 대체
    for cidx in num_cols:
        col_name = cols[cidx]
        df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0)

    data_to_insert = []

    def clean_value(val, idx):
        # 숫자형 컬럼은 float이나 int 타입 유지
        if idx in num_cols:
            if pd.isna(val):
                return 0
            if isinstance(val, str):
                try:
                    return float(val)
                except:
                    return 0
            return val
        else:
            # 문자형 컬럼, 결측치는 빈문자열로
            if pd.isna(val):
                return ''
            return val

    for row in df.itertuples(index=False, name=None):
        values = [clean_value(val, idx) for idx, val in enumerate(row)]
        data_to_insert.append(tuple(values))

        if len(data_to_insert) >= batch_size:
            try:
                cur.executemany(insert_sql, data_to_insert)
                conn.commit()
                print(f"{len(data_to_insert)} rows inserted so far from {csv_file}.")
                data_to_insert = []
            except mariadb.Error as e:
                print(f"Error inserting batch from {csv_file}: {e}")
                data_to_insert = []

    if data_to_insert:
        try:
            cur.executemany(insert_sql, data_to_insert)
            conn.commit()
            print(f"{len(data_to_insert)} rows inserted from {csv_file}.")
        except mariadb.Error as e:
            print(f"Error inserting final batch from {csv_file}: {e}")

cur.close()
conn.close()



