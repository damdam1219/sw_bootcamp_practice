import pandas as pd
import os

folder = 'c:/data/소상공인상권정보'
files = [f for f in os.listdir(folder) if f.endswith('.csv')]

df_list = []

def try_read_csv(file_path):
    encodings = ['utf-8', 'cp949', 'euc-kr']
    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc)
        except Exception as e:
            pass
    # 인코딩 시도 모두 실패하면 None 리턴
    return None

for file in files:
    file_path = os.path.join(folder, file)
    df = try_read_csv(file_path)
    if df is not None:
        print(f"{file} → CSV로 읽기 성공")
        df_list.append(df)
    else:
        print(f"{file} 읽기 실패: 인코딩 문제로 읽을 수 없음")

if df_list:
    merged_df = pd.concat(df_list, ignore_index=True)
    print(f"총 {len(merged_df)}행으로 병합 완료")
else:
    print("읽을 수 있는 데이터가 없습니다.")


import duckdb

# DuckDB 연결
duck_con = duckdb.connect('c:/data/duck_smb.db')

# DataFrame을 DuckDB에 임시 테이블로 등록
duck_con.register('tb_smb_file', merged_df)

# tb_smb_summary 테이블을 생성(덮어쓰기)하는 쿼리 실행
duck_con.execute("""
CREATE OR REPLACE TABLE tb_smb_summary AS
SELECT addr1, addr2, addr3, cate3_nm, cnt
FROM (
    SELECT addr1, addr2, addr3, cate3_nm, cnt,
           RANK() OVER (PARTITION BY addr3 ORDER BY cnt DESC) AS t_rank
    FROM (
        SELECT 
            "시도명" AS addr1,
            "시군구명" AS addr2,
            "행정동명" AS addr3,
            "상권업종소분류명" AS cate3_nm,
            COUNT(*) AS cnt
        FROM tb_smb_file
        GROUP BY "시도명", "시군구명", "행정동명", "상권업종소분류명"
    ) temp_rank
) temp_rank2
WHERE t_rank = 1
ORDER BY cnt DESC;
""")

# 결과 조회
result_df = duck_con.execute("SELECT * FROM tb_smb_summary LIMIT 10").df()
print(result_df)
