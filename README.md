# sw_bootcamp_practice
부트캠프 실습 과정에서 작성된 **Python 기반 웹 스크래핑 + DB 저장 자동화 스크립트 모음**입니다.

## 1. 프로젝트 개요
웹과 API를 통해 데이터를 수집하고, 이를 JSON이나 데이터베이스에 저장하여 향후 분석이 가능하도록 구축된 자동화 파이프라인입니다.

---
## 2. 스크립트 기능 요약
| 스크립트 파일 | 기능 요약 |
|---------------|-----------|
| `00_lg7_scrap_naver_news_list.py`, `01_lg7_scrap_naver_news_item.py` | 네이버 뉴스 목록 및 세부 기사 수집 |
| `02_Geeknews_scrap_list.py`, `03_Geeknews_scrap_item.py` | Geek News의 목록 및 상세 기사 스크래핑 |
| `04_lg7_file_json-DB_connect.py` | JSON 파일 형식의 데이터를 DB에 저장 |
| `05_API_data-DB_connect.py`, `05.2_API_data-DB_connect.py` | 외부 API 호출 결과를 DB에 연동 |
| `06_tb_weather_tcn_copy.py`, `07_lg7_api_weather_aws1.py` | 날씨 관련 API 데이터 수집 및 DB 저장 (AWS 연계 포함) |
| `08_2_DuckDB.py` | DuckDB 기반의 데이터 분석 및 저장 |
| `08_소상공인.py` | 소상공인 관련 데이터를 외부에서 수집해 저장 및 분석 |
