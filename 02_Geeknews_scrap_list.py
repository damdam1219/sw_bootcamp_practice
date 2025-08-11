from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
import time
import mariadb
import sys
from urllib.parse import urljoin

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

scrap_url_list = []
scrap_url_list.append(['https://news.hada.io/?page=', -1])  # 최신 뉴스 전체

source_type = '0'
duplicate_yn = 'Y'
duplicate_max = 30  # 중복 30개 이상이면 수집 중단

def get_comment_count(page, url):
    try:
        page.goto(url)
        # 댓글 영역 로딩 기다리기 (없으면 Timeout 에러 발생)
        page.wait_for_selector('div.comment_row > div.commentTD', timeout=5000)
        content = page.content()
        soup = BeautifulSoup(content, 'html.parser')
        comments = soup.select('div.comment_row > div.commentTD')
        return len(comments)
    except Exception as e:
        print(f'[error] 댓글 수 크롤링 실패: {e}')
        return 0

with sync_playwright() as p:
    current_list_pos = 0
    current_page = 1

    browser = p.firefox.launch(headless=True)
    main_page = browser.new_page()

    while True:
        try:
            time.sleep(5)
            main_page.goto(f'{scrap_url_list[current_list_pos][0]}{current_page}')
        except PlaywrightTimeoutError as te:
            print(f"Error browser: {te}")
            browser.close()
            browser = p.firefox.launch(headless=True)
            main_page = browser.new_page()
            time.sleep(60)
            main_page.goto(f'{scrap_url_list[current_list_pos][0]}{current_page}')

        time.sleep(5)
        print('[debug] list page : ', main_page.url)

        content = main_page.content()
        soup = BeautifulSoup(content, "html.parser")
        temp_soup = soup.select_one('body > main > article > div')

        if not temp_soup:
            print('[debug] no wrapper div found... break loop')
            break

        news_list = temp_soup.select('div.topicdesc > a')

        if len(news_list) == 0:
            print('[debug] no news found...')
            break

        duplicate_cnt = 0
        for news in news_list:
            source_url = news.get('href')
            if not source_url:
                continue
            source_url = urljoin(main_page.url, source_url)

            print('source_url : ', source_url)

            cur.execute('select * from gn_scrap_ready where source_type = ? and source_url = ?', (source_type, source_url))
            res = cur.fetchall()
            if len(res) > 0:
                print('[debug] DB에 source_url 존재 --> Skip')
                duplicate_cnt += 1
                if duplicate_yn == 'Y' and duplicate_cnt >= duplicate_max:
                    print('[debug] duplicate_cnt >= 30 --> break')
                    current_page = scrap_url_list[current_list_pos][1]
                    break
                continue

            comment_cnt = get_comment_count(main_page, source_url)
            print(f'[info] 댓글 수: {comment_cnt} for {source_url}')

            insert_sql = """
                insert into gn_scrap_ready(source_type, source_url, comment_cnt, create_dt)
                values (?, ?, ?, now())
            """
            cur.execute(insert_sql, (source_type, source_url, comment_cnt))
            conn.commit()

        if current_page == scrap_url_list[current_list_pos][1]:
            if current_list_pos == (len(scrap_url_list) - 1):
                current_list_pos = 0
            else:
                current_list_pos += 1
            current_page = 1
            print('[debug] next section : ', current_list_pos)
        else:
            current_page += 1
            print('[debug] next page : ', current_page)
            time.sleep(5)

    browser.close()


