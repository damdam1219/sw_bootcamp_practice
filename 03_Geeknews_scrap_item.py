from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
import time
import mariadb
import sys
from urllib.parse import urljoin

# DB 연결 설정
try:
    conn = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="localhost",
        port=3310,
        database="cp_data"
    )
except mariadb.Error as e:
    print(f"[error] MariaDB 연결 실패: {e}")
    sys.exit(1)

cur = conn.cursor()

# 크롤링 대상 URL
scrap_url_list = ['https://news.hada.io/?page=']

# 뉴스 상세 페이지 크롤링 함수
def get_news_data(page, url):
    try:
        page.goto(url)
        time.sleep(3)  # 페이지 로딩 대기
        content = page.content()
        soup = BeautifulSoup(content, 'html.parser')

        # 제목
        title_tag = soup.select_one('div.topictitle.link > a > h1')
        news_title = title_tag.get_text(strip=True) if title_tag else None


        # 본문 내용
        desc_tag = soup.select_one('#topic_contents')
        news_desc = desc_tag.get_text(strip=True) if desc_tag else None

        # 댓글
        comment_tags = soup.select('p[id^="contents"]') or soup.select('.comment_contents')
        news_comments = "\n\n".join([c.get_text(strip=True) for c in comment_tags]) if comment_tags else None

        # 전체 HTML 원문
        full_contents = soup.prettify()

        # 게시일 및 업데이트일: 현재 없음 (NULL 처리)
        news_pub_date = None
        news_update = None

        return news_title, news_desc, news_comments, news_pub_date, news_update, full_contents
    except Exception as e:
        print(f"[error] 뉴스 상세 크롤링 실패: {e}")
        return None, None, None, None, None, None

# Playwright 실행
with sync_playwright() as p:
    browser = p.firefox.launch(headless=True)
    main_page = browser.new_page()

    current_page = 1
    while True:
        try:
            list_url = f'{scrap_url_list[0]}{current_page}'
            main_page.goto(list_url, wait_until='networkidle')
            main_page.wait_for_selector('div.topicdesc > a') 
        except PlaywrightTimeoutError as te:
            print(f"[error] 리스트 페이지 로딩 실패: {te}")
            break

        list_content = main_page.content()
        list_soup = BeautifulSoup(list_content, 'html.parser')

        # 뉴스 링크 추출
        news_links = list_soup.select('div.topicdesc > a')
        if not news_links:
            print('[info] 더 이상 뉴스 없음. 종료.')
            break

        for link in news_links:
            href = link.get('href')
            if not href:
                continue
            source_url = urljoin(list_url, href)

            # 중복 여부 확인
            cur.execute('SELECT 1 FROM gn_master WHERE news_url = ?', (source_url,))
            if cur.fetchone():
                print(f'[skip] 이미 존재하는 뉴스: {source_url}')
                continue

            # 상세 페이지 크롤링
            news_title, news_desc, news_comments, news_pub_date, news_update, full_contents = get_news_data(main_page, source_url)
            if not news_title:
                print(f"[warn] 뉴스 제목 없음: {source_url}")
                continue

            print(f'[info] 수집 뉴스: {news_title} ({source_url})')

            # DB 삽입
            insert_sql = """
                INSERT INTO gn_master
                (news_title, news_desc, news_url, news_comments, news_pub_date, news_update, full_contents)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            cur.execute(insert_sql, (
                news_title,
                news_desc,
                source_url,
                news_comments,
                news_pub_date,
                news_update,
                full_contents
            ))
            conn.commit()

        current_page += 1
        print(f'[debug] 다음 페이지 이동: {current_page}')
        time.sleep(3)

    browser.close()
    conn.close()


