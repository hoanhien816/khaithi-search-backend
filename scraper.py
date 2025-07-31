import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import re
import time
import os

# --- Cấu hình Database ---
# Lấy chuỗi kết nối từ biến môi trường DATABASE_URL
# Trong môi trường cục bộ, bạn có thể đặt biến này hoặc sử dụng thông tin trực tiếp
# Ví dụ: export DATABASE_URL="postgresql://khaithi_user:your_strong_password@localhost:5432/khaithi_db"
# Hoặc thay thế bằng thông tin cục bộ của bạn nếu không dùng biến môi trường khi chạy cục bộ
# DB_NAME = "khaithi_db"
# DB_USER = "khaithi_user"
# DB_PASSWORD = "your_strong_password"
# DB_HOST = "localhost"

# --- Cấu hình Website ---
BASE_URL = "https://timkhaithi.pmtl.site"
RSS_FEED_URL = f"{BASE_URL}/feeds/posts/default?orderby=published&alt=json-in-script&max-results=999" # Có thể tăng max-results nếu cần
# User-Agent để tránh bị chặn bởi một số website
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_db_connection():
    """Kết nối tới cơ sở dữ liệu PostgreSQL."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        # Fallback cho môi trường cục bộ nếu biến môi trường không được đặt
        # Bạn có thể thay thế bằng thông tin kết nối cục bộ của mình
        print("DATABASE_URL environment variable not set. Attempting local connection.")
        return psycopg2.connect(
            dbname="khaithi_db",
            user="khaithi_user",
            password="your_strong_password", # THAY THẾ BẰNG MẬT KHẨU CỦA BẠN KHI CHẠY CỤC BỘ
            host="localhost"
        )
    conn = psycopg2.connect(database_url)
    return conn

def get_article_urls_from_feed():
    """Lấy danh sách URL bài viết từ RSS feed của Blogger."""
    print(f"Đang lấy URL từ RSS Feed: {RSS_FEED_URL}")
    try:
        # Blogger JSONP feed thực chất là JavaScript, cần xử lý để lấy JSON
        response = requests.get(RSS_FEED_URL, headers=HEADERS)
        response.raise_for_status() # Ném lỗi nếu status code không phải 2xx
        
        # Trích xuất JSON từ phản hồi JSONP
        jsonp_data = response.text
        # Tìm chuỗi JSON bên trong callback function
        match = re.search(r'showrecentposts\((.*)\)', jsonp_data, re.DOTALL)
        if not match:
            print("Không tìm thấy dữ liệu JSON trong phản hồi JSONP.")
            return []
        
        json_data_str = match.group(1)
        json_data = eval(json_data_str) # Cẩn thận khi dùng eval, nhưng với nguồn tin cậy thì chấp nhận được

        urls = []
        if 'feed' in json_data and 'entry' in json_data['feed']:
            for entry in json_data['feed']['entry']:
                post_url = ''
                for link in entry['link']:
                    if link['rel'] == 'alternate':
                        post_url = link['href']
                        break
                if post_url:
                    urls.append({
                        'url': post_url,
                        'title': entry['title']['$t'],
                        'published': entry['published']['$t'] # Lấy ngày xuất bản
                    })
        print(f"Tìm thấy {len(urls)} URL bài viết từ RSS Feed.")
        return urls
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy RSS Feed: {e}")
        return []
    except Exception as e:
        print(f"Lỗi khi xử lý JSONP: {e}")
        return []

def scrape_article_content(url):
    """Tải và trích xuất nội dung chính của một bài viết."""
    print(f"Đang scrape nội dung từ: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=10) # Thêm timeout
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # --- Điều chỉnh selector này dựa trên cấu trúc HTML thực tế của bài viết ---
        # Bạn cần kiểm tra mã nguồn của một bài viết trên timkhaithi.pmtl.site
        # để tìm thẻ HTML hoặc class CSS chứa nội dung chính.
        # Ví dụ: nếu nội dung nằm trong một div có class 'post-body'
        content_div = soup.find('div', class_='post-body entry-content') # Đây là một class phổ biến của Blogger
        
        if not content_div:
            print(f"Không tìm thấy nội dung bài viết cho URL: {url}. Vui lòng kiểm tra selector.")
            return None

        # Loại bỏ các thẻ không mong muốn (ví dụ: script, style, quảng cáo)
        for unwanted_tag in content_div.find_all(['script', 'style', 'ins', 'iframe', 'a[href*="blogspot.com/"]']):
            unwanted_tag.decompose() # Xóa thẻ này khỏi cây DOM

        # Lấy văn bản từ nội dung chính
        content_text = content_div.get_text(separator='\n', strip=True)
        
        # Xóa các khoảng trắng thừa và dòng trống
        content_text = re.sub(r'\n\s*\n', '\n\n', content_text).strip()

        return content_text
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải trang {url}: {e}")
        return None
    except Exception as e:
        print(f"Lỗi khi phân tích HTML cho {url}: {e}")
        return None

def insert_or_update_article(conn, article_data):
    """Chèn hoặc cập nhật bài viết vào cơ sở dữ liệu."""
    cursor = conn.cursor()
    try:
        # Chuyển đổi published_date sang định dạng TIMESTAMP WITH TIME ZONE
        published_date_obj = datetime.fromisoformat(article_data['published'].replace('Z', '+00:00'))

        # Kiểm tra xem bài viết đã tồn tại chưa
        cursor.execute("SELECT id FROM articles WHERE url = %s", (article_data['url'],))
        existing_article = cursor.fetchone()

        if existing_article:
            # Cập nhật bài viết nếu đã tồn tại
            print(f"Cập nhật bài viết: {article_data['title']}")
            cursor.execute(
                """
                UPDATE articles
                SET title = %s, content = %s, published_date = %s,
                    tsv = to_tsvector('vietnamese', unaccent(COALESCE(%s, '') || ' ' || COALESCE(%s, '')))
                WHERE url = %s
                """,
                (article_data['title'], article_data['content'], published_date_obj,
                 article_data['title'], article_data['content'], article_data['url'])
            )
        else:
            # Chèn bài viết mới
            print(f"Thêm bài viết mới: {article_data['title']}")
            cursor.execute(
                """
                INSERT INTO articles (title, url, content, published_date, tsv)
                VALUES (%s, %s, %s, %s,
                        to_tsvector('vietnamese', unaccent(COALESCE(%s, '') || ' ' || COALESCE(%s, ''))))
                """,
                (article_data['title'], article_data['url'], article_data['content'], published_date_obj,
                 article_data['title'], article_data['content'])
            )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Lỗi DB khi xử lý bài viết '{article_data['title']}': {e}")
    finally:
        cursor.close()

def main_scraper():
    conn = None
    try:
        conn = get_db_connection()
        article_urls_from_feed = get_article_urls_from_feed()

        for article_info in article_urls_from_feed:
            url = article_info['url']
            title = article_info['title']
            published = article_info['published']

            # Kiểm tra xem bài viết đã có trong DB với ngày xuất bản mới nhất chưa
            cursor = conn.cursor()
            cursor.execute("SELECT published_date FROM articles WHERE url = %s", (url,))
            db_published_date_raw = cursor.fetchone()
            cursor.close()

            if db_published_date_raw:
                db_published_date = db_published_date_raw[0]
                feed_published_date = datetime.fromisoformat(published.replace('Z', '+00:00'))
                if feed_published_date <= db_published_date:
                    print(f"Bài viết '{title}' đã được cập nhật hoặc mới hơn trong DB. Bỏ qua.")
                    continue # Bỏ qua nếu bài viết đã có và không cũ hơn

            content = scrape_article_content(url)
            if content:
                insert_or_update_article(conn, {
                    'title': title,
                    'url': url,
                    'content': content,
                    'published': published
                })
            time.sleep(1) # Đợi 1 giây giữa các yêu cầu để không gây quá tải cho server

    except Exception as e:
        print(f"Lỗi tổng quát trong quá trình scrape: {e}")
    finally:
        if conn:
            conn.close()
            print("Đã đóng kết nối database.")

if __name__ == "__main__":
    main_scraper()
