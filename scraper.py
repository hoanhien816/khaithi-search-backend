import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import re
import time
import json
from supabase import create_client, Client

# --- Cấu hình ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

BASE_URL = "https://timkhaithi.pmtl.site"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_supabase_client():
    """Khởi tạo và trả về Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise EnvironmentError("Vui lòng đặt biến môi trường SUPABASE_URL và SUPABASE_KEY.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_article_urls_from_feed():
    """
    Lấy TOÀN BỘ URL bài viết từ RSS feed, tự động xử lý phân trang
    để lấy hết tất cả các bài viết.
    """
    print("Bắt đầu lấy toàn bộ URL từ RSS Feed, có xử lý phân trang...")
    all_urls = []
    start_index = 1
    max_results = 500

    while True:
        paginated_url = f"{BASE_URL}/feeds/posts/default?orderby=published&alt=json-in-script&start-index={start_index}&max-results={max_results}"
        print(f"Đang lấy trang từ URL: {paginated_url}")
        try:
            response = requests.get(paginated_url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            jsonp_data = response.text
            start_json = jsonp_data.find('(')
            end_json = jsonp_data.rfind(')')
            if start_json == -1 or end_json == -1:
                print("Lỗi phân tích JSONP, không tìm thấy dấu ngoặc.")
                break
            json_str = jsonp_data[start_json + 1: end_json]
            json_data = json.loads(json_str)
            feed = json_data.get('feed', {})
            entries = feed.get('entry', [])
            if not entries:
                print("Không còn bài viết nào. Kết thúc quá trình lấy URL.")
                break
            for entry in entries:
                post_url = next((link['href'] for link in entry.get('link', []) if link.get('rel') == 'alternate'), None)
                if post_url:
                    all_urls.append({
                        'url': post_url,
                        'title': entry.get('title', {}).get('$t', ''),
                        'published': entry.get('published', {}).get('$t', '')
                    })
            print(f"Đã lấy được {len(entries)} bài viết. Tổng số hiện tại: {len(all_urls)}")
            start_index += len(entries)
            time.sleep(1)
        except Exception as e:
            print(f"Lỗi khi xử lý feed: {e}")
            break
    print(f"Hoàn tất! Đã tìm thấy tổng cộng {len(all_urls)} URL bài viết.")
    return all_urls

def scrape_article_content(url):
    """Tải và trích xuất nội dung chính của một bài viết."""
    print(f"Đang scrape nội dung từ: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        content_div = soup.find('div', class_='post-body')
        if not content_div:
            print(f"Không tìm thấy nội dung cho URL: {url}. Vui lòng kiểm tra selector.")
            return None
        for unwanted_tag in content_div.find_all(['script', 'style', 'ins', 'iframe']):
            unwanted_tag.decompose()
        content_text = content_div.get_text(separator='\n', strip=True)
        return re.sub(r'\n\s*\n', '\n\n', content_text).strip()
    except Exception as e:
        print(f"Lỗi khi phân tích HTML cho {url}: {e}")
        return None

def upsert_article_rpc(supabase_client: Client, article_data: dict):
    """Gọi một PostgreSQL Function (RPC) trong Supabase để chèn hoặc cập nhật bài viết."""
    try:
        # Chuyển đổi chuỗi ngày tháng từ feed sang đối tượng datetime
        published_date_obj = datetime.fromisoformat(article_data['published'].replace('Z', '+00:00'))
        params = {
            'p_title': article_data['title'],
            'p_url': article_data['url'],
            'p_content': article_data['content'],
            'p_published_date': published_date_obj.isoformat() # Gửi đi ở định dạng chuẩn
        }
        supabase_client.rpc('upsert_article', params).execute()
        return True
    except Exception as e:
        print(f"Lỗi RPC khi xử lý bài viết '{article_data['title']}': {e}")
        return False

# ======================================================================
# *** HÀM MỚI ĐỂ SỬA LỖI ĐỌC NGÀY THÁNG ***
# ======================================================================
def parse_db_datetime(dt_str: str) -> datetime:
    """Hàm chuyển đổi chuỗi ngày tháng từ DB một cách linh hoạt, chấp nhận cả +00:00 và +0000."""
    if dt_str.endswith('+00:00'):
        # Loại bỏ dấu hai chấm để tương thích với fromisoformat
        dt_str = dt_str[:-3] + dt_str[-2:]
    return datetime.fromisoformat(dt_str)

def main_scraper():
    """Hàm chính để chạy toàn bộ quá trình scraper, bao gồm cả việc xóa bài viết cũ."""
    try:
        supabase = get_supabase_client()

        # BƯỚC 1: Lấy URL từ blog gốc
        print("Bắt đầu lấy danh sách bài viết từ blog gốc...")
        articles_from_feed = get_article_urls_from_feed()
        if not articles_from_feed:
            print("Không có bài viết nào từ feed. Dừng quá trình.")
            return
        source_urls = {article['url'] for article in articles_from_feed}
        print(f"Đã tìm thấy {len(source_urls)} URL hợp lệ từ blog.")

        # BƯỚC 2: Lấy URL từ cơ sở dữ liệu
        print("\nBắt đầu lấy danh sách bài viết từ cơ sở dữ liệu...")
        response = supabase.table('articles').select('url, published_date').execute()
        if response.error:
            raise Exception(f"Lỗi khi lấy dữ liệu từ Supabase: {response.error.message}")
            
        # *** SỬA LỖI Ở ĐÂY: Sử dụng hàm parse_db_datetime mới ***
        db_articles = {item['url']: parse_db_datetime(item['published_date']) for item in response.data}
        db_urls = set(db_articles.keys())
        print(f"Cơ sở dữ liệu hiện có {len(db_urls)} bài viết.")

        # BƯỚC 3: Xác định và XÓA bài viết cũ
        urls_to_delete = db_urls - source_urls
        if urls_to_delete:
            print(f"\nTìm thấy {len(urls_to_delete)} bài viết cần xóa khỏi cơ sở dữ liệu.")
            urls_to_delete_list = list(urls_to_delete)
            try:
                print("Bắt đầu gửi yêu cầu xóa tới Supabase...")
                chunk_size = 100
                for i in range(0, len(urls_to_delete_list), chunk_size):
                    chunk = urls_to_delete_list[i:i + chunk_size]
                    delete_result = supabase.table('articles').delete().in_('url', chunk).execute()
                    if delete_result.error:
                        raise Exception(f"Lỗi Supabase khi xóa: {delete_result.error.message}")
                print("Đã xóa thành công các bài viết cũ.")
            except Exception as e:
                print(f"Lỗi khi thực thi việc xóa: {e}")
        else:
            print("\nKhông có bài viết nào cần xóa. Cơ sở dữ liệu đã được đồng bộ.")

        # BƯỚC 4: Cập nhật và Thêm bài viết mới
        print("\nBắt đầu quá trình thêm mới và cập nhật bài viết...")
        for article_info in articles_from_feed:
            url = article_info['url']
            title = article_info['title']
            feed_published_date = datetime.fromisoformat(article_info['published'].replace('Z', '+00:00'))

            if url not in db_articles or feed_published_date > db_articles[url]:
                print(f"Đang xử lý: '{title}'")
                content = scrape_article_content(url)
                if content:
                    upsert_article_rpc(supabase, {
                        'title': title,
                        'url': url,
                        'content': content,
                        'published': article_info['published']
                    })
                time.sleep(1)

    except Exception as e:
        print(f"Lỗi nghiêm trọng trong quá trình scrape: {e}")
    finally:
        print("\nHoàn tất quá trình scrape.")

if __name__ == "__main__":
    main_scraper()
