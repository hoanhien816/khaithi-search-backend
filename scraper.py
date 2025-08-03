import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
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
    """Lấy TOÀN BỘ URL bài viết từ RSS feed, tự động xử lý phân trang."""
    print("Bắt đầu lấy toàn bộ URL từ RSS Feed, có xử lý phân trang...")
    all_urls = []
    start_index = 1
    max_results = 500

    while True:
        paginated_url = f"{BASE_URL}/feeds/posts/default?orderby=published&alt=json-in-script&start-index={start_index}&max-results={max_results}"
        print(f"Đang lấy trang từ URL: {paginated_url}")
        try:
            response = requests.get(paginated_url, headers=HEADERS, timeout=30) # Tăng timeout
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
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        content_div = soup.find('div', class_='post-body')
        if not content_div: return None
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
        published_date_obj = datetime.fromisoformat(article_data['published'].replace('Z', '+00:00'))
        params = {
            'p_title': article_data['title'],
            'p_url': article_data['url'],
            'p_content': article_data['content'],
            'p_published_date': published_date_obj.isoformat()
        }
        supabase_client.rpc('upsert_article', params).execute()
    except Exception as e:
        print(f"Lỗi RPC khi xử lý bài viết '{article_data['title']}': {e}")

def parse_db_datetime(dt_str: str) -> datetime:
    """Hàm chuyển đổi chuỗi ngày tháng từ DB một cách linh hoạt, xử lý mọi trường hợp."""
    if dt_str.endswith('+00:00'):
        dt_str = dt_str[:-3] + dt_str[-2:]
    try:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S%z')

def main_scraper():
    """Hàm chính để chạy toàn bộ quá trình scraper, bao gồm cả việc xóa bài viết cũ."""
    try:
        supabase = get_supabase_client()

        # BƯỚC 1: Lấy URL từ blog gốc
        print("--- GIAI ĐOẠN 1: THU THẬP DỮ LIỆU ---")
        articles_from_feed = get_article_urls_from_feed()
        if not articles_from_feed: return
        source_urls = {article['url'] for article in articles_from_feed}
        print(f"[INFO] Tổng số URL duy nhất từ blog gốc: {len(source_urls)}")

        # BƯỚC 2: Lấy URL từ cơ sở dữ liệu
        print("\nBắt đầu lấy danh sách bài viết từ cơ sở dữ liệu...")
        response = supabase.table('articles').select('url, published_date').execute()
        db_articles = {item['url']: parse_db_datetime(item['published_date']) for item in response.data}
        db_urls = set(db_articles.keys())
        print(f"[INFO] Tổng số URL trong cơ sở dữ liệu: {len(db_urls)}")

        # BƯỚC 3: Xác định và XÓA bài viết cũ
        print("\n--- GIAI ĐOẠN 2: DỌN DẸP DỮ LIỆU CŨ ---")
        urls_to_delete = db_urls - source_urls
        if urls_to_delete:
            print(f"[DEBUG] Đã xác định được {len(urls_to_delete)} bài viết cần xóa.")
            # In ra 5 URL đầu tiên trong danh sách cần xóa để kiểm tra
            print("[DEBUG] 5 URL mẫu cần xóa:")
            for i, url in enumerate(list(urls_to_delete)[:5]):
                print(f"  - {i+1}: {url}")
            
            print("\n[ACTION] Bắt đầu gửi yêu cầu xóa tới Supabase...")
            urls_to_delete_list = list(urls_to_delete)
            chunk_size = 100
            for i in range(0, len(urls_to_delete_list), chunk_size):
                chunk = urls_to_delete_list[i:i + chunk_size]
                delete_result = supabase.table('articles').delete().in_('url', chunk).execute()
                print(f"  - Khối {i//chunk_size + 1}: Đã gửi lệnh xóa cho {len(chunk)} URL. Phản hồi từ Supabase: count={len(delete_result.data)}")
            print("[SUCCESS] Hoàn tất quá trình gửi lệnh xóa.")
        else:
            print("[INFO] Không có bài viết nào cần xóa. Cơ sở dữ liệu đã được đồng bộ.")

        # BƯỚC 4: Cập nhật và Thêm bài viết mới
        print("\n--- GIAI ĐOẠN 3: CẬP NHẬT DỮ LIỆU MỚI ---")
        update_count = 0
        for article_info in articles_from_feed:
            url = article_info['url']
            feed_published_date = datetime.fromisoformat(article_info['published'].replace('Z', '+00:00'))
            if url not in db_articles or feed_published_date > db_articles[url]:
                update_count += 1
                print(f"  - Đang xử lý bài viết mới/cập nhật: '{article_info['title']}'")
                content = scrape_article_content(url)
                if content:
                    upsert_article_rpc(supabase, {
                        'title': article_info['title'],
                        'url': url,
                        'content': content,
                        'published': article_info['published']
                    })
                time.sleep(1)
        
        if update_count == 0:
            print("[INFO] Không có bài viết mới hoặc cập nhật nào cần xử lý.")
        else:
            print(f"[SUCCESS] Đã xử lý {update_count} bài viết mới/cập nhật.")

    except Exception as e:
        print(f"\n[!!!] LỖI NGHIÊM TRỌNG TRONG QUÁ TRÌNH SCRAPE: {e}")
    finally:
        print("\n--- HOÀN TẤT TOÀN BỘ QUÁ TRÌNH SCRAPE ---")

if __name__ == "__main__":
    main_scraper()
