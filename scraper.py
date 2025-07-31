import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
import json
from supabase import create_client, Client

# --- Cấu hình ---
# Lấy thông tin kết nối từ biến môi trường của GitHub Actions hoặc môi trường cục bộ
# Bắt buộc phải có: SUPABASE_URL và SUPABASE_KEY (sử dụng service_role key)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

BASE_URL = "https://timkhaithi.pmtl.site"
RSS_FEED_URL = f"{BASE_URL}/feeds/posts/default?orderby=published&alt=json-in-script&max-results=500"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_supabase_client():
    """Khởi tạo và trả về Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise EnvironmentError("Vui lòng đặt biến môi trường SUPABASE_URL và SUPABASE_KEY.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_article_urls_from_feed():
    """Lấy danh sách URL bài viết từ RSS feed của Blogger một cách an toàn."""
    print(f"Đang lấy URL từ RSS Feed: {RSS_FEED_URL}")
    try:
        response = requests.get(RSS_FEED_URL, headers=HEADERS)
        response.raise_for_status()
        
        # AN TOÀN HƠN: Trích xuất JSON từ chuỗi JSONP mà không dùng eval()
        jsonp_data = response.text
        # Tìm vị trí bắt đầu và kết thúc của đối tượng JSON
        start_index = jsonp_data.find('(')
        end_index = jsonp_data.rfind(')')
        
        if start_index == -1 or end_index == -1:
            print("Không thể phân tích phản hồi JSONP.")
            return []

        json_str = jsonp_data[start_index + 1 : end_index]
        json_data = json.loads(json_str)

        urls = []
        if 'feed' in json_data and 'entry' in json_data['feed']:
            for entry in json_data['feed']['entry']:
                post_url = next((link['href'] for link in entry.get('link', []) if link.get('rel') == 'alternate'), None)
                if post_url:
                    urls.append({
                        'url': post_url,
                        'title': entry.get('title', {}).get('$t', ''),
                        'published': entry.get('published', {}).get('$t', '')
                    })
        print(f"Tìm thấy {len(urls)} URL bài viết từ RSS Feed.")
        return urls
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy RSS Feed: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Lỗi khi giải mã JSON: {e}")
        return []
    except Exception as e:
        print(f"Lỗi không xác định khi xử lý feed: {e}")
        return []

def scrape_article_content(url):
    """Tải và trích xuất nội dung chính của một bài viết."""
    print(f"Đang scrape nội dung từ: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # === THAY ĐỔI CHÍNH Ở ĐÂY ===
        # Selector đã được đơn giản hóa để chỉ tìm 'div' có class 'post-body'
        # vì class 'entry-content' không phải lúc nào cũng có.
        content_div = soup.find('div', class_='post-body')
        
        if not content_div:
            print(f"Không tìm thấy nội dung cho URL: {url}. Vui lòng kiểm tra selector.")
            return None

        # Loại bỏ các thẻ không mong muốn
        for unwanted_tag in content_div.find_all(['script', 'style', 'ins', 'iframe']):
            unwanted_tag.decompose()

        content_text = content_div.get_text(separator='\n', strip=True)
        content_text = re.sub(r'\n\s*\n', '\n\n', content_text).strip()
        return content_text
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải trang {url}: {e}")
        return None
    except Exception as e:
        print(f"Lỗi khi phân tích HTML cho {url}: {e}")
        return None

def upsert_article_rpc(supabase_client: Client, article_data: dict):
    """
    Gọi một PostgreSQL Function (RPC) trong Supabase để chèn hoặc cập nhật bài viết.
    Đây là cách hiệu quả và an toàn nhất để xử lý logic phức tạp.
    """
    try:
        # Chuyển đổi ngày tháng sang định dạng ISO 8601 chuẩn
        published_date_obj = datetime.fromisoformat(article_data['published'].replace('Z', '+00:00'))
        
        params = {
            'p_title': article_data['title'],
            'p_url': article_data['url'],
            'p_content': article_data['content'],
            'p_published_date': published_date_obj.isoformat()
        }
        
        # Gọi function tên là 'upsert_article' trong database của bạn
        result = supabase_client.rpc('upsert_article', params).execute()
        
        print(f"Đã xử lý bài viết '{article_data['title']}' thành công.")
        return result
    except Exception as e:
        print(f"Lỗi RPC khi xử lý bài viết '{article_data['title']}': {e}")
        return None

def main_scraper():
    """Hàm chính để chạy toàn bộ quá trình scraper."""
    try:
        supabase = get_supabase_client()
        articles_from_feed = get_article_urls_from_feed()

        if not articles_from_feed:
            print("Không có bài viết nào từ feed để xử lý.")
            return

        # Lấy danh sách URL đã có trong DB để so sánh
        existing_urls_data = supabase.table('articles').select('url, published_date').execute().data
        existing_articles = {item['url']: datetime.fromisoformat(item['published_date']) for item in existing_urls_data}
        
        for article_info in articles_from_feed:
            url = article_info['url']
            title = article_info['title']
            feed_published_date = datetime.fromisoformat(article_info['published'].replace('Z', '+00:00'))

            # So sánh ngày xuất bản để quyết định có scrape lại không
            if url in existing_articles and feed_published_date <= existing_articles[url]:
                print(f"Bài viết '{title}' đã tồn tại và không có cập nhật mới. Bỏ qua.")
                continue
            
            content = scrape_article_content(url)
            if content:
                upsert_article_rpc(supabase, {
                    'title': title,
                    'url': url,
                    'content': content,
                    'published': article_info['published']
                })
            
            time.sleep(1) # Giữ khoảng cách giữa các request

    except Exception as e:
        print(f"Lỗi nghiêm trọng trong quá trình scrape: {e}")
    finally:
        print("Hoàn tất quá trình scrape.")

if __name__ == "__main__":
    main_scraper()
