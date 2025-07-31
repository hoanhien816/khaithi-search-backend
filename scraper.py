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
    """
    Lấy TOÀN BỘ URL bài viết từ RSS feed, tự động xử lý phân trang
    để lấy hết tất cả các bài viết.
    """
    print("Bắt đầu lấy toàn bộ URL từ RSS Feed, có xử lý phân trang...")
    all_urls = []
    start_index = 1
    max_results = 500  # Số lượng tối đa cho mỗi yêu cầu

    while True:
        # Thêm tham số start-index để lấy các trang tiếp theo
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

            # Nếu không còn bài viết nào được trả về, nghĩa là đã hết
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

            # Cập nhật start_index cho lần lặp tiếp theo
            start_index += len(entries) # Cập nhật dựa trên số lượng thực tế trả về

            time.sleep(1)  # Tạm dừng 1 giây để tránh gây quá tải cho server

        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi lấy RSS Feed trang: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"Lỗi khi giải mã JSON: {e}")
            break
        except Exception as e:
            print(f"Lỗi không xác định khi xử lý feed: {e}")
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
        content_text = re.sub(r'\n\s*\n', '\n\n', content_text).strip()
        return content_text
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải trang {url}: {e}")
        return None
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

        existing_urls_data = supabase.table('articles').select('url, published_date').execute().data
        existing_articles = {item['url']: datetime.fromisoformat(item['published_date']) for item in existing_urls_data}
        
        for article_info in articles_from_feed:
            url = article_info['url']
            title = article_info['title']
            feed_published_date = datetime.fromisoformat(article_info['published'].replace('Z', '+00:00'))

            if url in existing_articles and feed_published_date <= existing_articles[url]:
                # Bỏ qua logic này trong lần chạy đầu để đảm bảo tất cả bài viết được cập nhật
                # print(f"Bài viết '{title}' đã tồn tại và không có cập nhật mới. Bỏ qua.")
                # continue
                pass # Tạm thời cho phép cập nhật lại tất cả
            
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
        print("Hoàn tất quá trình scrape.")

if __name__ == "__main__":
    main_scraper()
