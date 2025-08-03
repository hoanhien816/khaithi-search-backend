import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
import json
from supabase import create_client, Client

# *** THÊM CÁC HÀM CÒN THIẾU TỪ BẢN GỐC ĐỂ CHẠY ĐƯỢC ***
# Các hàm này cần có trong file của bạn. Tôi sẽ thêm vào đây để đảm bảo mã chạy hoàn chỉnh
def get_supabase_client():
    """Kết nối tới Supabase và trả về client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    # Các dòng print để gỡ lỗi
    print(f"[DEBUG] SUPABASE_URL đã được đọc: {bool(url)}")
    print(f"[DEBUG] SUPABASE_KEY đã được đọc: {bool(key)}")
    if not url or not key:
        raise ValueError("Supabase URL hoặc Key không được cấu hình.")
    return create_client(url, key)

def scrape_article_content(url):
    """Giả định hàm scrape nội dung, cần logic thực tế của bạn."""
    print(f"[DEBUG] Scraping nội dung từ URL: {url}")
    # Thêm logic của bạn vào đây
    # Ví dụ:
    # response = requests.get(url)
    # soup = BeautifulSoup(response.text, 'html.parser')
    # content = soup.find('div', class_='post-body').get_text()
    # return "Nội dung bài viết giả lập."
    return "Nội dung bài viết giả lập."

def upsert_article_rpc(supabase, article_data):
    """Giả định hàm upsert, cần logic thực tế của bạn."""
    print(f"[DEBUG] Upserting bài viết: {article_data['title']}")
    # Thêm logic của bạn vào đây
    # Ví dụ:
    # supabase.rpc('upsert_article', article_data).execute()
    pass

def get_article_urls_from_feed():
    """
    Lấy danh sách URL bài viết và ngày xuất bản từ nguồn cấp dữ liệu (RSS/Atom feed)
    của blog gốc.
    """
    BLOG_FEED_URL = "https://hoanhien.vn/feeds/posts/default?alt=json"  # URL feed JSON của blog Blogger
    print(f"[DEBUG] Bắt đầu lấy URL từ blog gốc tại: {BLOG_FEED_URL}")
    
    try:
        response = requests.get(BLOG_FEED_URL)
        response.raise_for_status()  # Ném lỗi nếu yêu cầu không thành công

        data = response.json()
        articles = []
        
        # Lặp qua các bài viết trong feed JSON
        for entry in data['feed']['entry']:
            url = next(link['href'] for link in entry['link'] if link['rel'] == 'alternate')
            published_date_str = entry['published']['$t']
            
            articles.append({
                'url': url,
                'published_date': published_date_str
            })

        print(f"[DEBUG] Đã lấy được {len(articles)} URL từ feed.")
        return articles

    except requests.exceptions.RequestException as e:
        print(f"[!!!] Lỗi khi lấy dữ liệu từ blog gốc: {e}")
        return []
    except KeyError as e:
        print(f"[!!!] Lỗi phân tích cấu trúc feed JSON: {e}")
        return []

def parse_db_datetime(dt_str: str) -> datetime:
    """Hàm chuyển đổi chuỗi ngày tháng từ DB một cách linh hoạt, xử lý mọi trường hợp."""
    if dt_str.endswith('+00:00'): dt_str = dt_str[:-3] + dt_str[-2:]
    try: return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError: return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S%z')

def main_scraper():
    """Hàm chính để chạy toàn bộ quá trình scraper, bao gồm cả việc xóa và xác minh."""
    print("[DEBUG] Bắt đầu main_scraper...")
    try:
        supabase = get_supabase_client()
        print("[DEBUG] Kết nối Supabase thành công.")
        
        # BƯỚC 1: Lấy URL từ blog gốc
        print("--- GIAI ĐOẠN 1: THU THẬP DỮ LIỆU ---")
        articles_from_feed = get_article_urls_from_feed()
        if not articles_from_feed: 
            print("[INFO] Không tìm thấy URL nào từ blog gốc, dừng scraper.")
            return
        source_urls = {article['url'] for article in articles_from_feed}
        print(f"[INFO] Tổng số URL duy nhất từ blog gốc: {len(source_urls)}")

        # BƯỚC 2: Lấy URL từ cơ sở dữ liệu
        print("\n--- GIAI ĐOẠN 2: SO SÁNH VÀ DỌN DẸP ---")
        response = supabase.table('articles').select('url, published_date').execute()
        db_articles = {item['url']: parse_db_datetime(item['published_date']) for item in response.data}
        db_urls = set(db_articles.keys())
        print(f"[INFO] Tổng số URL trong cơ sở dữ liệu: {len(db_urls)}")

        # BƯỚC 3: Xác định và XÓA bài viết cũ
        urls_to_delete = db_urls - source_urls
        if urls_to_delete:
            print(f"[ACTION] Tìm thấy {len(urls_to_delete)} bài viết cần xóa.")
            urls_to_delete_list = list(urls_to_delete)
            chunk_size = 100
            for i in range(0, len(urls_to_delete_list), chunk_size):
                chunk = urls_to_delete_list[i:i + chunk_size]
                supabase.table('articles').delete().in_('url', chunk).execute()
            print("[SUCCESS] Đã gửi lệnh xóa cho tất cả các bài viết cũ.")

            # *** BƯỚC 3.1: XÁC MINH VIỆC XÓA ***
            print("\n[VERIFY] Bắt đầu xác minh lại việc xóa...")
            time.sleep(5) # Chờ 5 giây để DB có thời gian cập nhật
            remaining_response = supabase.table('articles').select('url').in_('url', urls_to_delete_list).execute()
            if not remaining_response.data:
                print("[VERIFY-SUCCESS] OK! Tất cả các bài viết đã được xóa thành công khỏi cơ sở dữ liệu.")
            else:
                print(f"[VERIFY-FAIL] LỖI! Vẫn còn {len(remaining_response.data)} bài viết chưa được xóa. Vui lòng kiểm tra quyền của SUPABASE_KEY.")

        else:
            print("[INFO] Không có bài viết nào cần xóa.")

        # BƯỚC 4: Cập nhật và Thêm bài viết mới
        print("\n--- GIAI ĐOẠN 3: CẬP NHẬT DỮ LIỆU MỚI ---")
        # Giả định logic cập nhật
        # articles_to_upsert = # Logic để lấy các bài viết cần cập nhật
        # for article in articles_to_upsert:
        #     article_data = scrape_article_content(article['url'])
        #     upsert_article_rpc(supabase, article_data)
        # print("[SUCCESS] Đã cập nhật xong dữ liệu mới.")

    except Exception as e:
        print(f"\n[!!!] LỖI NGHIÊM TRỌNG TRONG QUÁ TRÌNH SCRAPE: {e}")
    finally:
        print("\n--- HOÀN TẤT TOÀN BỘ QUÁ TRÌNH SCRAPE ---")

# (Giữ nguyên dòng này)
if __name__ == '__main__':
    main_scraper()
