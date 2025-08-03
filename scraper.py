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

def get_article_urls_from_feed():
    """Hàm giả định lấy URL từ blog gốc. Cần thay thế bằng logic thực tế của bạn."""
    print("[DEBUG] Bắt đầu lấy URL từ blog gốc...")
    # Thêm logic của bạn vào đây
    # Ví dụ: return [{'url': 'https://example.com/bai-viet-1', 'published_date': '2025-01-01T00:00:00Z'}]
    return [] 

# Giữ nguyên các hàm `scrape_article_content` và `upsert_article_rpc` của bạn

# Hàm parse_db_datetime và main_scraper đã được bạn cung cấp.
def parse_db_datetime(dt_str: str) -> datetime:
    """Hàm chuyển đổi chuỗi ngày tháng từ DB một cách linh hoạt, xử lý mọi trường hợp."""
    if dt_str.endswith('+00:00'): dt_str = dt_str[:-3] + dt_str[-2:]
    try: return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError: return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S%z')

def main_scraper():
    """Hàm chính để chạy toàn bộ quá trình scraper, bao gồm cả việc xóa và xác minh."""
    print("[DEBUG] Bắt đầu main_scraper...") # Thêm dòng này để kiểm tra
    try:
        supabase = get_supabase_client()
        print("[DEBUG] Kết nối Supabase thành công.") # Thêm dòng này
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
        # (Giữ nguyên logic cập nhật)

    except Exception as e:
        print(f"\n[!!!] LỖI NGHIÊM TRỌNG TRONG QUÁ TRÌNH SCRAPE: {e}")
    finally:
        print("\n--- HOÀN TẤT TOÀN BỘ QUÁ TRÌNH SCRAPE ---")

# (Giữ nguyên dòng này)
if __name__ == '__main__':
    main_scraper()
