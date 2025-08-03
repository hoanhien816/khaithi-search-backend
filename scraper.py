def main_scraper():
    """Hàm chính để chạy toàn bộ quá trình scraper, bao gồm cả việc xóa bài viết cũ."""
    try:
        supabase = get_supabase_client()

        # --- BƯỚC 1: Lấy TOÀN BỘ URL bài viết từ blog gốc ---
        print("Bắt đầu lấy danh sách bài viết từ blog gốc...")
        articles_from_feed = get_article_urls_from_feed()
        if not articles_from_feed:
            print("Không có bài viết nào từ feed. Dừng quá trình.")
            return
        # Chuyển thành một set để tra cứu nhanh hơn
        source_urls = {article['url'] for article in articles_from_feed}
        print(f"Đã tìm thấy {len(source_urls)} URL hợp lệ từ blog.")

        # --- BƯỚC 2: Lấy TOÀN BỘ URL hiện có trong cơ sở dữ liệu ---
        print("\nBắt đầu lấy danh sách bài viết từ cơ sở dữ liệu...")
        existing_articles_data = supabase.table('articles').select('url, published_date').execute().data
        db_articles = {item['url']: datetime.fromisoformat(item['published_date']) for item in existing_articles_data}
        db_urls = set(db_articles.keys())
        print(f"Cơ sở dữ liệu hiện có {len(db_urls)} bài viết.")

        # --- BƯỚC 3: Xác định và XÓA các bài viết không còn tồn tại trên blog ---
        urls_to_delete = db_urls - source_urls
        if urls_to_delete:
            print(f"\nTìm thấy {len(urls_to_delete)} bài viết cần xóa khỏi cơ sở dữ liệu.")
            # Chuyển set thành list để dùng trong câu lệnh `in_`
            urls_to_delete_list = list(urls_to_delete)
            try:
                # Xóa các bài viết theo từng khối 100 để tránh URL quá dài
                chunk_size = 100
                for i in range(0, len(urls_to_delete_list), chunk_size):
                    chunk = urls_to_delete_list[i:i + chunk_size]
                    print(f"Đang xóa khối {i//chunk_size + 1}...")
                    supabase.table('articles').delete().in_('url', chunk).execute()
                print("Đã xóa thành công các bài viết cũ.")
            except Exception as e:
                print(f"Lỗi khi xóa bài viết cũ: {e}")
        else:
            print("\nKhông có bài viết nào cần xóa. Cơ sở dữ liệu đã được đồng bộ.")

        # --- BƯỚC 4: Cập nhật và Thêm các bài viết mới (Logic UPSERT như cũ) ---
        print("\nBắt đầu quá trình thêm mới và cập nhật bài viết...")
        total_articles = len(articles_from_feed)
        for index, article_info in enumerate(articles_from_feed):
            url = article_info['url']
            title = article_info['title']
            feed_published_date = datetime.fromisoformat(article_info['published'].replace('Z', '+00:00'))

            # Chỉ scrape và upsert nếu bài viết là mới, hoặc đã có nhưng ngày cập nhật trên feed mới hơn
            # Điều này giúp tiết kiệm thời gian, không cần scrape lại những bài không thay đổi.
            if url not in db_articles or feed_published_date > db_articles[url]:
                print(f"({index + 1}/{total_articles}) Đang xử lý: '{title}'")
                content = scrape_article_content(url)
                if content:
                    upsert_article_rpc(supabase, {
                        'title': title,
                        'url': url,
                        'content': content,
                        'published': article_info['published']
                    })
                time.sleep(1) # Giữ khoảng nghỉ để tránh quá tải
            else:
                # Bỏ qua vì bài viết đã tồn tại và không có cập nhật
                # print(f"({index + 1}/{total_articles}) Bỏ qua (đã tồn tại và không đổi): '{title}'")
                pass


    except Exception as e:
        print(f"Lỗi nghiêm trọng trong quá trình scrape: {e}")
    finally:
        print("\nHoàn tất quá trình scrape.")

if __name__ == "__main__":
    main_scraper()
