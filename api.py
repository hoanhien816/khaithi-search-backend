import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import DictCursor # Để lấy kết quả dưới dạng dictionary
from flask_cors import CORS # Để cho phép frontend truy cập API

app = Flask(__name__)
CORS(app) # Cho phép tất cả các nguồn gốc (origins) để frontend có thể gọi API

def get_db_connection():
    """Kết nối tới cơ sở dữ liệu PostgreSQL."""
    # Lấy chuỗi kết nối từ biến môi trường DATABASE_URL
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set.")
    conn = psycopg2.connect(database_url) # Sử dụng trực tiếp chuỗi URL
    return conn

@app.route('/api/search', methods=['GET'])
def search_articles():
    query = request.args.get('q', '').strip()
    if not query:
        # Trả về các bài viết gần đây nhất nếu không có query
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=DictCursor)
            cursor.execute(
                """
                SELECT id, title, url, content, published_date
                FROM articles
                ORDER BY published_date DESC
                LIMIT 20;
                """
            )
            results = cursor.fetchall()
            articles = []
            for row in results:
                article = dict(row)
                if 'published_date' in article and article['published_date']:
                    article['published_date'] = article['published_date'].isoformat()
                articles.append(article)
            return jsonify(articles)
        except psycopg2.Error as e:
            print(f"Lỗi DB khi lấy bài viết gần đây: {e}")
            return jsonify({"error": "Lỗi cơ sở dữ liệu"}), 500
        except Exception as e:
            print(f"Lỗi không xác định: {e}")
            return jsonify({"error": "Lỗi máy chủ"}), 500
        finally:
            if conn:
                conn.close()


    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor) # Sử dụng DictCursor để lấy kết quả dạng dict

        # Xử lý query để tạo tsquery:
        # - unaccent: bỏ dấu tiếng Việt
        # - replace ' ' with '&': coi khoảng trắng là toán tử AND
        # - escape: bảo vệ khỏi các ký tự đặc biệt trong tsquery
        # Sử dụng plainto_tsquery để xử lý các từ khóa đơn giản mà không cần toán tử
        search_tsquery = ' & '.join([f"'{word}'" for word in query.split()])
        
        # Sử dụng to_tsquery để xử lý query và tsvector để tìm kiếm
        # Sắp xếp theo mức độ liên quan (rank) và sau đó theo ngày xuất bản
        cursor.execute(
            """
            SELECT
                id,
                title,
                url,
                content,
                ts_headline('vietnamese', content, to_tsquery('vietnamese', unaccent(%s)), 'StartSel=<strong>, StopSel=</strong>, MaxWords=50, MinWords=20, ShortWord=3, HighlightAll=FALSE') as content_snippet,
                published_date,
                ts_rank(tsv, to_tsquery('vietnamese', unaccent(%s))) as rank
            FROM articles
            WHERE tsv @@ to_tsquery('vietnamese', unaccent(%s))
            ORDER BY rank DESC, published_date DESC
            LIMIT 20; -- Giới hạn 20 kết quả mỗi lần
            """,
            (search_tsquery, search_tsquery, search_tsquery)
        )
        results = cursor.fetchall()
        
        # Chuyển đổi kết quả từ DictRow sang dictionary chuẩn để jsonify
        articles = []
        for row in results:
            article = dict(row)
            # Chuyển đổi datetime object sang string để jsonify
            if 'published_date' in article and article['published_date']:
                article['published_date'] = article['published_date'].isoformat()
            articles.append(article)

        return jsonify(articles)

    except psycopg2.Error as e:
        print(f"Lỗi DB khi tìm kiếm: {e}")
        return jsonify({"error": "Lỗi cơ sở dữ liệu"}), 500
    except Exception as e:
        print(f"Lỗi không xác định: {e}")
        return jsonify({"error": "Lỗi máy chủ"}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000) # Chạy trên cổng 5000
