import os
from flask import Flask, request, jsonify, make_response
import psycopg2
from psycopg2.extras import DictCursor
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def get_db_connection():
    """Kết nối tới cơ sở dữ liệu PostgreSQL."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set.")
    conn = psycopg2.connect(database_url)
    return conn

@app.route('/api/search', methods=['GET'])
def search_articles():
    query = request.args.get('q', '').strip()
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        if not query:
            # Trả về các bài viết gần đây nhất nếu không có query
            cursor.execute(
                """
                SELECT id, title, url, content, published_date
                FROM articles ORDER BY published_date DESC LIMIT 20;
                """
            )
        else:
            # Tìm kiếm nếu có query
            search_tsquery = ' & '.join([f"'{word}'" for word in query.split()])
            cursor.execute(
                """
                SELECT
                    id, title, url, content,
                    ts_headline('vietnamese', content, to_tsquery('vietnamese', unaccent(%s)),
                                'StartSel=<strong>, StopSel=</strong>, MaxWords=50, MinWords=20, ShortWord=3, HighlightAll=FALSE') as content_snippet,
                    published_date,
                    ts_rank(tsv, to_tsquery('vietnamese', unaccent(%s))) as rank
                FROM articles
                WHERE tsv @@ to_tsquery('vietnamese', unaccent(%s))
                ORDER BY rank DESC, published_date DESC LIMIT 20;
                """,
                (search_tsquery, search_tsquery, search_tsquery)
            )

        results = cursor.fetchall()
        articles = []
        for row in results:
            article = dict(row)
            if 'published_date' in article and article['published_date']:
                article['published_date'] = article['published_date'].isoformat()
            articles.append(article)
        
        # *** BẮT ĐẦU SỬA LỖI: THÊM HEADER CHỐNG CACHE ***
        response = make_response(jsonify(articles))
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
        # *** KẾT THÚC SỬA LỖI ***

    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
        return jsonify({"error": "Lỗi máy chủ"}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
