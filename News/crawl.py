# import feedparser
# import requests
# import mysql.connector
# from datetime import datetime, timezone
# from nltk.sentiment.vader import SentimentIntensityAnalyzer
# import nltk

# # =========================
# # NLTK SETUP
# # =========================
# nltk.download("vader_lexicon", quiet=True)
# sid = SentimentIntensityAnalyzer()

# # =========================
# # RSS SOURCES
# # =========================
# COINDESK_RSS = "https://www.coindesk.com/arc/outboundfeeds/rss/"
# NEWSBTC_RSS = "https://www.newsbtc.com/feed/"

# # =========================
# # MYSQL CONFIG
# # =========================
# DB_CONFIG = {
#     "host": "localhost",
#     "user": "spark_user",
#     "password": "spark123",
#     "database": "crypto_dw"
# }

# # =========================
# # FETCH RSS
# # =========================
# def fetch_rss(url):
#     feed = feedparser.parse(url)
#     return feed.entries

# # =========================
# # SENTIMENT
# # =========================
# def sentiment_score(text: str) -> float:
#     return sid.polarity_scores(text)["compound"]

# # =========================
# # TAG HANDLING
# # =========================
# def get_or_create_tag(cursor, tag_name):
#     if not tag_name:
#         return None

#     cursor.execute(
#         "SELECT tag_id FROM tag_dim WHERE tag_name = %s",
#         (tag_name,)
#     )
#     row = cursor.fetchone()
#     if row:
#         return row[0]

#     cursor.execute(
#         "INSERT INTO tag_dim(tag_name) VALUES (%s)",
#         (tag_name,)
#     )
#     return cursor.lastrowid

# # =========================
# # MAIN
# # =========================
# def main():
#     conn = mysql.connector.connect(**DB_CONFIG)
#     cursor = conn.cursor()

#     articles = []
#     articles.extend(fetch_rss(COINDESK_RSS))
#     articles.extend(fetch_rss(NEWSBTC_RSS))

#     print(f"Found {len(articles)} articles")

#     for item in articles:
#         try:
#             title = item.get("title", "").strip()
#             url = item.get("link", "").strip()

#             if not title or not url:
#                 continue

#             # published date
#             if hasattr(item, "published_parsed") and item.published_parsed:
#                 created_date = datetime(
#                     *item.published_parsed[:6],
#                     tzinfo=timezone.utc
#                 )
#             else:
#                 created_date = datetime.now(timezone.utc)

#             # tag (category)
#             tag_name = None
#             if "tags" in item and len(item.tags) > 0:
#                 tag_name = item.tags[0]["term"]

#             score = sentiment_score(title)

#             tag_id = get_or_create_tag(cursor, tag_name)

#             cursor.execute(
#                 """
#                 INSERT IGNORE INTO news_fact
#                 (title, url, sentiment_score, created_date, view_number, tag_id)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#                 """,
#                 (
#                     title,
#                     url,
#                     score,
#                     created_date,
#                     None,
#                     tag_id
#                 )
#             )

#         except Exception as e:
#             print("Skip article due to error:", e)

#     conn.commit()
#     cursor.close()
#     conn.close()

#     print("✅ Crawl & Insert Done")

# # =========================
# if __name__ == "__main__":
#     main()

import feedparser
import mysql.connector
from datetime import datetime, timezone
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# =========================
# NLTK SETUP
# =========================
nltk.download("vader_lexicon", quiet=True)
sid = SentimentIntensityAnalyzer()

# =========================
# RSS SOURCES (MỞ RỘNG)
# =========================
RSS_SOURCES = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "newsbtc": "https://www.newsbtc.com/feed/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "bitcoinmagazine": "https://bitcoinmagazine.com/.rss/full/",
    "decrypt": "https://decrypt.co/feed"
}

# =========================
# MYSQL CONFIG (GIỮ NGUYÊN)
# =========================
DB_CONFIG = {
    "host": "localhost",
    "user": "spark_user",
    "password": "spark123",
    "database": "crypto_dw"
}

# =========================
# FETCH RSS
# =========================
def fetch_rss(url):
    feed = feedparser.parse(url)
    return feed.entries if feed and "entries" in feed else []

# =========================
# SENTIMENT
# =========================
def sentiment_score(text: str) -> float:
    return sid.polarity_scores(text)["compound"]

# =========================
# TAG HANDLING
# =========================
def get_or_create_tag(cursor, tag_name):
    if not tag_name:
        return None

    cursor.execute(
        "SELECT tag_id FROM tag_dim WHERE tag_name = %s",
        (tag_name,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        "INSERT INTO tag_dim(tag_name) VALUES (%s)",
        (tag_name,)
    )
    return cursor.lastrowid

# =========================
# MAIN
# =========================
def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    total_articles = 0
    inserted = 0

    for source, url in RSS_SOURCES.items():
        articles = fetch_rss(url)
        print(f"[{source}] Found {len(articles)} articles")
        total_articles += len(articles)

        for item in articles:
            try:
                title = item.get("title", "").strip()
                link = item.get("link", "").strip()

                if not title or not link:
                    continue

                # Published date
                if hasattr(item, "published_parsed") and item.published_parsed:
                    created_date = datetime(
                        *item.published_parsed[:6],
                        tzinfo=timezone.utc
                    )
                else:
                    created_date = datetime.now(timezone.utc)

                # Tag (category)
                tag_name = None
                if "tags" in item and item.tags:
                    tag_name = item.tags[0].get("term")

                score = sentiment_score(title)
                tag_id = get_or_create_tag(cursor, tag_name)

                cursor.execute(
                    """
                    INSERT IGNORE INTO news_fact
                    (title, url, sentiment_score, created_date, view_number, tag_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        title,
                        link,
                        score,
                        created_date,
                        None,
                        tag_id
                    )
                )

                if cursor.rowcount > 0:
                    inserted += 1

            except Exception as e:
                print("Skip article due to error:", e)

    conn.commit()
    cursor.close()
    conn.close()

    print("===================================")
    print(f"Total fetched articles : {total_articles}")
    print(f"Inserted new articles : {inserted}")
    print("✅ Crawl & Insert Done")

# =========================
if __name__ == "__main__":
    main()
