import wmill
import feedparser
from datetime import datetime, timedelta
from pymongo import MongoClient
from typing import List, Dict
import hashlib


def check_rss_feeds(feeds: List[str], lookback_hours: int = 1) -> List[Dict]:
    client = MongoClient(wmill.get_variable("u/oudev2/mongo_uri_theanh"))  # MONGO_URI
    db = client.financial_news  # tên db
    new_articles = []
    cutoff_time = datetime.utcnow() - timedelta(
        hours=lookback_hours
    )  # time hiện tại - 1
    for feed_url in feeds:
        try:
            feed = feedparser.parse(feed_url)  # parse feed XML về JSON
            for entry in feed.entries:
                pub_date = datetime(
                    *entry.published_parsed[:6]
                )  # parse về datetime -> ngày đăng bài

                if pub_date < cutoff_time:  # Cũ hơn 1 giờ trở lại bỏ .
                    continue
                content_hash = hashlib.sha256(
                    entry.link.encode("utf-8")  # hash tránh duplicate link entry
                ).hexdigest()
                # check content lại trong Mongodb có exists
                existing = db.raw_documents.find_one(
                    {"metadata.content_hash": content_hash}
                )

                if existing:
                    continue  # bỏ qua
                # Lưu tạm vào new_articles
                new_articles.append(
                    {
                        "url": entry.link,
                        "title": entry.title,
                        "summary": entry.get("summary", ""),
                        "published_at": pub_date,
                        "source_feed": feed_url,
                        "content_hash": content_hash,
                    }
                )
        except Exception as e:
            print(f"Error parsing feed {feed_url}: {str(e)}")
            continue
    return new_articles


def main(feeds: list = None) -> dict:
    if feeds is None:
        # Mặc định nếu không truyền gì
        feeds = [
            "https://finance.yahoo.com/news/rss",
            # "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        ]

    articles = check_rss_feeds(feeds)

    return {
        "new_articles": articles,
        "count": len(articles),
        "timestamp": datetime.utcnow().isoformat(),
    }
