"""
News/social collector package for the ingestion service.
"""

from services.ingestion.sources.base import (
    DEFAULT_NEWS_SYMBOLS,
    NewsCollector,
    NewsFetcher,
    coerce_datetime,
    extract_symbols,
    hash_news_event,
    normalize_symbols,
)
from services.ingestion.sources.rest import RestSocialCollector, SocialFeed
from services.ingestion.sources.rss import RssCollector, RssFeed
from services.ingestion.sources.service import NewsPollingMetrics, NewsPollingService

__all__ = [
    "DEFAULT_NEWS_SYMBOLS",
    "NewsCollector",
    "NewsFetcher",
    "coerce_datetime",
    "extract_symbols",
    "hash_news_event",
    "normalize_symbols",
    "RssFeed",
    "RssCollector",
    "SocialFeed",
    "RestSocialCollector",
    "NewsPollingMetrics",
    "NewsPollingService",
]

