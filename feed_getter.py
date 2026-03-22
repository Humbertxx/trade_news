import os
import queue
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Iterable, Optional

import feedparser
import pandas as pd

from alpaca.data.live import NewsDataStream
import config


class FeedGetter:
    def __init__(self, 
                tickers: Optional[Iterable[str] | str] = None, 
                rss_source: Optional[Iterable[str] | str] = None, 
                max_seen_headlines: int = None,
                process_batch_callback: Optional[Callable] = None
                ) -> None:
        
        self.tickers = tickers
        self.rss_sources = rss_source # takes key values from previous code calls
        self.news_queue = queue.Queue()
        self.seen_headlines = deque(maxlen=max_seen_headlines)
        self.process_batch_callback = process_batch_callback
        self.alpaca_key = os.getenv("ALPACA_KEY")
        self.alpaca_secret = os.getenv("ALPACA_SECRET")

    def fetch_ticker_rss(self, ticker: str) -> list[dict]:
        articles = []
        configured_sources = config.RSS_PREFER_NEWS
        for source in self.rss_sources:
            template = configured_sources.get(source)
            if not template:
                print(f"RSS source '{source}' is not configured. Skipping.")
                continue
            try:
                print(f"Fetching RSS for {ticker} from {source}")
                url = template.format(ticker=ticker)
                feed = feedparser.parse(url)

                for entry in getattr(feed, "entries", []):
                    description = getattr(entry, "summary", "") or getattr(entry, "description", "")
                    published = getattr(entry, "published", datetime.now())
                    news_entry = {
                        "ticker": ticker,
                        "source": source,
                        "title": entry.title,
                        "description": description,
                        "link": entry.link,
                        "published": published,
                    }
                    articles.append(news_entry)
            except Exception as exc:
                print(f"Error fetching RSS for {ticker} from {source}: {exc}")
        return articles

    def enqueue_articles(self, articles: list[dict]) -> None:
        for article in articles:
            title = article.get("title")
            if not title or title in self.seen_headlines:
                continue

            self.seen_headlines.append(title)
            print(f"Enqueuing RSS article: {title}")
            self.news_queue.put(article)

    def rss_market_news(self, timeout: int = 5, poll_interval: int = 5) -> None:
        if not self.tickers:
            raise ValueError("No tickers configured. Call 'set_tickers' first.")
 
        max_workers = max(1, min(len(self.tickers), 16))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                futures = [executor.submit(self.fetch_ticker_rss, ticker) for ticker in self.tickers]
                for future in futures:
                    try:
                        articles = future.result(timeout=timeout)
                        self.enqueue_articles(articles)
                    except Exception as exc:
                        print(f"Error processing article batch: {exc}")

                time.sleep(poll_interval)

    def worker_logic(self, timeout: int = 5) -> None:
        buffer = []
        while True:
            try:
                item = self.news_queue.get(timeout=timeout)
                buffer.append(item)
                print(f"worker received item. buffer size: {len(buffer)}")
                self.news_queue.task_done()
            except queue.Empty:
                if not buffer:
                    continue

                df_batch = pd.DataFrame(buffer)
                try:
                    self.run_processor(df_batch)
                except Exception as exc:
                    print(f"Error in pipeline: {exc}")
                finally:
                    buffer = []

    def start_alpaca_stream(self) -> None:
        symbols = self.tickers
        key = self.alpaca_key
        secret = self.alpaca_secret
        if not key or not secret:
            raise ValueError("Alpaca credentials are missing.")
        
        async def alpaca_handler(news):
            relevant_tickers = [symbol for symbol in news.symbols if symbol in symbols]
            if not relevant_tickers:
                return

            if news.headline in self.seen_headlines:
                return
            self.seen_headlines.append(news.headline)
            packet = {
                "source": "Benzinga",
                "title": news.headline,
                "ticker": relevant_tickers[0],
                "published": news.created_at,
            }
            self.news_queue.put(packet)
            print(f"Enqueuing Alpaca article: {packet['title']}")

        stream_client = NewsDataStream(key, secret)
        stream_client.subscribe_news(alpaca_handler, *symbols)
        stream_client.run()

