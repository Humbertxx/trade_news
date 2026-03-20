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
from normalization import clean_text

from sentiment import overall_scores as DEFAULT_PROCESSOR


def _as_symbols(value: Optional[Iterable[str] | str]) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)

class FeedGetter:
    def __init__(self, tickers: Optional[Iterable[str] | str] = None, rss_source: str = "Yahoo Finance", max_seen_headlines: int = 500,
        news_queue_obj: Optional[queue.Queue] = None, process_batch_callback: Optional[Callable] = DEFAULT_PROCESSOR,
        alpaca_key: Optional[str] = None,
        alpaca_secret: Optional[str] = None) -> None:
        
        self.tickers = _as_symbols(tickers)
        self.rss_source = rss_source
        self.news_queue = news_queue_obj or queue.Queue()
        self.seen_headlines = deque(maxlen=max_seen_headlines)
        self.process_batch_callback = process_batch_callback
        self.alpaca_key = os.getenv("ALPACA_KEY")
        self.alpaca_secret = os.getenv("ALPACA_SECRET")

    def set_tickers(self, tickers: Iterable[str] | str) -> None:
        self.tickers = _as_symbols(tickers)

    def fetch_ticker_rss(self, ticker: str) -> list[dict]:
        try:
            print(f"Fetching RSS for {ticker}") 
            template = config.RSS_PREFER_NEWS.get(self.rss_source)
            if not template:
                raise KeyError(f"RSS source '{self.rss_source}' is not configured.")

            url = template.format(ticker=ticker)
            feed = feedparser.parse(url)
            articles = []

            for entry in getattr(feed, "entries", []):
                description = getattr(entry, "summary", "") or getattr(entry, "description", "")
                if description:
                    description = clean_text(description)

                published = getattr(entry, "published", datetime.now())
                news_entry = {
                    "ticker": ticker,
                    "source": self.rss_source,
                    "title": entry.title,
                    "description": description,
                    "link": entry.link,
                    "published": published,
                }
                articles.append(news_entry)

            return articles
        except Exception as exc:
            print(f"Error fetching RSS for {ticker}: {exc}")
            return []

    def _enqueue_articles(self, articles: list[dict]) -> None:
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
                        self._enqueue_articles(articles)
                    except Exception as exc:
                        print(f"Error processing article batch: {exc}")

                time.sleep(poll_interval)

    def _run_processor(self, df_batch: pd.DataFrame) -> None:
        if not self.process_batch_callback:
            return 

        try:
            self.process_batch_callback(df_batch)
        except TypeError:
            source_weights = getattr(config, "SOURCE_WEIGHTS", None)
            if source_weights is None:
                raise
            self.process_batch_callback(df_batch, source_weights)

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
                    self._run_processor(df_batch)
                except Exception as exc:
                    print(f"Error in pipeline: {exc}")
                finally:
                    buffer = []

    def start_alpaca_stream(
        self,
        tickers: Optional[Iterable[str] | str] = None,
        alpaca_key: Optional[str] = None,
        alpaca_secret: Optional[str] = None,
    ) -> None:
        symbols = _as_symbols(tickers) or self.tickers
        if not symbols:
            raise ValueError("No tickers configured for Alpaca stream.")

        key = alpaca_key or self.alpaca_key
        secret = alpaca_secret or self.alpaca_secret
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


# Backward-compatible functional API.
_DEFAULT_FEED_GETTER = FeedGetter()
SEEN_HEADLINES = _DEFAULT_FEED_GETTER.seen_headlines
news_queue = _DEFAULT_FEED_GETTER.news_queue


def worker_logic(timeout: int = 5) -> None:
    _DEFAULT_FEED_GETTER.worker_logic(timeout=timeout)


def fetch_ticker_rss(ticker: str) -> list[dict]:
    return _DEFAULT_FEED_GETTER.fetch_ticker_rss(ticker)


def rss_market_news(tickers: Iterable[str] | str, timeout: int = 5, poll_interval: int = 5) -> None:
    _DEFAULT_FEED_GETTER.set_tickers(tickers)
    _DEFAULT_FEED_GETTER.rss_market_news(timeout=timeout, poll_interval=poll_interval)


def start_alpaca_stream(ticker: Iterable[str] | str, 
                        alpaca_key: Optional[str] = None, alpaca_secret: Optional[str] = None) -> None:
    _DEFAULT_FEED_GETTER.start_alpaca_stream(tickers=ticker, alpaca_key=alpaca_key, alpaca_secret=alpaca_secret)
    
