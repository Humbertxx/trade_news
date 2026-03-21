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

from normalization import clean_text
import config


def _as_symbols(value: Optional[Iterable[str] | str]) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _as_sources(value: Optional[Iterable[str] | str]) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def resolve_rss_sources(requested_sources: Optional[Iterable[str] | str] = None) -> tuple[str, ...]:
    configured_sources = getattr(config, "RSS_PREFER_NEWS", {})
    if not configured_sources:
        raise ValueError("RSS_PREFER_NEWS has no configured sources.")

    normalized_lookup = {key.strip().lower(): key for key in configured_sources}
    if requested_sources is None:
        return tuple(configured_sources.keys())

    resolved_sources = []
    unknown_sources = []
    for source in _as_sources(requested_sources):
        if source in configured_sources:
            matched_source = source
        else:
            normalized_source = str(source).strip().lower()
            matched_source = normalized_lookup.get(normalized_source)

        if not matched_source:
            unknown_sources.append(str(source))
            continue

        if matched_source not in resolved_sources:
            resolved_sources.append(matched_source)

    valid_sources = ", ".join(sorted(configured_sources.keys()))
    if unknown_sources:
        invalid = ", ".join(unknown_sources)
        raise ValueError(
            f"Unknown rss_source value(s): {invalid}. Valid RSS_PREFER_NEWS keys: {valid_sources}"
        )
    if not resolved_sources:
        raise ValueError(f"No valid rss_source values provided. Valid RSS_PREFER_NEWS keys: {valid_sources}")
    return tuple(resolved_sources)


def resolve_rss_source(requested_source: str) -> str:
    return resolve_rss_sources(requested_source)[0]
 
class FeedGetter:
    def __init__(self, tickers: Optional[Iterable[str] | str] = None, rss_source: Optional[Iterable[str] | str] = None, max_seen_headlines: int = 500,
        news_queue_obj: Optional[queue.Queue] = None, process_batch_callback: Optional[Callable] = DEFAULT_PROCESSOR,
        alpaca_key: Optional[str] = None,
        alpaca_secret: Optional[str] = None) -> None:
        
        self.tickers = _as_symbols(tickers)
        self.rss_sources = resolve_rss_sources(rss_source)
        self.rss_source = self.rss_sources[0]
        self.news_queue = news_queue_obj or queue.Queue()
        self.seen_headlines = deque(maxlen=max_seen_headlines)
        self.process_batch_callback = process_batch_callback
        
        self.alpaca_key = os.getenv("ALPACA_KEY")
        self.alpaca_secret = os.getenv("ALPACA_SECRET")

    def set_tickers(self, tickers: Iterable[str] | str) -> None:
        self.tickers = _as_symbols(tickers)

    def set_rss_source(self, rss_source: Iterable[str] | str) -> None:
        self.rss_sources = resolve_rss_sources(rss_source)
        self.rss_source = self.rss_sources[0]

    def set_rss_sources(self, rss_sources: Iterable[str] | str) -> None:
        self.set_rss_source(rss_sources)

    def fetch_ticker_rss(self, ticker: str) -> list[dict]:
        articles = []
        configured_sources = getattr(config, "RSS_PREFER_NEWS", {})
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
                    if description:
                        description = clean_text(description)

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

    def run_processor(self, df_batch: pd.DataFrame) -> None:
        if not self.process_batch_callback:
            return 
        try:
            self.process_batch_callback(df_batch)
        except TypeError:
            source_weights = os.getenv("SOURCE_WEIGHTS")
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
                    self.run_processor(df_batch)
                except Exception as exc:
                    print(f"Error in pipeline: {exc}")
                finally:
                    buffer = []

    def start_alpaca_stream(self, tickers: Optional[Iterable[str] | str] = None,
        alpaca_key: Optional[str] = None,
        alpaca_secret: Optional[str] = None) -> None:
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

