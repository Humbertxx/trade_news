import os
from functools import partial
from threading import Thread

from feed_getter import FeedGetter
from normalization import process_batch
import config

def main():
    tickers_env = os.getenv("TICKERS")
    if not tickers_env:
        raise ValueError("not ticker to process, please set up!")
    
    tickers = [ticker.strip() for ticker in tickers_env.split(",") if ticker.strip()]
    rss_sources = list(config.RSS_PREFER_NEWS.keys())

    process_batch_callback = partial(process_batch, source_weights=config.SOURCE_WEIGHTS)

    feed = FeedGetter(tickers=tickers,rss_source=rss_sources,max_seen_headlines=config.MAX_SEEN_HEADLINE,
                      process_batch_callback=process_batch_callback)
    
    Thread(target=feed.worker_logic(timeout=config.POLL_INTERVAL), daemon=True).start()
    Thread(target=feed.start_alpaca_stream, daemon=True).start()
    feed.rss_market_news(timeout=config.TIMEOUT_FETCH, poll_interval=config.POLL_INTERVAL)

if __name__ == "__main__":
    main()
