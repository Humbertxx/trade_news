from threading import Thread
from feed_getter import FeedGetter

def main():
    feed = FeedGetter(tickers=["AAPL"])
    Thread(target=feed.worker_logic, daemon=True).start()
    Thread(target=feed.start_alpaca_stream, daemon=True).start() 
    feed.rss_market_news(timeout=5, poll_interval=5)

if __name__ == "__main__":
    main()