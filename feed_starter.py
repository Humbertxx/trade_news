from feed_getter import FeedGetter

FeedGetter.seen_headlines
FeedGetter.news_queue

def worker_logic(timeout: int = 5) -> None:
    FeedGetter.worker_logic(timeout=timeout)


def fetch_ticker_rss(ticker: str) -> list[dict]:
    return FeedGetter.fetch_ticker_rss(ticker)


def rss_market_news(tickers: Iterable[str] | str, timeout: int = 5, poll_interval: int = 5) -> None:
    FeedGetter.set_tickers(tickers)
    FeedGetter.rss_market_news(timeout=timeout, poll_interval=poll_interval)

def start_alpaca_stream(ticker: Iterable[str] | str, 
                        alpaca_key: Optional[str] = None, alpaca_secret: Optional[str] = None) -> None:
    FeedGetter.start_alpaca_stream(tickers=ticker, alpaca_key=alpaca_key, alpaca_secret=alpaca_secret)
    
