# Real-Time Financial News Sentiment getter

A multi-threaded Python pipeline designed to ingest financial news from multiple sources (Yahoo Finance RSS and Alpaca News API) in real-time. It normalizes the data, deduplicates headlines, and feeds them into a sentiment (`FinBERT`) analysis engine.

## Features
**Hybrid Ingestion:** 

_Streaming:_ Connects to Alpaca's WebSocket for push-based real-time news updates (Benzinga). 

_Polling:_ Periodically fetches RSS feeds from Yahoo Finance (async/threaded). 

_Concurrency:_ Uses Python threading and queue to separate data fetching from data processing, ensuring the stream is never blocked by heavy analysis tasks. 

_Smart Deduplication:_ Maintains a rolling buffer of seen headlines to prevent processing the same news twice. 

_Data Cleaning:_ Automatically strips HTML tags, URLs, and metadata from descriptions using compiled Regex. 

## Prerequisites
You will need Python 3.x and the following libraries:
```pip install pandas feedparser alpaca-py```

### Configuration
The script relies on a local config.py file to store sensitive keys and settings. Ensure this file exists in your project directory with the following variables:

config.py
```python
# API Keys for Alpaca Markets
ALPACA_KEY = "your_alpaca_public_key"
ALPACA_SECRET = "your_alpaca_secret_key"

# RSS Feed URLs
RSS_PREFER_NEWS = {
    'Yahoo Finance': 'https://finance.yahoo.com/rss/headline?s={ticker}'
}
```
Note: You also need a sentiment.py module with an overall_scores(df) function to handle the processing logic.

### Usage
1. Set your Tickers:
   Open the script and modify the TICKERS list in the __main__ block:
```python
    if __name__ == "__main__":
        TICKERS = ["AAPL", "TSLA", "NVDA"] 
```
2. Run the script:
```python feed_getter.py```
(Replace feed_getter.py with whatever you named this file)

3. Operation:

* The script initializes a Worker Thread that listens for incoming news data.
* It starts an Alpaca Stream thread to push real-time news.
* (Optional) Uncomment the t_rss lines in the main block to enable RSS polling.
* Processed news batches are passed to overall_scores() for sentiment analysis.

## Code Structure
* ```worker_logic```: The "brain" of the operation. It sits in an infinite loop waiting for data in the ```news_queue```. When data arrives, it batches it into a Pandas DataFrame and sends it to the sentiment engine.
* ```rss_market_news```: Uses a ```ThreadPoolExecutor``` to fetch RSS feeds for multiple tickers simultaneously without blocking the main program.
* ```start_alpaca_stream```: An async wrapper that connects to the Alpaca ```NewsDataStream``` and pushes events into the shared queue.
* ```CLEAN_*``` Regex: Pre-compiled patterns used to sanitize raw HTML content from RSS feeds.

## Notes
* RSS Threading: The RSS polling thread (`t_rss`) is currently commented out in the `__main__` section. Uncomment it if you want to pull Yahoo Finance data alongside the Alpaca stream.
* Deduplication: The script uses `difflib.SequenceMatcher` to detect slight variations in headlines, preventing duplicate processing of the same story from different sources.
