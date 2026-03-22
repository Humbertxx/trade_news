WIP: Current README.md is from first version of project. It will be updated as the project evolves completely. For now, it serves as a basic overview of the project's purpose. Structure and usage changes are expected as the codebase develops. Please refer to the latest code for the most accurate implementation details.

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




## Notes

