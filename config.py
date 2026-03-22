# can expand later on the amount of rss feeds use
## REMEMBER instability of rss feeds!
RSS_PREFER_NEWS = {'Yahoo Finance':'https://finance.yahoo.com/rss/headline?s={ticker}' }                                
    
SOURCE_WEIGHTS = {
    'Benzinga': 2.0,       
    'Yahoo Finance': 0.6,  
    'Unknown': 0.2
}
TIMEOUT_FETCH = 5       
POLL_INTERVAL = 5  
MAX_SEEN_HEADLINE = 500  