import pandas as pd
from rapidfuzz import fuzz

############################ VERSION 1, HEADLINE COMPARISON AND TIME FIX IN TWO DIFFERENT FUNCTIONS ############################

# Simple duplication handler using fuzz (On), text level similarity
def is_duplicate(new_headline : str , seen_headlines : str, threshold : int =  85) -> bool:
    for seen in seen_headlines:
        if new_headline == seen:
            return True
        
        similarity = fuzz.token_set_ratio(new_headline, seen)
        if similarity > threshold:
            return True
    return False

# change each format of time to UTC time 
def time_fix(df: pd.DataFrame) -> pd.DataFrame:
    df['published'] = pd.to_datetime(df['published'], utc=True, format='mixed')
    return df
    
############################ VERSION 2, CHANGE ############################
    
# O(n^2) process for similarity, using fuzz, remove article with less relevance based on the weights
def remove_similar_rows_weighted(df : pd.DataFrame, weights_dict : dict, threshold: int = 85, 
                                time_window : int = 1800, default_weight : int = 1) -> pd.DataFrame:
    df = df.copy()
    df['published'] = pd.to_datetime(df['published'], utc=True).astype('int64') // 10**9
    df = df.drop_duplicates(subset=['title']).sort_values(by='published').reset_index(drop=True)
    
    weights = df['source'].map(weights_dict).fillna(default_weight).to_numpy()
    titles = df['title'].tolist()
    timestamps = df['published'].to_numpy()
    
    indices_to_drop = set()
    n = len(df)
    
    for i in range(n):
        if i in indices_to_drop:
            continue
        current_weight = weights[i]
        current_title = titles[i]
        current_ts = timestamps[i]
        
        for j in range(i + 1, n):
            if j in indices_to_drop:
                continue   
            time_diff = timestamps[j] - current_ts
            
            if time_diff > time_window:
                break
            ratio = fuzz.token_set_ratio(current_title, titles[j], score_cutoff=threshold)
            
            if ratio > threshold:
                if current_weight < weights[j]:
                    indices_to_drop.add(i)
                    break 
                else:
                    indices_to_drop.add(j)
                    
    return df.iloc[[i for i in range(n) if i not in indices_to_drop]].drop(columns=['published']).reset_index(drop=True)
     