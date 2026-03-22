import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import config
from sentiment import *

def process_batch(df: pd.DataFrame) -> pd.DataFrame:
    if "description" not in df.columns:
        df["description"] = ""
    df = concatenate_text(df)
    
    df = dedup_tiingo_marketaux(df, weights_dict=config.SOURCE_WEIGHTS)
    df = calculated_weighted_sentiment(df, source_weight=config.SOURCE_WEIGHTS)
    score = final_weight_score(df)
    return results(df, score)

    # allows for full description (headline + text) allowing better model clustering
def concatenate_text(df: pd.DataFrame) -> pd.DataFrame:
    df['description'] = df['description'].fillna('')
    df['full_text'] = df['title'] + ". " + df['description']
    return df

# Scikit Learn TF-IDF and cross cosine functions
def dedup_tiingo_marketaux(df, weights_dict=None, time_window=1800, sim_threshold=0.85,default_weight=1):
    
    if weights_dict is None:
        weights_dict = {}

    df = df.copy()
    df["published"] = pd.to_datetime(df["published"], utc=True)

    subset_cols = ["title", "source"]
    subset_cols = [c for c in subset_cols if c in df.columns]

    df = df.drop_duplicates(subset=subset_cols).reset_index(drop=True)
    df["temp_weight"] = df["source"].map(weights_dict).fillna(default_weight)

    # Time bucketing
    epoch = pd.Timestamp("1970-01-01", tz="utc")
    df["time_bucket"] = ((df["published"] - epoch).dt.total_seconds() // time_window).astype(int)

    # Single global TF-IDF over all titles
    titles = df["title"].astype(str).tolist()
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), lowercase=True, stop_words="english")
    tfidf_matrix = vectorizer.fit_transform(titles)

    indices_to_drop = set()

    for _, group in df.groupby("time_bucket"):
        idx_in_bucket = group.index.to_numpy()
        if len(idx_in_bucket) < 2:
            continue
        bucket_matrix = tfidf_matrix[idx_in_bucket]
        sim_matrix = cosine_similarity(bucket_matrix)
        
        weights = df.loc[idx_in_bucket, "temp_weight"].to_numpy()
        n = len(idx_in_bucket)

        for i in range(n):
            gi = idx_in_bucket[i]
            if gi in indices_to_drop:
                continue

            similar_js = np.where(sim_matrix[i] >= sim_threshold)[0]
            for j in similar_js:
                if j == i:
                    continue
                gj = idx_in_bucket[j]
                if gj in indices_to_drop:
                    continue

                if weights[i] < weights[j]:
                    indices_to_drop.add(gi)
                    break
                else:
                    indices_to_drop.add(gj)

    df_clean = df.drop(index=list(indices_to_drop)).reset_index(drop=True)
    return df_clean.drop(columns=["temp_weight", "time_bucket"])