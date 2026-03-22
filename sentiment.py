import pandas as pd
from transformers import pipeline
from typing import Mapping
     

def get_sentiment_pipe():
    sentiment_pipe = pipeline("text-classification", model="ProsusAI/finbert", top_k=None)
    return sentiment_pipe

# get the aggregated title (title + description) and calculate sentiment
def calculated_weighted_sentiment(df: pd.DataFrame, pipe=None, source_weight: Mapping[str, float] | None = None, 
                                  default_weight: int = 1) -> pd.DataFrame:
    pipe = get_sentiment_pipe()
    texts = df['title'].tolist() 
    raw_results = pipe(texts)
    
    sentiment_scores = []
    
    for result in raw_results:
        result_items = [result] if isinstance(result, dict) else result
        scores = {str(item['label']).lower(): item['score'] for item in result_items}
        # Calculate Scalar: Positive - Negative
        scalar = scores.get('positive', 0) - scores.get('negative', 0)
        sentiment_scores.append(scalar)
        
    df['sentiment_score'] = sentiment_scores
    df['weight'] = df['source'].map(source_weight).fillna(default_weight)
    df['weighted_contribution'] = df['sentiment_score'] * df['weight']
    
    return df

# returns final score based on the weighted average of all news sources
def final_weight_score(df: pd.DataFrame) -> float:  
    total_weight = float(df['weight'].sum())
    if total_weight == 0:
        return 0
    final_signal_score = float((df['weighted_contribution'].sum()) / total_weight)
    
    return final_signal_score

def results(overall_dataframe: pd.DataFrame, final_signal_score: float) -> dict:
    print("\n" + "="*50)
    print("PROCESSED DATAFRAME (Top 5)")
    print("="*50)
    cols_to_show = ['published', 'source', 'sentiment_score', 'weight', 'title']
    print(overall_dataframe[cols_to_show].head())

    print("\n" + "="*50)
    print("FINAL TRADING SIGNAL")
    print("="*50)

    result = {"Aggregated Sentiment Score": final_signal_score}
    
    if final_signal_score > 0.2:
        result["Signal"] = "STRONG BUY"
    elif final_signal_score > 0.05:
        result["Signal"] = "WEAK BUY"
    elif final_signal_score < -0.2:
        result["Signal"] = "STRONG SELL"
    elif final_signal_score < -0.05:
        result["Signal"] = "WEAK SELL"
    else:
        result["Signal"] = "HOLD / NEUTRAL"

    return result
    
    
