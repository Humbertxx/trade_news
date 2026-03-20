import pandas as pd
import re

stopwords = {"the", "a", "an", "to", "of", "and", "for", "in", "on"}
CLEAN_HTML = re.compile(r"<[^>]+>")
CLEAN_URLS = re.compile(r"http[s]?://\S+")
CLEAN_METADATA = re.compile(r"(Article URL|Comments URL|Points|# Comments):.*?(?=\n|$)")
CLEAN_SPACES = re.compile(r"\n\s*\n")

# function that normalize text base on pass functions
def normalize(df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
    df = collected_data(df1,df2)
    df = concatenate_text(df)
    df = time_fix(df)
    return df

# normalize into a single data frame both APIS feed and APIS RSS
def collected_data(df_api: pd.DataFrame, df_rss: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df_api, df_rss], ignore_index=True)

# change each format of time to UTC time
def time_fix(df: pd.DataFrame) -> pd.DataFrame:
    df['published'] = pd.to_datetime(df['published'], utc=True, format='mixed')
    return df

def clean_text(title: str) -> str:
    cleaned = CLEAN_HTML.sub(" ", title or "")
    cleaned = CLEAN_URLS.sub(" ", cleaned)
    cleaned = CLEAN_METADATA.sub(" ", cleaned)
    cleaned = CLEAN_SPACES.sub(" ", cleaned)
    tokens = re.findall(r"\w+", cleaned.lower())
    tokens = [token for token in tokens if token not in stopwords]
    return " ".join(tokens)

def combine_table(all_articles: pd.DataFrame) -> pd.DataFrame:
    if not all_articles:
        return pd.DataFrame()
    # Combine everything into one table
    full_df = pd.concat(all_articles, ignore_index=True)
    
    # Remove duplicates inside this batch
    full_df.drop_duplicates(subset=['title'], inplace=True)
    
    #Remove headlines we have ALREADY seen in previous loops
    new_articles = []
    for index, row in full_df.iterrows():
        headline = row['title']
        if headline not in SEEN_HEADLINES:
            new_articles.append(row)
            SEEN_HEADLINES.append(headline) 
    
    return pd.DataFrame(new_articles)



# allows for full description (headline + text) allowing better model clustering
def concatenate_text(df: pd.DataFrame) -> pd.DataFrame:
    df['description'] = df['description'].fillna('')
    df['full_text'] = df['title'] + ". " + df['description']
    return df
