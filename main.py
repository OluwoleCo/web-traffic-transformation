import pandas as pd
import requests
import string
import io

ROOT_URL = 'https://public.wiwdata.com/engineering-challenge/data/'
letters = list(string.ascii_lowercase)

def get_webpages(ROOT_URL):
    appended_df = []
    
    for item in letters:
        try:
            req = requests.get(ROOT_URL + item + '.csv')
            req.raise_for_status()
            response = req.content
            result_to_df = pd.read_csv(io.StringIO(response.decode('utf-8'))) # pages to df
            appended_df.append(result_to_df)
        except requests.exceptions.RequestException as e:
            print(f"Request exception: {e}")
    
    if appended_df:
        appended_df = pd.concat(appended_df, ignore_index=True)
    
    return pd.DataFrame(appended_df)

webpage_df = get_webpages(ROOT_URL)
user_grouping = webpage_df.groupby(['user_id', 'path'])['length'].sum()
pivot_users = user_grouping.unstack(level='path', fill_value=0)

pivot_users.to_csv('data/web_traffic_transformed.csv')
