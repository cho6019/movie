import os
import requests
import pandas as pd

BASE_URL = 'http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
KEY = os.getenv("MOVIE_KEY")
#'834d198b630513ee773cea7d5830e23f'

num_cols=['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
             'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten', 'salesChange',
             'audiInten', 'audiChange']

def gen_url(dt="20120101", url_param={}):
    "호출url 생성, url_param이 입력되면 multiMovieYn, repNationCd처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    for k, v in url_param.items():
        url = url + f"&{k}={v}"
    return url


def call_api(dt='20120101', url_param={}):
    respond = requests.get(gen_url(dt, url_param))
    if respond.status_code==200:
        return respond.json()['boxOfficeResult']["dailyBoxOfficeList"]
    else: return None

def list2df(data: list, dt: str, url_params={}):
    df = pd.DataFrame(data)
    df['dt'] = dt
    for k, v in url_params.items():
        df[k] = v
        
    num_cols=['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
             'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten', 'salesChange',
             'audiInten', 'audiChange']
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    return df

def save_df(df: pd.DataFrame, path, partitions=['dt']):
    df.to_parquet(path, partition_cols=partitions)
    save_path = path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path

def list2df_check_num(df: pd.DataFrame, l=num_cols):
    for c in l:
        df[c] = df[c].apply(pd.to_numeric)
    return df

def fill_na_with_column(origin_df, c_name):
    df = origin_df.copy()
    for i, row in df.iterrows():
        if pd.isna(row[c_name]):
            same_movie_df = df[df["movieCd"] == row["movieCd"]]
            notna_idx = same_movie_df[c_name].dropna().first_valid_index()
            if notna_idx is not None:
                df.at[i, c_name] = df.at[notna_idx, c_name]
    return df

def gen_unique(df: pd.DataFrame, drop_columns: list) -> pd.DataFrame:
    df_drop = df.drop(columns=['rnum', 'rank', 'rankInten', 'salesShare'])
    unique_df = df_drop.drop_duplicates()
    return unique_df

def re_ranking(df: pd.DataFrame) -> pd.DataFrame:
    df["rnum"] = df["audiCnt"].rank(method="dense", ascending=False).astype(int)
    df["rank"] = df["audiCnt"].rank(method="min", ascending=False).astype(int)
    return df

def combine_unique_parquet(base_path):
    combined_df = pd.DataFrame()
    parquet_files = [f for f in os.listdir(base_path) if f.endswith(".parquet")]
    
    for file in parquet_files:
        file_path = os.path.join(base_path, file)
        df = pd.read_parquet(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    # 중복 제거
    combined_df = combined_df.drop_duplicates()
    return combined_df