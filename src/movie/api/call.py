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