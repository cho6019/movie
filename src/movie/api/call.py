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

def list2df(data=[], ymd="20120101"):
    result = pd.DataFrame(data)
    result['dt'] = ymd
    return result

def save_df(df: pd.DataFrame, path: str):
    df.to_parquet(path, partition_cols=['dt'])
    save_path = f"{path}/dt={df['dt'][0]}"
    return save_path

def list2df_check_num(df: pd.DataFrame, l=num_cols):
    for c in l:
        df[c] = df[c].apply(pd.to_numeric)
    return df