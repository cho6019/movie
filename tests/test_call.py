from movie.api.call import (
    gen_url, call_api, list2df, save_df,
    fill_na_with_column, gen_unique, re_ranking, combine_unique_parquet
)
import os
import pandas as pd
from pandas.api.types import is_numeric_dtype
import pyarrow

def test_gen_url():
    r = gen_url()
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r
    assert True
    
    
def test_gen_url_default():
    r = gen_url(url_param={'multiMovieYn': 'Y', 'repNationCd': "K"})
    assert "multiMovieYn=Y" in r
    assert "&repNationCd=K" in r
    
def test_call_api():
    r = call_api()
    assert isinstance(r, list)
    assert isinstance(r[0]['rnum'], str)
    assert len(r) == 10
    for e in r:
        assert isinstance(e, dict)
        
def test_list2df():
    ymd="20120101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    assert isinstance(df, pd.DataFrame)
    assert len(data) == len(df)
    assert set(data[0].keys()).issubset(set(df.columns))
    assert "dt" in df.columns, "df 컬럼이 있어야 함"
    assert (df["dt"]==ymd).all()
    
def test_save_df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    base_path = "~/temp/movie"
    r = save_df(df, base_path)
    assert r == f"{base_path}/dt={ymd}"
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns
    
# def test_save_df_url_params():
    # ymd = "20210101"
    # url_params={'multiMovieYn': 'Y'}
    
    # data = call_api(ymd, url_params)
    # df = list2df(data, ymd, url_params)
    # base_path = "~/temp/movie"
    # r = save_df(df, base_path, ['dt'] + list(url_params.keys()))
    # assert r == f"{base_path}/dt={ymd}"
    # read_df = pd.read_parquet(r)
    # assert 'dt' not in read_df.columns
    # assert 'dt' in pd.read_parquet(base_path).columns
    
def test_merge_df():
    PATH = "~/data/movies/dailyboxoffice/dt=20240101"
    df = pd.read_parquet(PATH)
    assert len(df) == 50
    
    df1 = fill_na_with_column(df, 'multiMovieYn')
    assert df1["multiMovieYn"].isna().sum() == 5
    
    df2 = fill_na_with_column(df1, 'repNationCd')
    assert df2["repNationCd"].isna().sum() == 5

    drop_columns=['rnum', 'rank', 'rankInten', 'salesShare']
    unique_df = gen_unique(df=df2, drop_columns=drop_columns) 
    assert len(unique_df) == 25
    
    new_ranking_df = re_ranking(unique_df)
    assert new_ranking_df.iloc[0]['movieNm'] == '노량: 죽음의 바다'
    
def test_unique_parquet():
    base_path = '/home/ubuntu/data/movies/dailyboxoffice/dt=20240101'
    a = combine_unique_parquet(base_path)
    assert len(a)==10