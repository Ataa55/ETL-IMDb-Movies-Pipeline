from sqlalchemy import create_engine
from airflow.decorators import dag, task 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from selenium import webdriver
import datetime 
import pandas as pd
 

def del_chars(votes):
    chars = {'K': 1e3, 'M': 1e6, 'B': 1e9}
    data = votes[-1].upper()
    if data.isdigit():
        return(votes)
    elif data in chars:
        number = float(votes[:-1]) * chars[data]
        return number
    else:
        return None

@dag(
    dag_id = "ETL_Movies",
    schedule_interval="@daily",
    start_date = pendulum.datetime(2023,11,10),
    catchup= False
)
def movies_etl():

    @task()
    def scrap_movies():
        Scrapper = webdriver.Firefox()
        Scrapper.get("https://www.imdb.com/chart/boxoffice/?ref_=nv_ch_cht")
        My_movies = Scrapper.find_elements("xpath", '//div[@class="sc-479faa3c-0 fMoWnh cli-children"]')
        Movies = []
        for movie in My_movies:
            Movies.append(movie.text)
        clean_movies = []

        data = {}
        for movie in Movies:
            raw_data = movie.split('\n')
            Movie_title = raw_data[0][3:]
            move_total_gross =del_chars(raw_data[1].split('$')[1])
            move_users_vote = del_chars(raw_data[5][2:-1])
            Movie_year = datetime.date.today().year
            Movie_rate = raw_data[4]
            data = {
                "movie_name":Movie_title,
                "movie_total_gross":move_total_gross,
                "movie_users_votes":move_users_vote,    
                "movie_year":Movie_year,
                "movie_rate":Movie_rate
                    }
            clean_movies.append(data)
                
        Movies_df = pd.DataFrame(clean_movies)
        Movies_df['movie_users_votes'] = pd.to_numeric(Movies_df['movie_users_votes'], errors='coerce')
        Final_scrap_filtered = Movies_df.dropna(subset=['movie_users_votes'])
        return Final_scrap_filtered

#connect to the database and extract the data    
    @task()
    def retrieve_data():
        sql_query = """select movie_name, movie_total_gross,
            movie_users_votes, movie_year, movie_rate from movies"""
        hook = PostgresHook(postgres_conn_id = "movies")
        df_db = hook.get_pandas_df(sql_query)
        return df_db
    
#integrate and process our data 
    @task()
    def IntegrateAndProcsess(scraped_df, retrieved_df):
        merged_df = pd.concat([scraped_df, retrieved_df])
        merged_df['movie_total_gross'] = merged_df['movie_total_gross'].astype(float)
        merged_df['movie_users_votes'] = merged_df['movie_users_votes'].astype(float)
        merged_df['movie_year'] = merged_df['movie_year'].astype(int)
        merged_df['movie_rate'] = merged_df['movie_rate'].astype(float)

        return merged_df
    
    #load tge data 
    @task()
    def laodToDB(Transformed_Data):
        #['movie_name','movie_total_gross', 'movie_users_votes', 'movie_year', 'movie_rate']
        target_fields = list(Transformed_Data.columns)
        print(target_fields)
        hook = PostgresHook(postgres_conn_id = "movies")
        hook.insert_rows(table="load_movies", rows = Transformed_Data.values.tolist(), target_fields=target_fields)   
    
    @task()
    def writeTocsv(Totaldf):
        csv_path = "/home/oem/Documents/final_Data.csv"
        Totaldf.to_csv(csv_path,index = False)
#define tasks
    scraped_data  = scrap_movies()
    postgres_data = retrieve_data()
    collectData   = IntegrateAndProcsess(scraped_data,postgres_data)
    loadData      = laodToDB(collectData)
    loadTocsv     = writeTocsv(collectData) 

movies_etl()
    