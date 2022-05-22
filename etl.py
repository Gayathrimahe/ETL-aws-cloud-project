import pandas as pd



ny_url = "data/ETL_data.csv"
jh_url = "data/ETL_recovery.csv"
def extract_transform(ny_url, jh_url):
    try:
        #------Read---------------------------
        df_ny = pd.read_csv(ny_url)
        df_jh = pd.read_csv(jh_url)

        #-----Transform-----------------------
        #df_ny = df_ny.astype({'date': 'datetime64[ns]', 'cases': 'int64', 'deaths': 'int64'})
        df_ny['date'] = pd.to_datetime(df_ny['date'])
        df_jh = df_jh[df_jh['Country/Region'] == 'US']
        #df_jh = df_jh.astype({'Date': 'datetime64[ns]', 'Recovered': 'int64'})
        df_jh['Date'] = pd.to_datetime(df_jh['Date'])
        df_jh = df_jh[['Date', 'Recovered']]
        df_jh.rename(columns={'Date': 'date', 'Recovered': 'recovered'}, inplace=True)
        df_final = pd.merge(df_ny, df_jh, on='date')
        return df_final
    except Exception as e:
        print(e)
        exit(1)

extract_transform(ny_url, jh_url)




