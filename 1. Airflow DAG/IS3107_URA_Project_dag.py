import json
from datetime import datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from google.cloud import bigquery
import time

default_args = {
    'owner':'airflow' ,
}

with DAG(
    dag_id="IS3107_URA_Project_dag",
    default_args=default_args,
    description='URA Project DAG' ,
    schedule_interval='0 * * * *',
    start_date=datetime( 2022 , 3 , 3 ),
    catchup=False ,
    tags=['project'],
) as dag:

    dag.doc_md = __doc__

    def get_URA_Token(**kwargs):
        ti = kwargs['ti']
        headers = {"accesskey":"f84b64c0-d851-4282-8aa2-da0e1bd4bde9",
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
        response = requests.get("https://www.ura.gov.sg/uraDataService/insertNewToken.action", headers=headers)
        token = response.json()['Result']
        print("Token: " + str(token))
        ti.xcom_push(key='token', value=token)


    def get_Carpark_Availability(**kwargs):
        ti = kwargs['ti']
        token = ti.xcom_pull(task_ids='get_URA_Token', key='token')
        print("Token: " + str(token))
        headers = {"accesskey":"f84b64c0-d851-4282-8aa2-da0e1bd4bde9", "token":token,
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}

        response = requests.get("https://www.ura.gov.sg/uraDataService/invokeUraDS?service=Car_Park_Availability", headers=headers)
        carparkAvailibility = response.json()
        result = carparkAvailibility['Result']
        print("Number of rows returned: " + str(len(result)))
        carparkAvailibility_json_string = json.dumps(result)
        ti.xcom_push('carparkAvailibility_json_string', carparkAvailibility_json_string)


    def transform_carpark_data(**kwargs):
        ti = kwargs['ti']
        carparkAvailibility_json_string = ti.xcom_pull(task_ids='get_Carpark_Availability', key= 'carparkAvailibility_json_string' )
        carparkAvailibility = json.loads(carparkAvailibility_json_string)
        print(len(carparkAvailibility))
        carpark_df =  pd.DataFrame(carparkAvailibility)
        carpark_df["ingestionTime"] = datetime.now().replace(minute=0, second=0, microsecond=0)
        print(carpark_df.head())
        carpark_df_json_string = carpark_df.to_json(orient = 'index')   
        ti.xcom_push('carpark_df_json_string', carpark_df_json_string)


    def get_Weather_data(**kwargs):
        ti = kwargs['ti']
        carpark_df_json_string = ti.xcom_pull(task_ids='transform_carpark_data', key= 'carpark_df_json_string' )
        carpark_df = pd.read_json(carpark_df_json_string, orient ='index')

        carparkNoList = []
        weatherList = []
        descriptionList = []
        tempList = []
        humidityList = []

        geometry = carpark_df['geometries']

        coordinates = geometry.str[0]

        xAndy = coordinates.str['coordinates'].str.split(",")
        x = xAndy.str[0]
        y = xAndy.str[1]


        # # Add Longitutde and Latitude back to main dataframe
        carpark_df["x"] = x
        carpark_df["y"] = y

        # print(carpark_df)
        latList = []
        lngList = []


        for lng,lat in zip(carpark_df.x, carpark_df.y):
            response = requests.get("https://developers.onemap.sg/commonapi/convert/3414to4326?X=" + str(lng) + "&Y=" + str(lat))
            latList.append(response.json()['latitude'])
            lngList.append(response.json()['longitude'])


        carpark_df["latitude"] = latList
        carpark_df["longitude"] = lngList

        lat_lng = carpark_df[['carparkNo', 'latitude', 'longitude']]

        for index, row in lat_lng.iterrows():

            carparkNo = row['carparkNo']
            lat = row['latitude']
            lng = row['longitude']

            print("Retrieving for car park no: " + carparkNo)

            # Removed API KEY
            response = requests.get("https://api.openweathermap.org/data/2.5/weather?lat=" + str(lat) + "&lon=" + str(lng) + "&appid=" + "&units=metric")
            output = response.json()

            carparkNoList.append(carparkNo)
            weatherList.append(output['weather'][0]['main'])
            descriptionList.append(output['weather'][0]['description'])
            tempList.append(output['main']['temp'])
            humidityList.append(output['main']['humidity'])
            time.sleep(1)


        # weather_df = pd.DataFrame(list(zip(carparkNoList, weatherList, descriptionList, tempList, humidityList)), columns=['carparkNo', 'weather','description', 'temperature', 'humidity']).set_index('carparkNo')

        carpark_df["weather"] = weatherList
        carpark_df["description"] = descriptionList
        carpark_df["temperature"] = tempList
        carpark_df["humidity"] = humidityList

        carpark_df = carpark_df.drop('geometries', axis=1)
        carpark_df = carpark_df.drop('x', axis=1)
        carpark_df = carpark_df.drop('y', axis=1)
        carpark_df = carpark_df.drop('latitude', axis=1)
        carpark_df = carpark_df.drop('longitude', axis=1)
        carpark_df_json_string = carpark_df.to_json(orient = 'index')   
        ti.xcom_push('carpark_df_json_string', carpark_df_json_string)
        
        

    def load_data_to_bigquery(**kwargs):
        ti = kwargs['ti']
        carpark_df_json_string = ti.xcom_pull(task_ids='get_Weather_data', key= 'carpark_df_json_string' )
        carpark_df = pd.read_json(carpark_df_json_string, orient ='index')

        carpark_df.index.name='tempIndex'

        print(carpark_df.head())

        print(carpark_df.dtypes)

        print("Loading data to bigquery")

        # connect to BigQuery
        client = bigquery.Client()

        table_id = "is3107-ura-project-379703.is3107project.carpark_availability"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            carpark_df,
            table_id,
            job_config=job_config
        ) 
        job.result()  
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        

        
        



    get_URA_Token = PythonOperator(task_id= 'get_URA_Token' , python_callable=get_URA_Token)
    get_Carpark_Availability = PythonOperator(task_id= 'get_Carpark_Availability' , python_callable=get_Carpark_Availability)
    transform_carpark_data = PythonOperator(task_id= 'transform_carpark_data' , python_callable=transform_carpark_data)
    get_Weather_data = PythonOperator(task_id= 'get_Weather_data' , python_callable=get_Weather_data)
    load_data_to_bigquery = PythonOperator(task_id= 'load_data_to_bigquery' , python_callable=load_data_to_bigquery)



    get_URA_Token >>  get_Carpark_Availability >> transform_carpark_data >> get_Weather_data >> load_data_to_bigquery
