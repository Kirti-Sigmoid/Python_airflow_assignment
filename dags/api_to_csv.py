import csv

import requests
from datetime import datetime

def write_api_data_to_csv():
    api_url = "https://community-open-weather-map.p.rapidapi.com/weather"

    headers = {
    'x-rapidapi-host': 'community-open-weather-map.p.rapidapi.com',
    'x-rapidapi-key': 'de44fbe964msh0afeef2586ea4a8p1d0defjsn156d75bc2035'

    }

    places = ["Delhi","Noida","Bhopal","Bangalore","Lucknow","Srinagar","Surat","Mumbai","Patna","Unnao"]
    Header=['City-Name','Description','Temperature','Feels Like Temperature','Min Temperature','Max Temperature', 'Humidity', 'Clouds']
    data_In_JSON=[]

    for place in places:
        try:

            query = {"q":f"{place},india","lat":"0","lon":"0","id":"2172797","lang":"null","units":"imperial","mode":"JSON"}
            response = requests.request("GET", api_url, headers=headers, params=query)
            data = response.json()
            print(response.text)
            item=[data['name'],data['weather'][0]['description'],data['main']['temp'],data['main']['feels_like'],data['main']['temp_min'],
                  data['main']['temp_max'],data['main']['humidity'],data['clouds']['all']]
            data_In_JSON.append(item)
            print(item)
        except:
            print("exceeded the limit of API request")

   # try:
    with open('/usr/local/airflow/store_files_airflow/weather.csv', 'w', encoding='UTF8',newline='') as f:
            writer = csv.writer(f)
            writer.writerow(Header)
            writer.writerows(data_In_JSON)


    # except:
    #     print("some error occured")


write_api_data_to_csv()

