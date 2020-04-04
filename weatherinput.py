"""
command to upload weather.csv file
scp /Users/andrew/Desktop/filteredweather.csv andrew@10.10.11.35:/home/andrew/filteredweather.csv
"""

"""
reading weather data into Spark
"""
weather = sc.textFile("/home/andrew/filteredweather.csv")
header = weather.take(1)
weather = weather.filter(lambda x: False if x == header[0] else True)

def convertweather(x):
    y=x.split(',')
    return(int(y[0]),int(y[1]),int(y[2]),int(y[3]),float(y[4]),y[5])


weather = weather.map(convertweather)

WeatherDF = sqlContext.createDataFrame(weather, ['Yearw','Monthw','Dayw','Hourw','TempC','Type'])

#Structure HourlyData: (Latitude, Longitude, Month, Day, Year, Hour, Count, Average)
HourlyDataW = HourlyData.join(WeatherDF, (HourlyData.Year==WeatherDF.Yearw) & (HourlyData.Month==WeatherDF.Monthw) & (HourlyData.Day==WeatherDF.Dayw) & (HourlyData.Hour==WeatherDF.Hourw), how='left_outer')

HourlyDataW = HourlyDataW.drop('Yearw', 'Monthw', 'Dayw', 'Hourw')

HourlyDataW.toPandas().to_csv('/home/andrew/HourlyDataW.csv')

"""
scp andrew@10.10.11.35:/home/andrew/HourlyDataW.csv /Users/andrew/Desktop
"""
