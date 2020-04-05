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

#WeatherDF = sqlContext.createDataFrame(weather, ['Yearw','Monthw','Dayw','Hourw','TempC','Type'])

#Structure HourlyData: (Latitude, Longitude, Month, Day, Year, Hour, Count, Average)

#HourlyDataW = HourlyData.join(WeatherDF, (HourlyData.Year==WeatherDF.Yearw) & (HourlyData.Month==WeatherDF.Monthw) & (HourlyData.Day==WeatherDF.Dayw) & (HourlyData.Hour==WeatherDF.Hourw), how='left_outer')
#HourlyDataW = HourlyDataW.drop('Yearw', 'Monthw', 'Dayw', 'Hourw')
#HourlyDataW.toPandas().to_csv('/home/andrew/HourlyDataW.csv')

#finding factors of weather type
weathertest = weather.map(lambda x: (x[5],1))
weathertest = weathertest.reduceByKey(lambda x,n: x+n)
WeathertestDF = sqlContext.createDataFrame(weathertest, ['Type','Count'])


#Make the giant 24M line dataset for later subsettting:
newLLD=LLd.map(lambda x: (x[0],x[1]))

BigRDD=weather.cartesian(newLLD)

def stripout(x):
    a=x[0]
    b=x[1]
    return(b[0],b[1],a[0],a[1],a[2],a[3],a[4],a[5])


BigRDD=BigRDD.map(stripout)

BigDF=sqlContext.createDataFrame(BigRDD, ['Latitudeb','Longitudeb','Yearb','Monthb','Dayb','Hourb','TempC','Type'])

#Join on count and average data:


"""
scp andrew@10.10.11.35:/home/andrew/HourlyDataW.csv /Users/andrew/Desktop
"""
