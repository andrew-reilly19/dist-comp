"""
This python file is designed to take the spark outputs from Diffmapping.py and attach other
data to it for subsetting and plotting.
"""



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

def wtypefil(x, cond):
    if x[5] == cond:
        return True
    else:
        return False


def filtostr(x):
    year = str(x[0])
    month = str(x[1])
    day = str(x[2])
    hour = str(x[3])
    key = year+','+month+','+day+','+hour
    return(key)


def ffilter(x):
    year = str(x[2])
    month = str(x[0])
    day = str(x[1])
    hour = str(x[3])
    key = year+','+month+','+day+','+hour
    if key in wtfilter:
        return True
    else:
        return False


def cfmap(x):
    lat = str(x[4])
    lng = str(x[5])
    hour = str(x[3])
    count = x[6]
    latlng = lat+','+lng+','+hour
    return(latlng, count)


def weathercsvs():
    types = ['broken clouds','scattered clouds','mist','sky is clear','light rain','few clouds','overcast clouds','moderate rain','haze','fog','heavy intensity rain']
    for type in types:
        typefilter = weather.filter(lambda x: wtypefil(x,type))
        typefilter = typefilter.map(filtostr)
        wtfilter = typefilter.collect()
        countfilter = totalHour.filter(lambda x: ffilter(x))
        CDF = sqlContext.createDataFrame(countfilter, ['Month', 'Day', 'Year', 'Hour','Lat','Lng','Count'])
        WDF = CDF.join(AvgDF, (CDF.Lat == AvgDF.Latitudea) & (CDF.Lng == AvgDF.Longitudea) & (CDF.Hour == AvgDF.Hour), how='left_outer')
        WDF = WDF.drop('Month','Day','Year','Hour','Latitudea','Longitudea','Hour')
        outpath = '/home/andrew/output/events/'+str(type)+'.csv'
        WDF.toPandas().to_csv(outpath)

weathercsvs()

"""
#finding factors of weather type
weathertest = weather.map(lambda x: (x[5],1))
weathertest = weathertest.reduceByKey(lambda x,n: x+n)
WeathertestDF = sqlContext.createDataFrame(weathertest, ['Type','Count'])
"""

#Make the giant 24M line dataset for later subsettting:
def imap(x):
    lat=str(x[0])
    lng=str(x[1])
    ll=lat+','+lng
    return(ll,1)


LLd = LLh.map(imap)

LLd = LLd.reduceByKey(lambda x,n: x+n)

LLnull = LLd.map(lambda x: (x[0], x[1], 2014, -1, -1, -1, "null", "null"))

def rmap(x):
    y=x[0]
    y=y.split(',')
    lat=float(y[0])
    lng=float(y[1])
    return(lat, lng)


LLd=LLd.map(rmap)

BigRDD=weather.cartesian(LLd)

def stripout(x):
    a=x[0]
    b=x[1]
    return(b[0],b[1],a[0],a[1],a[2],a[3],a[4],a[5])


BigRDD=BigRDD.map(stripout)

# def map1(x):
#     lat = str(x[0])
#     lng = str(x[1])
#     year = str(x[2])
#     month = str(x[3])
#     day = str(x[4])
#     hour = str(x[5])
#     temp = x[6]
#     type = x[7]
#     info = (lat+','+lng+','+year+','+month+','+day+','+hour)
#     return(info, temp, type)
#
# BigRDD=BigRDD.map(map1)

#totalHour structure: (Month, Day, Year, Hour, Latitude, Longitude, count)
def prep_rdd(x):
    hour=str(x[3])
    lat=str(x[4])
    lng=str(x[5])
    latlng = hour+','+lat+','+lng
    count = x[6]
    return (latlng,count)


#BigRDD=BigRDD.union(LLnull)

BigDF=sqlContext.createDataFrame(BigRDD, ['Latitudeb','Longitudeb','Yearb','Monthb','Dayb','Hourb','TempC','Type'])

#Join on count and average data:

BigDF = BigDF.join(AvgDF, (BigDF.Latitudeb == AvgDF.Latitudea) & (BigDF.Longitudeb == AvgDF.Longitudea) & (BigDF.Hourb == AvgDF.Hour), how='left_outer')
BigDF = BigDF.drop('Latitudea','Longitudea', 'Hour')


#Pipeline: filter out by Temp range or Type and create new df, left join the counts onto it, and then output that as it's own .csv


"""
Begin Filtering section for events: code will change based on what is desired
"""
from pyspark.sql.functions import col

#BaseAvg = BigDF.filter(col("Hourb") == -1)

#(9/21, 8/30, 8/25, 4/7, 7/24, 5/9, 9/25, 5/26, 7/4) month/day

#09_21.csv
Avgout = BigDF.filter((col("Monthb")==9) & (col("Dayb")==21))

Countout = TotalDF.filter((col("Month")==9) & (col("Day")==21))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/09_21.csv')


#08_30.csv
Avgout = BigDF.filter((col("Monthb")==8) & (col("Dayb")==30))

Countout = TotalDF.filter((col("Month")==8) & (col("Day")==30))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/08_30.csv')


#08_25.csv
Avgout = BigDF.filter((col("Monthb")==8) & (col("Dayb")==25))

Countout = TotalDF.filter((col("Month")==8) & (col("Day")==25))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/08_25.csv')


#04_07.csv
Avgout = BigDF.filter((col("Monthb")==4) & (col("Dayb")==7))

Countout = TotalDF.filter((col("Month")==4) & (col("Day")==7))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/04_07.csv')


#07_24.csv
Avgout = BigDF.filter((col("Monthb")==7) & (col("Dayb")==24))

Countout = TotalDF.filter((col("Month")==7) & (col("Day")==24))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/07_24.csv')


#05_09.csv
Avgout = BigDF.filter((col("Monthb")==5) & (col("Dayb")==9))

Countout = TotalDF.filter((col("Month")==5) & (col("Day")==9))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/05_09.csv')


#09_25.csv
Avgout = BigDF.filter((col("Monthb")==9) & (col("Dayb")==25))

Countout = TotalDF.filter((col("Month")==9) & (col("Day")==25))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/09_25.csv')


#05_26.csv
Avgout = BigDF.filter((col("Monthb")==5) & (col("Dayb")==26))

Countout = TotalDF.filter((col("Month")==5) & (col("Day")==26))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/05_26.csv')


#07_04.csv
Avgout = BigDF.filter((col("Monthb")==7) & (col("Dayb")==4))

Countout = TotalDF.filter((col("Month")==7) & (col("Day")==4))

outputdf = Avgout.join(Countout, (Avgout.Latitudeb == Countout.Latitude) & (Avgout.Longitudeb == Countout.Longitude) & (Avgout.Hourb == Countout.Hour), how='left_outer')

outputdf = outputdf.drop('Month', 'Day', 'Year','Hour', 'Latitude','Longitude')

outputdf.toPandas().to_csv('/home/andrew/output/events/07_04.csv')


"""
scp -r andrew@10.10.11.35:/home/andrew/output/events /Users/andrew/Desktop
"""

"""
Normalization function written by Justin
"""

minimum = min(df["column_name"])
maximum  = max(df["column_name"])
mean = df["column_name"].mean()
​
​
def reshape(point, smallest = minimum, biggest = maximum, avg = mean):
	'''
	The function reshapes any datapoint in [minimum, maximum] to [0,1] where the
	points at the mean are turned into 1/2. Therefore anything above the mean
	is greater than 1/2.

	That's the basic idea. Not continiously differentiable, but it is continuous.
	'''
	if avg - point > 0:
		return 1/2 * float(smallest - point)/(smallest - avg)
	else:
		return 1/2 * float(avg - point)/(avg - biggest)




"""
scp andrew@10.10.11.35:/home/andrew/output/events/09_21.csv /Users/andrew/Desktop
"""
