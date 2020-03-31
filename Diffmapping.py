"""
First, Justin's BuildData function is here:
"""
def BuildData():
    '''
    This function builds the entire dataset (all months in 2014)

    Returns the spark RDD
    '''
    def reformat_2014(line):
        '''
        Takes a line as input, and reformats into a proper shape.

        Returns an array with the following shape:
        [Month, Day, Year, Hour, Minute, Second, Latitude, Longitude, Base]
        '''
        # Break up into the differet columns seperated by commas
        total = line.split(",")
        #Remove the quotes
        date_time = total[0].replace('"',"").split(" ")
        date = date_time[0].split("/")
        date = [int(x) for x in date]
        # Checking for date errors
        time = date_time[1].split(":")
        time = [int(x) for x in time]
        # Recast the other columns
        lat = float(total[1])
        lon = float(total[2])
        # Finally create the output
        date.extend(time)
        date.append(lat)
        date.append(lon)
        date.append(total[3].replace('"',""))
        return date
    # Builds the actual data
    months =  ["may14", "sep14", "apr14", "jul14", "aug14", "jun14"]
    count = 0
    for month in months:
        first = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-"+ month +".csv")
        header = first.take(1)
        second = first.filter(lambda x: False if x == header[0] else True)
        third = second.map(reformat_2014)
        if count == 0:
            total = third
            count += 1
        else:
            total = total.union(third)
    return total


total = BuildData()
#structure: [Month, Day, Year, Hour, Minute, Second, Latitude, Longitude, Base]
"""
Now, creating an RDD flattend down to the day:
"""

#Flattens data to each day, up to two decimal points for the lat/lng
def flatten_to_day(rdd):
    """
    This funciton will 'flatten' the RDD so that records now have a count in them,
    for each pickup that happened at that lat/lng for that specific day
    """
    #first, mapping each element to exclude hour (x[3]), minute (x[4]), second(x[5]), and base(x[8])
    #also rounding the lat/lng (x[6] and x[7]) to 2 decimal plcaces(removes 2 decimal places)
    rdd1 = rdd.map(lambda x: (x[0],x[1],x[2],round(x[6],2),round(x[7],2)))
    #mapping again to put original info into a string for the reduceByKey function later and adding 1
    rdd1 = rdd1.map(lambda x: (str(x).strip('[]'),1))
    rdd1 = rdd1.reduceByKey(lambda x,n: x+n)
    #structure: ('(Month, Day, Year, Latitude, Longitude)',count)
    def convert_back(x):
        #This function takes the previous output and converts the data out of the string
        y = x[0]
        y = y.strip(')')
        y = y.strip('(')
        y = y.split(',')
        #mapping back with additional 1 at the end of each record for averaging
        z = (int(y[0]),int(y[1]),int(y[2]),float(y[3]),float(y[4]),x[1])
        return z
    rdd1 = rdd1.map(convert_back)
    #structure: (Month, Day, Year, Latitude, Longitude, count)
    return rdd1


totalDay = flatten_to_day(total)
#structure: (Month, Day, Year, Latitude, Longitude, count)
"""
Same as above, but creates an RDD flattend to hour:
"""

#Flattens data to each hour, up to two decimal points for the lat/lng
def flatten_to_hour(rdd):
    """
    This funciton will 'flatten' the RDD so that records now have a count in them,
    for each pickup that happened at that lat/lng for that specific hour and day
    """
    #first, mapping each element to exclude minute (x[4]), second(x[5]), and base(x[8])
    #also rounding the lat/lng (x[6] and x[7]) to 3 decimal plcaces
    rdd1 = rdd.map(lambda x: (x[0],x[1],x[2],x[3],round(x[6],2),round(x[7],2)))
    #mapping again to put original info into a string for the reduceByKey function later and adding 1
    rdd1 = rdd1.map(lambda x: (str(x).strip('[]'),1))
    rdd1 = rdd1.reduceByKey(lambda x,n: x+n)
    #structure: ('(Month, Day, Year, Hour, Latitude, Longitude)',count)
    def convert_back(x):
        #This function takes the previous output and converts the data out of the string
        y = x[0]
        y = y.strip(')')
        y = y.strip('(')
        y = y.split(',')
        #mapping back with additional 1 at the end of each record for averaging
        z = (int(y[0]),int(y[1]),int(y[2]),int(y[3]),float(y[4]),float(y[5]),x[1])
        return z
    rdd1 = rdd1.map(convert_back)
    #structure: (Month, Day, Year, Hour, Latitude, Longitude, count)
    return rdd1


totalHour = flatten_to_hour(total)
#structure: (Month, Day, Year, Hour, Latitude, Longitude, count)
"""
Now, to take this and get the average pickups at given lat/lng for our data
"""
#the first function here simply returns the number of days in the dataset (important for finding average)
def numdays(rdd):
    def daymap(x):
        #y1 value should be unique for each day: Month*31 +Day +Year*365
        y1 = x[0]*31+x[1]+x[2]*365
        return(y1,1)
    days = rdd.map(daymap)
    days = days.reduceByKey(lambda x,n: x+n)
    numdays = days.count()
    return(numdays)


ndays = numdays(totalDay)
#This function will return the same # of days for total, totalDay, and totalHour
#183 days


"""
Finally, the function to find the average for each lat/lng per day:
This follows a very similar approach as the flatten formula, but first putting the lat/lng into a string
and then reducing by key on that value to sum the counts and dividing by the number of days in the dataset.
"""

def avgday(rdd):
    #we can ignore the month/day/year, since they've already been incorporated.
    def prep_rdd(x):
        lat=str(x[3])
        lng=str(x[4])
        latlng = lat+','+lng
        count = x[5]
        return (latlng,count)
    rdd1 = rdd.map(prep_rdd)
    rdd1 = rdd1.reduceByKey(lambda x,n: x+n)
    def convert_back1(x):
        #This function takes the previous output and converts the data out of the string
        y = x[0]
        y = y.split(',')
        avg = x[1]/183 #number of days found earlier
        #mapping back with additional 1 at the end of each record for averaging
        z = (float(y[0]),float(y[1]),avg)
        return z
    rdd1 = rdd1.map(convert_back1)
    #Structure: lat, lng, avg_day
    return rdd1


point_avg_day = avgday(totalDay)


"""
This function does essentially the same thing as above, but is adjusted to get the daily average
for each lat/lng point for each hour.  This will give us roughly 24 entries for each lat/lng point.
"""

def avgday_h(rdd):
    #we can ignore the month/day/year, since they've already been incorporated.
    def prep_rdd(x):
        hour=str(x[3])
        lat=str(x[4])
        lng=str(x[5])
        latlng = hour+','+lat+','+lng
        count = x[6]
        return (latlng,count)
    rdd1 = rdd.map(prep_rdd)
    rdd1 = rdd1.reduceByKey(lambda x,n: x+n)
    def convert_back1(x):
        #This function takes the previous output and converts the data out of the string
        y = x[0]
        y = y.split(',')
        avg = x[1]/183 #number of days found earlier
        #mapping back with additional 1 at the end of each record for averaging
        z = (int(y[0]),float(y[1]),float(y[2]),avg)
        return z
    rdd1 = rdd1.map(convert_back1)
    #Structure: lat, lng, avg_day
    return rdd1


point_avg_hourly = avgday_h(totalHour)


"""
Writing out data to csv file
Code provided by Simona
"""
avg_df = sqlContext.createDataFrame(point_avg_day, ['Latitude', 'Longitude', 'Average'])

avg_df.toPandas().to_csv('/home/andrew/df_avg.csv')

"""
attempts to write out rdds to .csv files - not currently working

from pyspark import SparkContext
avgday_df = sqlContext.createDataFrame(point_avg_day, ['Latitude', 'Longitude', 'Average'])


#different method
schema = StructType([StructField(str(i), StringType(), True) for i in range(32)])
df = sqlContext.createDataFrame(rdd, schema)

"""
