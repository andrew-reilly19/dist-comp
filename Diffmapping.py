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
#totalDay structure: (Month, Day, Year, Latitude, Longitude, count)
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
#totalHour structure: (Month, Day, Year, Hour, Latitude, Longitude, count)

"""
Verification that the flatten didn't lose data - these should equal 4,534,327
"""

totalDayv = totalDay.map(lambda x: x[5])

totalDayv = totalDayv.reduce(lambda x,y: x+y)

print(totalDayv)

totalHourv = totalHour.map(lambda x: x[6])

totalHourv = totalHourv.reduce(lambda x,y: x+y)

print(totalHourv)

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
        z = (float(y[0]),float(y[1]),round(avg,4))
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
        z = (int(y[0]),float(y[1]),float(y[2]),round(avg,4))
        return z
    rdd1 = rdd1.map(convert_back1)
    #Structure: hour, lat, lng, avg_hour
    return rdd1


point_avg_hourly = avgday_h(totalHour)


"""
Normalizing the averages and counts
"""

"""Daily"""
#average
minmax = point_avg_day.map(lambda x: x[2])
da_min = minmax.min()
da_max = minmax.max()

def norm(x):
    val = x[2]
    nval = (val-da_min)/(da_max-da_min)
    return(x[0],x[1],round(nval,8))

PAD_norm = point_avg_day.map(norm)


#count
minmax2 = totalDay.map(lambda x: x[5])
dc_min = minmax2.min()
dc_max = minmax2.max()


def norm2(x):
    val = x[5]
    nval = (val-dc_min)/(dc_max-dc_min)
    return(x[0],x[1],x[2],x[3],x[4],round(nval,8))

PCD_norm = totalDay.map(norm2)


"""Hourly"""
#average
minmax3 = point_avg_hourly.map(lambda x: x[3])
ha_min = minmax3.min()
ha_max = minmax3.max()

def norm3(x):
    val = x[3]
    nval = (val-ha_min)/(ha_max-ha_min)
    return(x[0],x[1],x[2],round(nval,8))

PAH_norm = point_avg_hourly.map(norm3)


#count
minmax4 = totalHour.map(lambda x: x[6])
hc_min = minmax4.min()
hc_max = minmax4.max()


def norm4(x):
    val = x[6]
    nval = (val-hc_min)/(hc_max-hc_min)
    return(x[0],x[1],x[2],x[3],x[4],x[5],round(nval,8))

PCH_norm = totalHour.map(norm4)



"""
Before joining and writing out, each of these needs to be put into a dataframe first
"""
#restructuring totalDay, totalHour, and avgHour RDDs:
PCD_norm1 = PCD_norm.map(lambda x: (x[3],x[4],x[0],x[1],x[2],x[5]))
#Structure: (Latitude, Longitude, Month, Day, Year, Count)

PCH_norm1 = PCH_norm.map(lambda x: (x[4],x[5],x[0],x[1],x[2],x[3],x[6]))
#Structure: (Latitude, Longitude, Month, Day, Year, Count)

PAH_norm1 = PAH_norm.map(lambda x: (x[1],x[2],x[0],x[3]))
#Structure: (Latitude, Longitude, Hour, Average)

#Creating Dataframes from RDDs
TotalDayDF = sqlContext.createDataFrame(PCD_norm1, ['Latitude', 'Longitude', 'Month', 'Day', 'Year', 'Countnorm'])

TotalHourDF = sqlContext.createDataFrame(PCH_norm1, ['Latitude', 'Longitude', 'Month', 'Day', 'Year', 'Hour', 'Countnorm'])

AvgDayDF = sqlContext.createDataFrame(PAD_norm, ['Latitudea', 'Longitudea', 'Averagenorm'])

AvgHourDF = sqlContext.createDataFrame(PAH_norm1, ['Latitudea', 'Longitudea', 'Houra', 'Averagenorm'])

#Joining the Dataframes
DailyData = TotalDayDF.join(AvgDayDF, (TotalDayDF.Latitude == AvgDayDF.Latitudea) & (TotalDayDF.Longitude == AvgDayDF.Longitudea), how='left_outer')

HourlyData = TotalHourDF.join(AvgHourDF, (TotalHourDF.Latitude == AvgHourDF.Latitudea) & (TotalHourDF.Longitude == AvgHourDF.Longitudea) & (TotalHourDF.Hour == AvgHourDF.Houra), how='left_outer')

#Dropping duplicate columns
DailyData = DailyData.drop('Latitudea','Longitudea')

HourlyData = HourlyData.drop('Latitudea', 'Longitudea', 'Houra')

#Structure DailyData: (Latitude, Longitude, Month, Day, Year, Count, Average)
#Structure HourlyData: (Latitude, Longitude, Month, Day, Year, Hour, Count, Average)

#No empties here!(need to get right package for this to work, don't remember what it is)
#
# DailyData.filter(f.col('Average').isNull()).show()
#
# HourlyData.filter(f.col('Average').isNull()).show()

"""
Writing out data to csv file
Starter Code provided by Simona
"""
# avg_df = sqlContext.createDataFrame(point_avg_day, ['Latitude', 'Longitude', 'Average'])
# avg_df.toPandas().to_csv('/home/andrew/df_avg.csv')

DailyData.toPandas().to_csv('/home/andrew/DailyData.csv')

HourlyData.toPandas().to_csv('/home/andrew/HourlyData.csv')


"""
Bash commands to scp data out to my HD:
DailyData = 4.8 MB
HourlyData = 27 MB

scp andrew@10.10.11.35:/home/andrew/DailyData.csv /Users/andrew/Desktop
scp andrew@10.10.11.35:/home/andrew/HourlyData.csv /Users/andrew/Desktop
"""
