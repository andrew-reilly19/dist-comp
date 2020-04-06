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
        z = (int(y[0]),int(y[1]),int(y[2]), -1, float(y[3]),float(y[4]),x[1])
        return z
    rdd1 = rdd1.map(convert_back)
    #structure: (Month, Day, Year, Hour(-1 for whole day), Latitude, Longitude, count)
    return rdd1


totalDay = flatten_to_day(total)
#totalDay structure: (Month, Day, Year, Hour(-1), Latitude, Longitude, count)
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

totalcount = totalHour.union(totalDay)

"""
Verification that the flatten didn't lose data - these should equal 4,534,327
"""

# totalDayv = totalDay.map(lambda x: x[5])
#
# totalDayv = totalDayv.reduce(lambda x,y: x+y)
#
# print(totalDayv)
#
# totalHourv = totalHour.map(lambda x: x[6])
#
# totalHourv = totalHourv.reduce(lambda x,y: x+y)
#
# print(totalHourv)

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

#not needed anymore with changes that make the daily average equal to the -1 hour
#def avgday(rdd):
    #we can ignore the month/day/year/hour, since they've already been incorporated.
    # def prep_rdd(x):
    #     lat=str(x[4])
    #     lng=str(x[5])
    #     latlng = lat+','+lng
    #     count = x[6]
    #     return (latlng,count)
    # rdd1 = rdd.map(prep_rdd)
    # rdd1 = rdd1.reduceByKey(lambda x,n: x+n)
    # def convert_back1(x):
    #     #This function takes the previous output and converts the data out of the string
    #     y = x[0]
    #     y = y.split(',')
    #     avg = x[1]/183 #number of days found earlier
    #     #mapping back with additional 1 at the end of each record for averaging
    #     z = (float(y[0]),float(y[1]),round(avg,4))
    #     return z
    # rdd1 = rdd1.map(convert_back1)
    # #Structure: lat, lng, avg_day
    # return rdd1


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
point_avg_day = avgday_h(totalDay)

"""
Here we create a null dataset for every possible lat/lng point (should have 5442 entries)
And a second null dataset for every possible lat/lng point and hour (should have 130608 entries)
"""

#first map total to include only lat/lng points to two decimal places
#then reduceByKey to get just lat/lng points
#then make new one by creating hours and full joining them (?)

def llmap(x):
    a=str(round(x[6],2))
    b=str(round(x[7],2))
    y=a+','+b
    return(y,0)


aLL = total.map(llmap)

aLL = aLL.reduceByKey(lambda x,n: x+n)


# def remap_avg(x):
#     rkey=str(x[0])+','+str(x[1])
#     return(rkey, x[2])
#
#
# avgday = point_avg_day.map(remap_avg)
#
# LLd=aLL.union(avgday)
#
# LLd=LLd.reduceByKey(lambda x,n: x+n)
#
#
# def unmapd(x):
#     y=x[0]
#     y=y.split(',')
#     z = (float(y[0]),float(y[1]),x[1])
#     return z
#
#
# LLd=LLd.map(unmapd)
#Structure: (Latitude,Longitude,Average)

"""
hourly
"""
hr=sc.parallelize([-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23])

aLLhr=aLL.cartesian(hr)

def hrmap(x):
    y=x[0]
    hour=x[1]
    hour=str(hour)
    ll=y[0]
    ll2=ll.split(',')
    rkey=ll2[0]+','+ll2[1]+','+hour
    zeros=float(y[1])
    return(rkey,zeros)


aLLhr=aLLhr.map(hrmap)

def remap_havg(x):
    rkey=str(x[1])+','+str(x[2])+','+str(x[0])
    return(rkey, float(x[3]))

avghr = point_avg_hourly.map(remap_havg)
avgday = point_avg_day.map(remap_havg)


LLh=aLLhr.union(avghr)
LLh=LLh.union(avgday)

LLh=LLh.reduceByKey(lambda x,n: x+n)


def unmap(x):
    y=x[0]
    y=y.split(',')
    z = (float(y[0]),float(y[1]),int(y[2]),x[1])
    return z


LLh=LLh.map(unmap)
#Structure: (Latitude,Longitude,Hour,Average)


"""
Normalizing the averages and counts
Note: minimum has been set to 0 because there are omitted locations in this dataset with no pickups (although they still count)
Note2: This has been commented out because normalization is not being applied properly - needs to be done AFTER
    subtracting count and average
"""
#def hidden:
    """Daily"""
    #average
    """
    minmax = point_avg_day.map(lambda x: x[2])
    da_min = 0
    da_max = minmax.max()

    def norm(x):
        val = x[2]
        nval = (val-da_min)/(da_max-da_min)
        return(x[0],x[1],round(nval,8))

    PAD_norm = point_avg_day.map(norm)


    #count
    minmax2 = totalDay.map(lambda x: x[5])
    dc_min = 0
    dc_max = minmax2.max()


    def norm2(x):
        val = x[5]
        nval = (val-dc_min)/(dc_max-dc_min)
        return(x[0],x[1],x[2],x[3],x[4],round(nval,8))

    PCD_norm = totalDay.map(norm2)
    """

    """Hourly"""

    """
    #average
    minmax3 = point_avg_hourly.map(lambda x: x[3])
    ha_min = 0
    ha_max = minmax3.max()

    def norm3(x):
        val = x[3]
        nval = (val-ha_min)/(ha_max-ha_min)
        return(x[0],x[1],x[2],round(nval,8))

    PAH_norm = point_avg_hourly.map(norm3)


    #count
    minmax4 = totalHour.map(lambda x: x[6])
    hc_min = 0
    hc_max = minmax4.max()


    def norm4(x):
        val = x[6]
        nval = (val-hc_min)/(hc_max-hc_min)
        return(x[0],x[1],x[2],x[3],x[4],x[5],round(nval,8))

    PCH_norm = totalHour.map(norm4)
    """


    """
    Before joining and writing out, each of these needs to be put into a dataframe first
    """
    # #restructuring totalDay, totalHour, and avgHour RDDs:
    # PCD_norm1 = PCD_norm.map(lambda x: (x[3],x[4],x[0],x[1],x[2],x[5]))
    # #Structure: (Latitude, Longitude, Month, Day, Year, Count)
    #
    # PCH_norm1 = PCH_norm.map(lambda x: (x[4],x[5],x[0],x[1],x[2],x[3],x[6]))
    # #Structure: (Latitude, Longitude, Month, Day, Year, Count)
    #
    # PAH_norm1 = PAH_norm.map(lambda x: (x[1],x[2],x[0],x[3]))
    # #Structure: (Latitude, Longitude, Hour, Average)

"""
putting things into a dataframe
"""

TotalDF = sqlContext.createDataFrame(totalcount, ['Month', 'Day', 'Year', 'Hour', 'Latitude', 'Longitude', 'Count'])

AvgDF = sqlContext.createDataFrame(LLh, ['Latitudea', 'Longitudea', 'Hour', 'Average'])


#Creating Dataframes from RDDs
#totalHour structure: (Month, Day, Year, Hour, Latitude, Longitude, count)

#TotalDayDF = sqlContext.createDataFrame(totalDay, ['Month', 'Day', 'Year', 'Latitude', 'Longitude', 'Count'])

#TotalHourDF = sqlContext.createDataFrame(totalHour, ['Month', 'Day', 'Year', 'Hour', 'Latitude', 'Longitude', 'Count'])

#AvgDayDF = sqlContext.createDataFrame(LLd, ['Latitudea', 'Longitudea', 'Average'])

#AvgHourDF = sqlContext.createDataFrame(LLh, ['Latitudea', 'Longitudea', 'Hour', 'Average'])

#Joining the Dataframes
#def joining (hidden):
    """ Not joining dataframes anymore - will do that in regular python just prior to mapping
    #DailyData = TotalDayDF.join(AvgDayDF, (TotalDayDF.Latitude == AvgDayDF.Latitudea) & (TotalDayDF.Longitude == AvgDayDF.Longitudea), how='full')
    #DailyData has nearly 1M rows

    #HourlyData = TotalHourDF.join(AvgHourDF, (TotalHourDF.Latitude == AvgHourDF.Latitudea) & (TotalHourDF.Longitude == AvgHourDF.Longitudea) & (TotalHourDF.Hour == AvgHourDF.Houra), how='left_outer')
    #HourlyData would be too unwieldy, nearly 24M entries

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

"""
Writing out data to csv file
Starter Code provided by Simona
"""
# avg_df = sqlContext.createDataFrame(point_avg_day, ['Latitude', 'Longitude', 'Average'])
# avg_df.toPandas().to_csv('/home/andrew/df_avg.csv')

#TotalDayDF.toPandas().to_csv('/home/andrew/output/DailyCount.csv')

#TotalHourDF.toPandas().to_csv('/home/andrew/output/HourlyCount.csv')

#AvgDayDF.toPandas().to_csv('/home/andrew/output/AvgDay.csv')

#AvgHourDF.toPandas().to_csv('/home/andrew/output/AvgHours.csv')

"""
hourly data now goes to get weather info first, see weatherinput.py
"""

#HourlyData.toPandas().to_csv('/home/andrew/HourlyData.csv')


"""
Bash commands to scp data out to my HD:
DailyData = 4.8 MB
HourlyData = 27 MB

scp andrew@10.10.11.35:/home/andrew/DailyData.csv /Users/andrew/Desktop
scp andrew@10.10.11.35:/home/andrew/HourlyData.csv /Users/andrew/Desktop
"""
