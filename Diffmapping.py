"""

Pseudo code for aggregate images:

1. write a function to 'flatten' data - either by doing a ReduceByKey(sum) on
  the lat/lng pairs, or by rounding the lat/lng pairs by one decimal point and
  then mapping it, whichever makes more sense

1.(B) Look into finding some way to categorize each lat/lng point into something
  similar to the 2015 data - which has single locations instead of lat/lng

2. do one giant heatmap on the entire dataset, using the flatten function
  on the entire dataset - this can then be stored as it's own RDD and act as the
  "base case" of data
NOTE: This would work for a locational analysis, and not a temporal analysis.

2.(B) do the same as part 2, except using the average of days for temporal analysis?
  This part needs a bit more thought.
    Can possibly be done by instead grouping by the lat/lng points, and finding
    the average for each point/location per day

3. pull in weather data, either from NOAA dataset I found or from the Kaggle
  dataset, and map each day's data with the weather/temperature/wind speed/etc.
  of the day.  This can then be used to create RDDs of the base case for the different
  types of weather/temperature/etc.
    (Also, do this for the average per day per location)

4. Compare the various weather RDDs against the base case - the base case will be
  0 at every point, and the weather RDDs will either add to the average or subtract
  from the average of 0.  Then this can be plotted to show relative increases by position
  due to weather or other factors.  (Blue will be less than normal/average, Orange
  greater than)

"""
#First, Justin's BuildData function is here:
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

#Using Justin's BuildData function above
total = BuildData()
#structure: [Month, Day, Year, Hour, Minute, Second, Latitude, Longitude, Base]

#TODO: tweak this to flatten to the hour instead
def flatten_to_day(rdd):
    """
    This funciton will 'flatten' the RDD so that records now have a count in them,
    for each pickup that happened at that lat/lng for that specific day
    """
    #first, mapping each element to exclude hour (x[3]), minute (x[4]), second(x[5]), and base(x[8])
    #also rounding the lat/lng (x[6] and x[7]) to 3 decimal plcaces
    rdd1 = rdd.map(lambda x: (x[0],x[1],x[2],round(x[6],3),round(x[7],3)))
    #mapping again to put original info into a string for the reduceByKey function later and adding 1
    rdd1 = rdd1.map(lambda x: (str(x).strip('[]'),1))
    rdd1 = rdd1.reduceByKey(lambda x,n: x+n)
    return rdd1


total1 = flatten_to_day(total)
#structure: ('(Month, Day, Year, Latitude, Longitude)',count)

def convert_back(x):
    #This function takes the previous output and converts the data out of the string
    y = x[0]
    y = y.strip(')')
    y = y.strip('(')
    y = y.split(',')
    #mapping back with additional 1 at the end of each record for averaging
    z = (int(y[0]),int(y[1]),int(y[2]),float(y[3]),float(y[4]),x[1],1)
    return z


total2 = total1.map(convert_back)
#structure: (Month, Day, Year, Latitude, Longitude, count, 1)

"""
Now, to take this and get the average pickups at given lat/lng for our data
"""













#hi
