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