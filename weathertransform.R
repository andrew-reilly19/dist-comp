library(tidyverse)

setwd('/Users/andrew/non_icloud_ncf/distcomp_project/historical-hourly-weather-data')

temperature <- read_csv('temperature.csv')
nytemp <- select(temperature, c("datetime", "New York"))

wdesc <- read_csv('weather_description.csv')
nydesc <- select(wdesc, c("datetime", "New York"))

fixdate <- function(df){
  df1 <- separate(data=df, col ='datetime',into = c('date','time'),sep = ' ')
  df1 <- separate(data=df1, col='date', into = c('year', 'month', 'day'), sep = '-')
  df1 <- separate(data=df1, col='time', into = c('hour', 'minute', 'second'), sep = ':')
  df1 <- select(df1, -c('minute', 'second'))
  return(df1)
}

nytemp <- fixdate(nytemp)
nydesc <- fixdate(nydesc)

pmonths <- c('04','05','06','07','08','09')

nytemp1 <- filter(nytemp, year=='2014' & month %in% pmonths)
nydesc1 <- filter(nydesc, year=='2014' & month %in% pmonths)

nytemp1 <- transform(nytemp1, year=as.integer(year), month=as.integer(month), day=as.integer(day), hour=as.integer(hour))
nydesc1 <- transform(nydesc1, year=as.integer(year), month=as.integer(month), day=as.integer(day), hour=as.integer(hour))

nyweather <- merge(nytemp1, nydesc1, by=c('year', 'month', 'day', 'hour'))

colnames(nyweather) <- c("Year", "Month", "Day", "Hour", "TempC", "Type")

nyweather$TempC <- round((nyweather$TempC-273.15),2)

View(nyweather)

typeof(nyweather[,4])

write_csv(nyweather, 'filteredweather.csv')
