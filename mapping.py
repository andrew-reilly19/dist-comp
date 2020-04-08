import pandas as pd
import numpy as np
import plotly.express as px

#the reshape function here is Justin's normalization function
def reshape(point, smallest, biggest, avg):
	'''
	The function reshapes any datapoint in [minimum, maximum] to [0,1] where the
	points at the mean are turned into 1/2. Therefore anything above the mean
	is greater than 1/2.

	That's the basic idea. Not continiously differentiable, but it is continuous.
	'''
	if avg - point > 0:
		return 1/2 * float(smallest - point)/(smallest - avg)
	else:
		return (1/2 * float(avg - point)/(avg - biggest))+.5

"""
This function now uses the downloaded folder from the spark server to create a variety of .html heatmaps.  In the base condition,
it will create heatmaps for each hour of each day in the events folder you can download from the server with the command:
    'scp -r andrew@10.10.11.35:/home/andrew/output/events /Users/andrew/Desktop'
    (of course, change andrew to your name and the output folder to wherever you want it)
The functionality is there however for if we get the sliders working and only want to produce one html at a time.

There are currently 3 methods for the weight in the map: Justin's normalization function ('jnorm', the default), Andrew's normalization function
('anorm', which is basic - just (value-largest absolute value)/largest absolute value), and the straight count ('count')
"""
def plotday(day = 'all', hour = -1, method='jnorm'):
    #change this path to your downloaded folder
    path = '/Users/andrew/Desktop/events/'
    #if a day is not specified, it will default to outputting every day
    if day == 'all':
        csvs = ['04_07','05_09','05_26','07_04','07_24','08_25','08_30','09_21','09_25']
    if day != 'all':
        csvs = [day]

    #if hour is -1, this will output htmls for every hour of every specified day
    if hour == -1:
        hours = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
    if hour != -1:
        hours = [hour]

    for file in csvs:
        filename = path + file + '.csv'
        currentdf = pd.read_csv(filename)
        currentdf.drop(columns=['Unnamed: 0','Yearb','Monthb','Dayb','TempC','Type'], inplace=True)
        for h in hours:
            dfhour = currentdf[(currentdf.Hourb == h)]
            dfhour['Count'].fillna(0,inplace=True)
            dfhour['diff'] = dfhour.apply (lambda row: (row['Count']-row['Average']), axis=1)
            if method == 'anorm':
                absmax=max(abs(dfhour["diff"]))
                dfhour['diffnorm'] = dfhour.apply (lambda row: ((row['diff']+absmax)/absmax), axis=1)
            if method == 'jnorm':
                mi = min(dfhour["diff"])
                mx  = max(dfhour["diff"])
                mn = dfhour["diff"].mean()
                dfhour['diffnorm'] = dfhour.apply (lambda row: (reshape(row['diff'], mi, mx, mn)), axis=1)
            if method == 'count':
                mi = min(dfhour["diff"])
                dfhour['diffnorm'] = dfhour.apply (lambda row: (dfhour['diff']+mi), axis=1)
            fig = px.density_mapbox(dfhour, lat='Latitudeb', lon='Longitudeb', z='diffnorm', radius=10, center=dict(lat=40.730, lon=-73.935), zoom=8.3, mapbox_style="stamen-terrain")

            #note: you will need to create a folder with the exact same name as the date, e.g. 04_07. Otherwise the following path will not work:
            writepath = path + file + '/hour'+str(h)+'.html'
            fig.write_html(writepath)

#see note about writepath 4 lines above before running this
plotday(day='07_04', hour = -1)




