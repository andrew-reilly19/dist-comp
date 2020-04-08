import pandas as pd
import numpy as np
import plotly.express as px


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


def plotday(day = 'all', hour = -2, method='jnorm'):
    path = '/Users/andrew/Desktop/events/'
    if day == 'all':
        csvs = ['04_07','05_09','05_26','07_04','07_24','08_25','08_30','09_21','09_25']
    if day != 'all':
        csvs = [day]

    if hour == -2:
        hours = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
    if hour != -2:
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
            writepath = path + file + '/hour'+str(h)+'.html'
            fig.write_html(writepath)


plotday(day='07_04', hour = 10, method = 'jnorm')




