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


def plotweather(type = 'all'):
    path = '/Users/andrew/non_icloud_ncf/distcomp_project/weather/'
    if type == 'all':
        types = ['broken_clouds','scattered_clouds','mist','clear','light_rain','few_clouds','overcast','moderate_rain','haze','fog','heavy_rain']
    if type != 'all':
        types = [type]
    for t in types:
        file = path + t + '.csv'
        wtdf = pd.read_csv(file)

        wtdf['diff']= wtdf.apply(lambda row: (row['Count']-row['Average']), axis=1)
        wtdf.drop(['Count','Average'], inplace=True, axis = 1)
        wtdf2=wtdf.groupby(['Lat', 'Lng'], as_index=False).sum()

        mi = min(wtdf2["diff"])
        mx  = max(wtdf2["diff"])
        mn = wtdf2["diff"].mean()
        wtdf2['diffnorm'] = wtdf.apply (lambda row: (reshape(row['diff'], mi, mx, mn)), axis=1)

        fig = px.density_mapbox(wtdf2, lat='Lat', lon='Lng', z='diffnorm', radius=10, center=dict(lat=40.730, lon=-73.935), zoom=8.3, mapbox_style="stamen-terrain")
        writepath = path + t + '.html'
        fig.write_html(writepath)


plotweather(type='mist')

plotweather()
