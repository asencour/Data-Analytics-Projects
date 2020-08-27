#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import getopt
from datetime import datetime
import pandas as pd
import plotly.graph_objs as go


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from sqlalchemy import create_engine



#Задаём входные параметры
unixOptions = "s:e:"
gnuOptions = ["start_dt=", "end_dt="]
  
fullCmdArguments = sys.argv
argumentList = fullCmdArguments[1:] #excluding script name
   
try:
	arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
except getopt.error as err:
	print (str(err))
	sys.exit(2)
    
#start_dt = '2019-09-24 18:00:00' 
#end_dt = '2019-09-24 19:00:00'
start_dt = ''
end_dt = ''

for currentArgument, currentValue in arguments:
	if currentArgument in ("-s", "--start_dt"):
		start_dt = currentValue
	elif currentArgument in ("-e", "--end_dt"):
		end_dt = currentValue
   
db_config = {'user': 'my_user',         
			'pwd': 'my_user_password', 
			'host': 'localhost',       
			'port': 5432,              
			'db': 'zen'}             
    
connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'], db_config['pwd'], db_config['host'], db_config['port'], db_config['db'])
engine = create_engine(connection_string)
    
# получаем сырые данные
query = ''' SELECT * , TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' AS dt
		FROM log_raw
		WHERE TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
	'''.format(start_dt, end_dt)
    
data_raw = pd.io.sql.read_sql(query, con = engine, index_col = 'event_id')

#трансформируем к нужным типам
data_raw['dt'] = pd.to_datetime(data_raw['dt']).dt.round('min')

# Готовим агрегирующие таблицы
# История событий

dash_engagement = (data_raw
	.groupby(['item_topic', 'age_segment', 'dt', 'event'])
	.agg({'user_id':'nunique'})
	.reset_index()
	)
dash_engagement.rename(columns = {'user_id': 'unique_users'}, inplace=True)

# Воронка

dash_visits = (data_raw
	.groupby(['item_topic', 'source_topic', 'age_segment', 'dt'])
	.agg({'event':'count'})
	.reset_index()
	)
dash_visits.rename(columns = {'event': 'visits'}, inplace=True)

#dash_engagement = dash_engagement.fillna(0).reset_index()
#dash_visits = dash_visits.fillna(0).reset_index()


# Удаление старых записей и запись данных в нужные таблицы
tables = {'dash_visits': dash_visits, 'dash_engagement': dash_engagement}

for table_name, table_data in tables.items():
	query = '''
				DELETE FROM {} WHERE dt BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
			'''.format(table_name, start_dt, end_dt)
	engine.execute(query)
	table_data.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)

# задаём лейаут
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, compress=False)
app.layout = html.Div(children=[  
    
    # формируем html
	html.H1(children = 'Дашборд для Яндекс.Дзен'),
	html.Br(),  

	html.Div('''
			Анализ пользовательского взаимодействия с карточками статей
			'''), 
	html.Br(),

	# Input
	html.Div([
		html.Div([
			html.Label('Фильтр тем карточек'),
			dcc.Dropdown(
				id = 'item_topic_dropdown',
				options = [{'label': x, 'value': x} for x in dash_visits['item_topic'].unique()],
				value = dash_visits['item_topic'].unique(),
				multi = True
			),
		], className = 'four columns'),


		html.Div([
			html.Label('Фильтр возрастных категорий'),
			dcc.Dropdown(
				id = 'age-dropdown',
				options = [{'label': x, 'value': x} for x in dash_visits['age_segment'].unique()],
				value = dash_visits['age_segment'].unique(),
				multi = True
			),
		], className = 'four columns'),

		html.Div([
			html.Label('Фильтр даты и времени'),
			dcc.DatePickerRange(
				start_date = dash_visits['dt'].min(),
				end_date = dash_visits['dt'].max(),
				display_format = 'YYYY-MM-DD',
				id = 'dt_selector',
			),
		], className = 'four columns'),
	], className = 'row'),
	html.Br(),

	# Output
	html.Div([
		html.Div([
			html.Label('График истории событий по темам карточек'),
			dcc.Graph(
				id = 'history-absolute-visits',
				style = {'height': '50vw'},
			),
		], className = 'six columns'),		
		html.Div([
			html.Label('Разбивка событий по темам источников'),
			dcc.Graph(
				id = 'pie-visits',
				style = {'height': '25vw'},
			),
		], className = 'six columns'),
		html.Div([
			html.Label('График средней глубины взаимодействия'),
			dcc.Graph(
				id = 'engagement-graph',
				style = {'height': '25vw'},
			),
		], className = 'six columns'),
	], className = 'row'),
])


# Описываем логику дашборда
@app.callback(
	[
	Output('history-absolute-visits', 'figure'),
	Output('pie-visits', 'figure'),
	Output('engagement-graph', 'figure'),
	],
	[
	Input('item_topic_dropdown', 'value'),
	Input('age-dropdown', 'value'),
	Input('dt_selector', 'start_date'),
	Input('dt_selector', 'end_date'),
	])

def update_figures(selected_item_topics, selected_ages, start_date, end_date):

	filtered_data = (dash_visits
		.query('item_topic in @selected_item_topics and \
			dt >= @start_date and dt <= @end_date \
			and age_segment in @selected_ages')
		)

	#dash_engagement['total'] = dash_engagement['unique_users'].unique().max()
	#dash_engagement['avg_unique_users'] = (dash_engagement['unique_users'] / dash_engagement['total'] * 100).round(1)
	filtered_data2 = (dash_engagement
		.query('age_segment in @selected_ages and \
			dt >= @start_date and dt <= @end_date')
		)

	grouping_column = 'item_topic'
	history = (filtered_data
		.groupby([grouping_column, 'dt'])
		.agg({'visits': 'sum'})
		.reset_index()
		)


	history_data = []

	for current_name in history[grouping_column].unique():
		current_data = history[history[grouping_column] == current_name]
		history_data += [go.Scatter(x = current_data['dt'],
									y = current_data['visits'],
									mode = 'lines',
									stackgroup = 'one',
									line = {'width': 1},
									name = current_name)]

	visits_data = []
	visits = (filtered_data
		.groupby('source_topic')
		.agg({'visits': 'sum'})
		.reset_index()
		)
	visits_data = [go.Pie(labels = visits['source_topic'],
							values = visits['visits'],
							#name = 'any name'
							)]


	engagement_data = []
	engagement = (filtered_data2
		.groupby('event')
		.agg({'unique_users': 'mean'})
		.reset_index()
		.sort_values(by='unique_users', ascending=False)
		)
	engagement['total'] = engagement['unique_users'].unique().max()
	engagement['avg_unique_users'] = (engagement['unique_users'] / engagement['total'] * 100).round(1)
	engagement_data = [go.Bar(x = engagement['event'],
							y = engagement['avg_unique_users'])
	] 
	return (
		# history-absolute-visits
		{
			'data': history_data,
			'layout': go.Layout(xaxis = {'title': 'Время'},
								yaxis = {'title': 'История событий'},
								#hovermode = 'closest'
								)
		},

		#pie-visits
		{
			'data': visits_data,

		},

		#engagement-graph
		{
			'data': engagement_data,
			'layout': go.Layout(xaxis = {'title': 'Тип события'},
								yaxis = {'title': '% от показов'},
								hovermode = 'closest')			
		}

		)


if __name__ == '__main__':
	app.run_server(debug = True, host='0.0.0.0')