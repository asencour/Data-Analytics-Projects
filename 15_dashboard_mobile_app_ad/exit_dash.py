import sys
import getopt
from datetime import datetime
import pandas as pd
import plotly.graph_objs as go


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
#from sqlalchemy import create_engine

# предобработка

mobile_dataset = pd.read_csv('mobile_dataset.csv')
mobile_sources = pd.read_csv('mobile_sources.csv')
mobile_dataset.columns = ['event_time', 'event_name', 'user_id']
mobile_sources.columns = ['user_id', 'source']

# Обработка дат
mobile_dataset['event_time'] = pd.to_datetime(mobile_dataset['event_time'])
mobile_dataset['date'] = mobile_dataset['event_time'].dt.date
mobile_dataset['date'] = pd.to_datetime(mobile_dataset['date'], format = '%Y-%m-%d')
mobile_dataset = mobile_dataset[['event_name', 'user_id', 'date']]


# Обработка дубликатов в типах событий
def events_type(i):
    if i == 'show_contacts':
        new_name = 'contacts_show'
        return new_name
    elif 'search' in i:
        new_name = 'search'
        return new_name
    return i

mobile_dataset['event_name'] = mobile_dataset['event_name'].apply(events_type)

# Объединение датасетов
df = mobile_dataset.merge(mobile_sources, on = 'user_id', how = 'left')



# задаём лейаут
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, compress=False)
app.layout = html.Div(children=[  

	# формируем html
	html.H1(children = 'Дашборд для мобильного приложения "Ненужные вещи"'),
	html.Br(),  

	html.Div('''
			Этот дашборд показывает распределение и количество событий в приложении "Ненужные вещи" по дням
			'''), 
	html.Br(),

	# Input
	html.Div([

		html.Div([
			html.Label('Диапазон дат: '),
			dcc.DatePickerRange(
				start_date = df['date'].min(),
				end_date = df['date'].max(),
				display_format = 'YYYY-MM-DD',
				id = 'date_selector',
			),
		], className = 'three columns'),

		html.Div([
			html.Label('Тип события: '),
			dcc.Dropdown(
				id = 'event_dropdown',
				options = [{'label': x, 'value': x} for x in df['event_name'].unique()],
				value = df['event_name'].unique(),
				multi = True
			),
		], className = 'four columns'),

		html.Div([
			html.Label('Источник: '),
			dcc.Dropdown(
				id = 'source_dropdown',
				options = [{'label': x, 'value': x} for x in df['source'].unique()],
				value = df['source'].unique(),
				multi = True
			),
		], className = 'three columns'),

		html.Div([
			html.Label('Режим отображения: '),
			dcc.RadioItems(
				options = [
							{'label': 'События', 'value': 'event_name'},
							{'label': 'Источник', 'value': 'source'},
							],
				value = 'event_name',
				id = 'mode_selector'
			),

		], className = 'two columns')

	]),

	# Output
	html.Div([
		html.Div([
			html.Label('Количество событий по дням'),
			dcc.Graph(
				id = 'event_number',
				style = {'height': '30vw'},
			),
		], className = 'six columns'),
		html.Div([
			html.Label('Количество пользователей по дням'),
			dcc.Graph(
				id = 'users_sum',
				style = {'height': '30vw'},
			),
		], className = 'six columns'),

	], className = 'row'),

	html.Div([
		html.Label('Распределение количества событий по типу'),
		dcc.Graph(
			id = 'event_spread',
			style = {'height': '50vw'},
			),
	], className = 'twelve columns'),
])

# Описываем логику дашборда
@app.callback(
	[
	Output('event_number', 'figure'),
	Output('users_sum', 'figure'),
	Output('event_spread', 'figure')
	],
	[
	Input('date_selector', 'start_date'),
	Input('date_selector', 'end_date'),
	Input('event_dropdown', 'value'),
	Input('source_dropdown', 'value'),
	Input('mode_selector', 'value'),
	])

def update_figures(start_date, end_date, selected_events, selected_sources, grouping_column):

	#filtered_data = df.copy()
	filtered_data = (df
		.query('date >= @start_date and date <= @end_date and \
			event_name in @selected_events and \
			source in @selected_sources')
		)


	#grouping_column = 'event_name'
	event_history = filtered_data.groupby(['date', grouping_column]).agg({'user_id': 'count'}).rename(columns = {'user_id': 'event_number'})
	event_history = event_history.reset_index()


	events_history_data = []
	for current_name in event_history[grouping_column].unique():
		current_data = event_history[event_history[grouping_column] == current_name]
		events_history_data += [go.Scatter(x = current_data['date'],
											y = current_data['event_number'],
											mode = 'lines',
											stackgroup = 'one',
											line = {'width': 1},
											name = current_name)]

	users_number_data = []
	users_number = filtered_data.groupby(['date', grouping_column]).agg({'user_id': 'nunique'}).rename(columns = {'user_id': 'users_count'})
	users_number = users_number.reset_index()
	for current_name in users_number[grouping_column].unique():
		current_data = users_number[users_number[grouping_column] == current_name]
		users_number_data += [go.Bar(x = users_number['date'],
							y = users_number['users_count'],
							name = current_name)
	]

	event_spread_data = []
	event_spread = filtered_data.groupby(['date', 'event_name']).agg({'user_id': 'count'}).reset_index().rename(columns = {'user_id': 'event_number'})
	#event_spread['number'] = 1
	for current_name in event_spread['event_name'].unique():
		current = event_spread.query('event_name == @current_name')
		event_spread_data += [go.Box(y = current['event_number'], name = current_name)
		]

	
	return (
			#event_number
				{
					'data': events_history_data,
					'layout': go.Layout(xaxis = {'title': 'Дата'},
										yaxis = {'title': 'Число событий'},
										hovermode = 'closest')
				},
			#users_number
				{
					'data': users_number_data,
					'layout': go.Layout(xaxis = {'title': 'Дата'},
										yaxis = {'title': 'Число пользователей'},
										barmode='stack',
										hovermode = 'closest')

				},

			#event_spread

				{
					'data': event_spread_data,
					'layout': go.Layout(xaxis = {'title': 'Дата'},
										yaxis = {'title': 'Число событий'},
										#hovermode = 'closest'
										)
				},

		)




if __name__ == '__main__':
	app.run_server(debug = True, port=8051, host='0.0.0.0')