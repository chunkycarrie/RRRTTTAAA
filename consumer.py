from kafka import KafkaConsumer
import json
import dash
from dash import dcc, html
from dash.dependencies import Output, Input
from dash import dash_table
import pandas as pd

app = dash.Dash(__name__)

data_store = [] 

def create_consumer():
    consumer = KafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def consume_messages(consumer):
    global data_store
    message =  next(consumer, None)
    data_store = message.value

app.layout = html.Div([
    html.H1("Real-time Kafka Data Dashboard"),
    dcc.Interval(id='interval-component', interval=5*1000, n_intervals=0),  # Aktualizacja co 5 sekunda
    html.Div(id='live-update-content')
])

@app.callback(Output('live-update-content', 'children'),
              Input('interval-component', 'n_intervals'))

def update_content(n):
    consume_messages(consumer)
    try:
        df = pd.DataFrame(data_store)
    except:
        df = pd.DataFrame()
    
    if not df.empty:
        return [
            html.H2('Customer Profile'),
            dash_table.DataTable(
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict('records')
            )
        ]
    else:
        return html.Div("No data to display")

if __name__ == "__main__":
    consumer = create_consumer()
    app.run_server(debug=True, use_reloader=False)