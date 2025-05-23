import queue
import time
import random
import datetime

import pyarrow as pa
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, State, ctx
import pyarrow.compute as pc

from mockiproducer import Producer
from mockiConsumer import Consumer

import pyarrow as pa

producer: Producer | None = None
consumer: Consumer | None = None
ipc_queue = queue.Queue()
all_tables: list[pa.Table] = []

TYPE_MAP = {
    "Integer":           pa.int64(),
    "Float":             pa.float64(),
    "Uppercase String":  pa.string(),
    "Lowercase String":  pa.string(),
    "Datetime":          pa.timestamp("ns"),
}

consumer = Consumer()



ipc_queue = queue.Queue()

# Consumer will put each pa.Table into our ipc_queue

# We'll accumulate all batches here to build a growing table
all_tables = []


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.COSMO],
)

app.layout = dbc.Container(fluid=True, className="py-4", children=[
    dbc.Row(
        dbc.Col(
            html.H1("⇞ Arrow Streaming Dashboard"),
            width=8, className="offset-md-2 text-center"
        )
    ),

    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Controls"),
                dbc.CardBody([
                    dbc.Label("Data Type"),
                    dcc.Dropdown(
                        id="type-dropdown",
                        options=[{"label": t, "value": t} for t in [
                            "Integer", "Float",
                            "Uppercase String", "Lowercase String", "Datetime"
                        ]],
                        value="Integer",
                        clearable=False
                    ),
                    dbc.ButtonGroup([
                        dbc.Button("Start", id="start-button", color="success", className="me-2"),
                        dbc.Button("Stop",  id="stop-button",  color="danger")
                    ], className="mt-3")
                ])
            ]),
            width=4
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Live Stream Output"),
                dbc.CardBody(
                    html.Pre(
                        id="output-area",
                        style={
                            "whiteSpace": "pre-wrap",
                            "fontFamily": "monospace",
                            "fontSize": "0.9rem",
                            "height": "300px",
                            "overflowY": "auto",
                        }
                    )
                )
            ]),
            width=8
        )
    ], className="mt-4"),

    # interval to tick every second; we'll enable/disable it via callbacks
    dcc.Interval(id="interval", interval=1_000, disabled=True),
])


@app.callback(
    Output("interval", "disabled"),
    Input("start-button", "n_clicks"),
    Input("stop-button",  "n_clicks"),
    State("type-dropdown", "value"),
    prevent_initial_call=True,
)
def toggle_interval(start_clicks, stop_clicks, typ):
    global producer, consumer, all_tables, ipc_queue
    trigger = ctx.triggered_id
    if trigger == "start-button":
        dtype   = TYPE_MAP[typ]
        schema  = pa.schema([("value", dtype)])

        producer = Producer(schema)
        consumer = Consumer()

        # spawn consumer to write tables into the NEW queue
        consumer.spawn(callback=lambda tbl: ipc_queue.put(tbl))

        all_tables.clear()
        return False 
    if trigger == "stop-button":
        producer.end_stream()
        consumer.close_consuming()
        producer = None
        consumer = None
        ipc_queue = queue.Queue()  # new empty queue
        all_tables.clear()
        return True
    
    return True

producer = None

@app.callback(
    Output("output-area", "children"),
    Input("interval", "n_intervals"),
    State("type-dropdown", "value"),
    prevent_initial_call=True,
)
def stream_step(n_intervals, conversion_type):
    # generate a dummy value for the selected type
    if conversion_type == "Integer":
        val = random.randint(0, 100)
    elif conversion_type == "Float":
        val = round(random.random() * 100, 3)
    elif conversion_type == "Uppercase String":
        val = f"STR{random.randint(0, 999)}"
    elif conversion_type == "Lowercase String":
        val = f"str{random.randint(0, 999)}"
    else:  # Datetime → real datetime with ns precision
        val = datetime.datetime.now()

    # producer.schema = schema
    # producer  = Producer(schema=schema)
    # global producer
    producer.input_anchor(conversion_type, val)
    # producer.end_stream()

    # read one resulting table from the queue
    try:
        tbl = ipc_queue.get(timeout=1)
    except queue.Empty:
        return "[!] No data received this tick."

    # accumulate and concat so we show the full history
    all_tables.append(tbl)
    combined_table = pa.concat_tables(all_tables)

    # render as native Arrow text
    if conversion_type == "Datetime":
        col = combined_table.column("value") 
        try:
            str_arr = pc.strftime(col, format="%Y-%m-%d %H:%M:%S.%f")
        except pa.ArrowInvalid:
            # drop timezone if tzdata missing
            str_arr = pc.strftime(col, format="%Y-%m-%d %H:%M:%S.%f")
        # ── 2.  Intentionally parse with a format missing “.%f”
        try:
            bad_parse = pc.strptime(str_arr,
                                    format="%Y-%m-%d %H:%M:%S",
                                    unit="us")
            # on a fixed build this succeeds; on an old build it raises ArrowInvalid
            display = pa.Table.from_arrays([bad_parse], names=["value"]).to_string()
        except pa.ArrowInvalid as exc:
            display = f"ArrowInvalid (old bug triggered): {exc}"

        return display
    return str(combined_table)


if __name__ == "__main__":
    app.run(debug=True)
