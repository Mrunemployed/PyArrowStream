import queue
import random
import datetime

import pyarrow as pa
import pyarrow.compute as pc

import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, State, ctx

from mockiproducer  import Producer
from mockiConsumer  import Consumer
from streamhub import stream_pipeline          # <— holds the shared Queue

TYPE_MAP = {
    "Integer":           pa.int64(),
    "Float":             pa.float64(),
    "Uppercase String":  pa.string(),
    "Lowercase String":  pa.string(),
    "Datetime":          pa.timestamp("ns"),
}

producer:  Producer | None = None
consumer:  Consumer | None = None
ipc_queue = queue.Queue()        # Python std-lib queue
all_tables: list[pa.Table] = []                # accumulate batches

app = Dash(__name__, external_stylesheets=[dbc.themes.COSMO])

app.layout = dbc.Container(fluid=True, className="py-4", children=[
    dbc.Row(
        dbc.Col(html.H1("⇞ Arrow Streaming Dashboard"),
                width=8, className="offset-md-2 text-center")
    ),

    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Controls"),
                dbc.CardBody([
                    dbc.Label("Data Type"),
                    dcc.Dropdown(
                        id="type-dropdown",
                        options=[{"label": t, "value": t} for t in TYPE_MAP],
                        value="Integer",
                        clearable=False
                    ),
                    dbc.ButtonGroup([
                        dbc.Button("Start", id="start-button",
                                   color="success", className="me-2"),
                        dbc.Button("Stop",  id="stop-button",  color="danger")
                    ], className="mt-3")
                ])
            ]),
            width=4
        ),
        dbc.Col(
            dbc.Card([
                dbc.CardHeader("Live Stream Output"),
                dbc.CardBody(html.Pre(
                    id="output-area",
                    style={
                        "whiteSpace": "pre-wrap",
                        "fontFamily": "monospace",
                        "fontSize": "0.9rem",
                        "height": "300px",
                        "overflowY": "auto",
                    }
                ))
            ]),
            width=8
        )
    ], className="mt-4"),

    # ticks every second – enabled / disabled by callbacks
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
    """
    • On “Start”  → spin up Producer & Consumer (once) and enable ticks  
    • On “Stop”   → send EOF, pause consumer, disable ticks
    """
    global producer, consumer, all_tables, ipc_queue

    trigger = ctx.triggered_id

    if trigger == "start-button":
        dtype  = TYPE_MAP[typ]
        schema = pa.schema([("value", dtype)])

        # first-time initialisation
        if producer is None:
            # stream_pipeline._pipeline is the shared asyncio.Queue
            producer = Producer(schema)
            producer.start_streaming()                    # start thread
            consumer = Consumer()
            consumer.spawn(callback=lambda tbl: ipc_queue.put(tbl))
            consumer.start_streaming()
        # Producer doesn’t require active gating for push_task()
        # but we refresh its schema for safety
        producer.schema = schema 

        all_tables.clear()
        return False       # enable Interval

    if trigger == "stop-button":
        if producer:
            producer.stop_streaming()
            producer = None

        if consumer:
            consumer.stop_streaming()
            # consumer.stop_consuming()
            consumer = None

        ipc_queue = queue.Queue()
        all_tables.clear()

        return True

    return True            # default (disabled)


@app.callback(
    Output("output-area", "children"),
    Input("interval", "n_intervals"),
    State("type-dropdown", "value"),
    prevent_initial_call=True,
)
def stream_step(_, conversion_type):
    """
    Every tick:
    1. Generate a dummy value of the selected type
    2. Feed it to the producer
    3. Pop one Arrow table off ipc_queue
    4. Concatenate and pretty-print
    """
    if producer is None:
        return "[!] Click Start to begin streaming."

    if conversion_type == "Integer":
        val = random.randint(0, 100)
    elif conversion_type == "Float":
        val = round(random.random() * 100, 3)
    elif conversion_type == "Uppercase String":
        val = f"STR{random.randint(0, 999)}"
    elif conversion_type == "Lowercase String":
        val = f"str{random.randint(0, 999)}"
    else:  # Datetime
        val = datetime.datetime.now()

    producer.input_anchor(conversion_type, val)

    # ---------- get batch from consumer ----------
    try:
        tbl = ipc_queue.get(timeout=1)
    except queue.Empty:
        return "[!] No data received this tick."

    all_tables.append(tbl)
    combined = pa.concat_tables(all_tables)

    # ---------- render nicely ----------
    if conversion_type == "Datetime":
        col = combined.column("value")
        str_arr = pc.strftime(col, format="%Y-%m-%d %H:%M:%S.%f")
        return str(pa.Table.from_arrays([str_arr], names=["value"]))
    return str(combined)


if __name__ == "__main__":
    app.run(debug=True)
