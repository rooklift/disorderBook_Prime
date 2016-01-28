from websocket import create_connection

url = "ws://127.0.0.1:8001/ob/api/ws/NOISEBOTS/venues/TESTEX/executions/stocks/FOOBAR"
ws = create_connection(url)

while 1:

    try:
        raw_food = ws.recv()
    except:
        ws = create_connection(url)
        continue

    print(raw_food)
