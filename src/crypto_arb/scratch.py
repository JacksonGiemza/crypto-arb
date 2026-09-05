import websocket
import json


url = "wss://ws.kraken.com/v2"

subscription = {
    "method": "subscribe",
    "params": {
        "channel": "book",
        "symbol": ["BTC/USD"]
    }
}

ws = websocket.create_connection(url)
ws.send(json.dumps(subscription))

while True:
    message = ws.recv()
    data = json.loads(message)
    if data.get("channel") != "book":
        continue

    updates = data["data"]

    for update in updates:
        print(update)
    