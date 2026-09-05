import websocket
from decimal import Decimal
import json


url = "wss://ws.kraken.com/v2"

subscription = {
    "method": "subscribe",
    "params": {
        "channel": "book",
        "symbol": ["BTC/USD","ETH/USD"]
    }
}

ws = websocket.create_connection(url)
ws.send(json.dumps(subscription))

while True:
    message = ws.recv()
    data = json.loads(message, parse_float=Decimal)
    if data.get("channel") != "book":
        continue
    
    updates = data["data"]

    print(updates)
    break