import websocket
from decimal import Decimal
import json


url = "wss://advanced-trade-ws.coinbase.com"
subscription = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "level2"
}

ws = websocket.create_connection(url)
ws.send(json.dumps(subscription))

while True:
    message = ws.recv()
    data = json.loads(message)
    if data.get("channel") != "l2_data":
        continue
    
    print(data)
    break