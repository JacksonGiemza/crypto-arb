import websocket
import json

url = "wss://advanced-trade-ws.coinbase.com"

ws = websocket.create_connection(url)

subscription = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "ticker"
}

ws.send(json.dumps(subscription))

while True:
    message = ws.recv()
    data = json.loads(message)

    channel = data.get("channel")

    if channel == "ticker":
        prod = data['events'][0]['tickers'][0]['product_id']
        price = data['events'][0]['tickers'][0]['price']
        print(f"{prod}: {price}")
    


