import websocket
import json

url = "wss://advanced-trade-ws.coinbase.com"

ws = websocket.create_connection(url)

subscription = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "level2"
}

ws.send(json.dumps(subscription))

bids = {}
asks = {}

try:
    while True:
        message = ws.recv()
        data = json.loads(message)
        channel = data.get('channel')
        if channel != 'l2_data':
            continue

        events = data["events"]

        for event in events:
            for update in event["updates"]:
                side = update["side"]
                price = float(update["price_level"])
                quantity = float(update["new_quantity"])

                if side == "bid":
                    book = bids
                else:
                    book = asks

                if quantity == 0:
                    book.pop(price, None)
                else:
                    book[price] = quantity
        if bids and asks:
            best_bid = max(bids)
            best_ask = min(asks)

            print(f"Best Bid: price: {best_bid}, qty: {bids[best_bid]}")
            print(f"Best Ask: price: {best_ask}, qty: {asks[best_ask]}")
            print()
except Exception as e:
    print("WebSocket error: ", e)

finally:
    ws.close()
    print("Connection closed")