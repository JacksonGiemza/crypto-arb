import websocket
import json
import heapq

from crypto_arb.tools import timer, report_stats, add_update_count

url = "wss://advanced-trade-ws.coinbase.com"

subscription = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "level2"
}

ws = None

@timer
def apply_updates(data, bids, asks, bid_heap, ask_heap):
    if data.get('channel') != 'l2_data':
        return

    events = data["events"]

    update_count = 0

    for event in events:
        for update in event["updates"]:
            update_count += 1

            side = update["side"]
            price = float(update["price_level"])
            quantity = float(update["new_quantity"])

            book = bids if side == "bid" else asks
            heap = bid_heap if side == "bid" else ask_heap
            order = -price if side == "bid" else price
            
            if quantity == 0:
                book.pop(price, None)

            else:
                if price not in book:
                    heapq.heappush(heap, order)

                book[price] = quantity

    return update_count

def print_book(best_bid_price, best_ask_price):
    print(f"Best Bid: price: {best_bid_price}, qty: {bids[best_bid_price]}")
    print(f"Best Ask: price: {best_ask_price}, qty: {asks[best_ask_price]}")
    print()

@timer
def log_book(bids, asks, bid_heap, ask_heap):
    # remove stale bids
    while bid_heap and -bid_heap[0] not in bids:
        heapq.heappop(bid_heap)

    # remove stale asks
    while ask_heap and ask_heap[0] not in asks:
        heapq.heappop(ask_heap)
    
    if not bid_heap or not ask_heap:
        return

    best_bid_price = -bid_heap[0]
    best_ask_price = ask_heap[0]

    return best_bid_price, best_ask_price

while True:
    try:
        ws = websocket.create_connection(url)
        ws.send(json.dumps(subscription))

        bids = {}
        bid_heap = []

        asks = {}
        ask_heap = []

        while True:
            message = ws.recv()
            data = json.loads(message)

            update_count = apply_updates(data, bids, asks, bid_heap, ask_heap)
            add_update_count("apply_updates", update_count)
            
            best_bid_price, best_ask_price = log_book(bids, asks, bid_heap, ask_heap)

            print_book(best_bid_price, best_ask_price)

    except KeyboardInterrupt:
        report_stats()
        break

    except Exception as e:
        print("WebSocket error: ", e)
        print("Refreshing book")
        continue

    finally:
        if ws:
            ws.close()
        print("Connection closed")