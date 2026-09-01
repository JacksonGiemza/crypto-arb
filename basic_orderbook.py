import websocket
import json
import heapq
import time

timer_stats = {}

def timer(func):
    def wrapper(*args,**kwargs):
        name = func.__name__

        if name not in timer_stats:
            timer_stats = {
                "calls": 0,
                "total_ns": 0,
                "avg_ns": 0,
                "min_ns": float("inf"),
                "max_ns": -float("inf"),
            }

        start = time.perf_counter_ns()

        result = func(*args,**kwargs)

        end = time.perf_counter_ns()

        elapsed = end - start

        timer_stats[name]["calls"] += 1
        timer_stats[name]["total_ns"] += elapsed

        if elapsed < timer_stats[name]["min_ns"]:
            timer_stats[name]["min_ns"] = elapsed

        if elapsed > timer_stats[name]["max_ns"]:
            timer_stats[name]["max_ns"] = elapsed
            
        return result
    return wrapper

url = "wss://advanced-trade-ws.coinbase.com"

subscription = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "level2"
}

ws = None

def apply_updates(data, bids, asks, bid_heap, ask_heap):
    if data.get('channel') != 'l2_data':
        return

    events = data["events"]

    for event in events:
        for update in event["updates"]:

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

    print(f"Best Bid: price: {best_bid_price}, qty: {bids[best_bid_price]}")
    print(f"Best Ask: price: {best_ask_price}, qty: {asks[best_ask_price]}")
    print()

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

            apply_updates(data, bids, asks, bid_heap, ask_heap)
            
            log_book(bids, asks, bid_heap, ask_heap)

    except KeyboardInterrupt:
        break

    except Exception as e:
        print("WebSocket error: ", e)
        print("Refreshing book")
        continue

    finally:
        if ws:
            ws.close()
        print("Connection closed")