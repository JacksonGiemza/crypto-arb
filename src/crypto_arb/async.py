import asyncio
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
import json
import heapq

from crypto_arb.tools import timer, report_stats, add_update_count

url = "wss://advanced-trade-ws.coinbase.com"

subscription = {
    "type": "subscribe",
    "product_ids": ["BTC-USD"],
    "channel": "level2"
}

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
            quantity = update["new_quantity"]

            if side == "bid":
                book = bids
                heap = bid_heap
                order = -price
            else:
                book = asks
                heap = ask_heap
                order = price
            
            if quantity == "0":
                book.pop(price, None)

            else:
                if price not in book:
                    heapq.heappush(heap, order)

                book[price] = quantity

    return update_count

@timer
def log_book(bids, asks, bid_heap, ask_heap):
    # remove stale bids
    while bid_heap and -bid_heap[0] not in bids:
        heapq.heappop(bid_heap)

    # remove stale asks
    while ask_heap and ask_heap[0] not in asks:
        heapq.heappop(ask_heap)
    
    if not bid_heap or not ask_heap:
        return (None, None)

    best_bid_price = -bid_heap[0]
    best_ask_price = ask_heap[0]

    return best_bid_price, best_ask_price

def print_book(best_bid_price, best_ask_price, bids, asks):
    print(f"Best Bid: price: {best_bid_price}, qty: {bids[best_bid_price]}")
    print(f"Best Ask: price: {best_ask_price}, qty: {asks[best_ask_price]}")
    print()

RESET = object()

async def receiver(queue):
    while True:
        try:
            async with connect(url, max_size=None) as ws:
                await queue.put(RESET)
                await ws.send(json.dumps(subscription))

                async for message in ws:
                    await queue.put(message)

        except (ConnectionClosedError, ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)

async def processor(queue):
    bids = {}
    bid_heap = []

    asks = {}
    ask_heap = [] 

    while True:
        message = await queue.get()
        
        if message is RESET:
            bids.clear()
            asks.clear()
            bid_heap.clear()
            ask_heap.clear()
            continue

        data = json.loads(message)

        update_count = apply_updates(
            data, bids, asks, bid_heap, ask_heap
        )
        add_update_count("apply_updates", update_count)

        best_bid_price, best_ask_price = log_book(
            bids, asks, bid_heap, ask_heap
        )

        if best_bid_price is not None and best_ask_price is not None:
            print_book(
                best_bid_price, best_ask_price, bids, asks
            )

async def main():
    queue = asyncio.Queue(maxsize=1000)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(receiver(queue))
        tg.create_task(processor(queue))

if __name__ == "__main__":
    try:
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
        report_stats()
