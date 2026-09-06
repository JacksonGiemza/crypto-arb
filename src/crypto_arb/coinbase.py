from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from crypto_arb.book_processing import rebalance_book, print_top_of_book, init_books
# from crypto_arb.tools import add_update_count, report_stats
import asyncio
import json
import heapq

class SequenceError(Exception):
    pass

class CoinbaseAdapter:
    def __init__(self, products):
        self.URL = "wss://advanced-trade-ws.coinbase.com"
        self.subscription = {
            "type": "subscribe",
            "product_ids": products,
            "channel": "level2"
        }
        self.products = products
        self.queue = asyncio.Queue(maxsize=1000)
        self.books = init_books(products)
        self.RESET = object()
        self.last_sequence = None

    async def receiver(self):
        async with connect(self.URL, max_size=None) as ws:
            await self.queue.put(self.RESET)
            await ws.send(json.dumps(self.subscription))

            async for message in ws:
                await self.queue.put(message)

    async def processor(self):
        while True:
            message = await self.queue.get()
            
            if message is self.RESET:
                self.books = init_books(self.products)
                self.last_sequence = None
                continue

            data = json.loads(message)

            updated_products = self._apply_updates(data, self.books)
            # add_update_count("_apply_updates", update_count)
            rebalance_book(self.books, updated_products)
            print_top_of_book(self.books, updated_products)

    async def run(self):
        while True:
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.receiver())
                    tg.create_task(self.processor())

            except* SequenceError as eg:
                print("Sequence check failed, reconnecting...")
                self.books = init_books(self.products)
                self.queue = asyncio.Queue(maxsize=1000)
                await asyncio.sleep(1)

            except* (ConnectionClosedError, ConnectionClosedOK) as eg:
                print("Connection closed, reconnecting...")
                self.books = init_books(self.products)
                self.queue = asyncio.Queue(maxsize=1000)
                await asyncio.sleep(1)

    def _apply_updates(self, data, books):
        sequence_num = data.get("sequence_num")
        self._validate_sequence(sequence_num)

        if data.get("channel") != 'l2_data':
            return set()

        events = data["events"]

        updated_products = set()
        # update_count = 0
        for event in events:
            product = event["product_id"]
            product_book = books[product]
            updated_products.add(product)
            for update in event["updates"]:
                # update_count += 1

                side = update["side"]
                price = float(update["price_level"])
                quantity = update["new_quantity"]

                if side == "bid":
                    book = product_book["bids"]
                    heap = product_book["bid_heap"]
                    order = -price
                else:
                    book = product_book["asks"]
                    heap = product_book["ask_heap"]
                    order = price
                
                if quantity == "0":
                    book.pop(price, None)

                else:
                    if price not in book:
                        heapq.heappush(heap, order)

                    book[price] = quantity

        return updated_products

    def _validate_sequence(self, sequence_num):
        if sequence_num is None:
            return

        if self.last_sequence is not None:
            if sequence_num != self.last_sequence + 1:
                raise SequenceError(
                    f"expected={self.last_sequence + 1}, actual={sequence_num}"
                )
                
        self.last_sequence = sequence_num


if __name__ == "__main__":
    coinbase = CoinbaseAdapter(products=["BTC-USD", "ETH-USD"])

    try:
        asyncio.run(coinbase.run())
        
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
        # report_stats()
