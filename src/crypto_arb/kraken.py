from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from crypto_arb.book_processing import init_books, rebalance_book, print_top_of_book
import asyncio
import json
from decimal import Decimal
import heapq

class KrakenAdapter:
    def __init__(self, products):
        self.URL = "wss://ws.kraken.com/v2"
        self.subscription = {
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": products
            }
        }
        self.products = products
        self.queue = asyncio.Queue(maxsize=1000)
        self.books = init_books(products)
        self.RESET = object()

    async def receiver(self):
        while True:
            try:
                async with connect(self.URL, max_size=None) as ws:
                    await self.queue.put(self.RESET)
                    await ws.send(json.dumps(self.subscription))

                    async for message in ws:
                        await self.queue.put(message)
            except (ConnectionClosedError, ConnectionClosedOK):
                print('Connection closed, retrying..')
                await asyncio.sleep(1)

    async def processor(self):
        while True:
            message = await self.queue.get()
            
            if message is self.RESET:
                self.books = init_books(self.products)
                continue

            data = json.loads(message, parse_float=Decimal)

            updated_products = self._apply_updates(data, self.books)
            rebalance_book(self.books, updated_products)
            print_top_of_book(self.books, updated_products)

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.receiver())
            tg.create_task(self.processor())
    
    def _apply_updates(self, data, books):
        if data.get("channel") != 'book':
            return set()

        events = data["data"]

        updated_products = set()
        for event in events:

            product = event["symbol"]
            product_book = books[product]
            updated_products.add(product)

            bid_book = product_book['bids']
            bid_heap = product_book['bid_heap']

            ask_book = product_book['asks']
            ask_heap = product_book['ask_heap']

            for bid in event["bids"]:
                price = bid["price"]
                quantity = bid["qty"]

                self._update_level(price, quantity, bid_book, bid_heap, -price)

            for ask in event["asks"]:
                price = ask["price"]
                quantity = ask["qty"]

                self._update_level(price, quantity, ask_book, ask_heap, price)
                    
        return updated_products

    def _update_level(self, price, quantity, book, heap, order):
            if quantity == 0:
                book.pop(price, None)
            else:
                if price not in book:
                    heapq.heappush(heap, order)

                book[price] = quantity


if __name__ == "__main__":
    kraken = KrakenAdapter(products=["BTC/USD"])

    try:
        asyncio.run(kraken.run())
        
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
