from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
import asyncio
import json
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
        self.books = self._init_books()
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
                self.books = self._init_books()
                continue

            data = json.loads(message)

            updated_products = self._apply_updates(data, self.books)

            self._rebalance_book(self.books, updated_products)

            self._print_top_of_book(self.books, updated_products)

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.receiver())
            tg.create_task(self.processor())
    
    def _apply_updates(self, data, books):
        """
        {'symbol': 'BTC/USD', 'bids': [{'price': 79610.0, 'qty': 0.0}, {'price': 79599.4, 'qty': 0.06}, {'price': 79605.3, 'qty': 0.0932249}], 'asks': [], 'checksum': 552779255, 'timestamp': '2026-09-05T03:31:20.832897Z'}
        """
        if data.get("channel") != 'book':
            return 0, set()

        events = data["data"]

        updated_products = set()
        for event in events:

            product = event["symbol"]
            product_book = books[product]
            updated_products.add(self.product)

            for bid in data["bids"]:
                bid_book = product_book['bids']
                bid_heap = product_book['bid_heap']

                price = bid["price"]
                quantity = bid["qty"]

                if quantity == 0:
                    bid_book.pop(price, None)

                else:
                    if price not in bid_book:
                        heapq.heappush(bid_heap, -price)

                    bid_book["price"] = quantity

            for ask in data["asks"]:
                ask_book = product_book['asks']
                ask_heap = product_book['ask_heap']

                price = ask["price"]
                quantity = ask["qty"]

                if quantity == 0:
                    ask_book.pop(price, None)

                else:
                    if price not in ask_book:
                        heapq.heappush(ask_heap, -price)

                    ask_book["price"] = quantity

    def _rebalance_book(self, books, updated_products):
        pass

    def _print_top_of_book(self, books, updated_products):
        pass

    def _init_books(self):
        books = {}
        for product in self.products:
            books[product] = {
                "bids": {},
                "bid_heap": [],
                "asks": {},
                "ask_heap": [],
            }
        return books

if __name__ == "__main__":
    kraken = KrakenAdapter(products=["BTC/USD"])

    try:
        asyncio.run(kraken.run())
        
    except KeyboardInterrupt:
        print("Exiting WebSocket..")