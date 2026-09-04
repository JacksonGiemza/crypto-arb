from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from crypto_arb.book_processing import apply_updates, rebalance_book, print_top_of_book
from crypto_arb.tools import add_update_count, report_stats
import asyncio
import json

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

            update_count, updated_products = apply_updates(data, self.books)
            add_update_count("apply_updates", update_count)
            rebalance_book(self.books, updated_products)
            print_top_of_book(self.books, updated_products)

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.receiver())
            tg.create_task(self.processor())

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
    coinbase = CoinbaseAdapter(products=["BTC-USD", "ETH-USD"])

    try:
        asyncio.run(coinbase.run())
        
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
        report_stats()
