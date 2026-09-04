from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
from crypto_arb.processing import apply_updates, log_book, print_book
from crypto_arb.tools import add_update_count, report_stats
import asyncio
import json

class CoinbaseAdapter:
    def __init__(self, URL, products, subscription):
        self.URL = URL,
        self.subscription = json.dumps(subscription)
        self.products = products,

        self.queue = asyncio.Queue(maxsize=1000)
        self.books = {}
        self.RESET = object()

    async def receiver(self):
        while True:
            try:
                async with connect(self.URL, max_size=None) as ws:
                    await self.queue.put(self.RESET)
                    await ws.send(self.subscription)

                    async for message in ws:
                        await self.queue.put(message)

            except (ConnectionClosedError, ConnectionClosedOK):
                print('Connection closed, retrying..')
                await asyncio.sleep(1)

    async def processor(self):
        bids = {}
        bid_heap = []

        asks = {}
        ask_heap = [] 

        while True:
            message = await self.queue.get()
            
            if message is self.RESET:
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

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.receiver())
            tg.create_task(self.processor())
        