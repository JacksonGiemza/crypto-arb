from crypto_arb.coinbase import CoinbaseAdapter
from crypto_arb.kraken import KrakenAdapter

import asyncio

async def get_update(quote_queue):
    print quote_queue

async def main():
    quote_queue = asyncio.Queue()

    coinbase = CoinbaseAdapter(
        products=["BTC-USD", "ETH-USD"],
        quote_queue=quote_queue,
    )

    kraken = KrakenAdapter(
        products=["BTC/USD", "ETH/USD"],
        quote_queue=quote_queue,
    )

    async with asyncio.TaskGroup() as tg:
        tg.create_task(coinbase.run())
        tg.create_task(kraken.run())

if __name__ == "__main__":
    asyncio.run(main())