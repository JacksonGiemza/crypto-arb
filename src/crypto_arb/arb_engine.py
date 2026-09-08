from crypto_arb.coinbase import CoinbaseAdapter
from crypto_arb.kraken import KrakenAdapter

from itertools import permutations
from decimal import Decimal
import asyncio
import time


class ArbEngine:
    def __init__(self):
        self.products = ['BTC-USD', 'ETH-USD']
        self.exchanges = ['coinbase', 'kraken']
        self.quote_queue = asyncio.Queue(maxsize=1000)
        self.quotes = self.init_quotes(["BTC-USD", "ETH-USD"], ["coinbase", "kraken"])

    def match(self, quotes):
        fees = {
            "coinbase": Decimal("0.004"),
            "kraken": Decimal("0.004"),
        }

        def get_raw_edge(buy_quote, sell_quote):
            buy_price = buy_quote["ask"]
            sell_price = sell_quote["bid"]

            ask_qty = buy_quote["ask_qty"]
            bid_qty = sell_quote["bid_qty"]
            
            size = min(ask_qty, bid_qty)

            raw_edge = sell_price - buy_price

            return raw_edge, size

        for product in quotes:
            coinbase_quote = quotes[product]["coinbase"]
            kraken_quote = quotes[product]["kraken"]

            if coinbase_quote == {} or kraken_quote == {}:
                continue

            now = time.time_ns()
            coinbase_time = coinbase_quote["local_ts"]
            kraken_time = kraken_quote["local_ts"]

            coinbase_age = (now - coinbase_time) / 1_000_000
            kraken_age = (now - kraken_time) / 1_000_000

            time_diff = abs(coinbase_age - kraken_age)

            if kraken_age > 1_000 or coinbase_age > 1_000:
                continue

            if time_diff > 1_000:
                continue

            for exchange_pair in permutations(self.exchanges, 2):
                buy_exchange, sell_exchange = exchange_pair

                buy_quote = quotes[product][buy_exchange]
                sell_quote = quotes[product][sell_exchange]

                raw_edge, size = get_raw_edge(buy_quote,sell_quote)

                buy_fee = buy_quote["ask"] * size * fees[buy_exchange]
                sell_fee = sell_quote["bid"] * size * fees[sell_exchange]

                gross_profit = raw_edge * size

                net_profit = gross_profit - buy_fee - sell_fee

                if net_profit > 0:
                    print(f"{product} | buy {buy_exchange}, sell {sell_exchange} | gross: {gross_profit}, net: {net_profit}")

    async def get_update(self):
        while True:
            quote = await self.quote_queue.get()
            if quote != {}:
                product = quote["symbol"]
                exchange = quote["exchange"]

                self.quotes[product][exchange] = quote

            self.match(self.quotes)
            
    async def main(self):
        coinbase = CoinbaseAdapter(
            products=["BTC-USD", "ETH-USD"],
            quote_queue=self.quote_queue,
        )

        kraken = KrakenAdapter(
            products=["BTC/USD", "ETH/USD"],
            quote_queue=self.quote_queue,
        )

        async with asyncio.TaskGroup() as tg:
            tg.create_task(coinbase.run())
            tg.create_task(kraken.run())
            tg.create_task(self.get_update())

    def init_quotes(self, products, exchanges):
        quotes = {}
        for product in products:
            quotes[product] = {}
            for exchange in exchanges:
                quotes[product][exchange] = {}
        return quotes


if __name__ == "__main__":
    engine = ArbEngine()
    try:
        asyncio.run(engine.main())
    except KeyboardInterrupt:
        print("Exiting WebSockets..")
