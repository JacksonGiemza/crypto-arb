import heapq
# from crypto_arb.tools import timer

def init_books(products):
        books = {}
        for product in products:
            books[product] = {
                "bids": {},
                "bid_heap": [],
                "asks": {},
                "ask_heap": [],
            }
        return books

# @timer
def rebalance_book(books, updated_products):
    if not updated_products:
        return 
    
    for product in updated_products:
        book = books[product]
        bids = book["bids"]
        asks = book["asks"]
        bid_heap = book["bid_heap"]
        ask_heap = book["ask_heap"]
            
        # remove stale bids
        while bid_heap and -bid_heap[0] not in bids:
            heapq.heappop(bid_heap)

        # remove stale asks
        while ask_heap and ask_heap[0] not in asks:
            heapq.heappop(ask_heap)

def print_top_of_book(books, updated_products):
    if not updated_products:
        return
    
    for product in updated_products:
        book = books[product]
        if not book["bid_heap"] or not book["ask_heap"]:
            continue

        bids = book['bids']
        asks = book['asks']
        best_bid_price = -book["bid_heap"][0]
        best_ask_price = book["ask_heap"][0]
        
        print(f"{product} | Bid: ${best_bid_price}, {bids[best_bid_price]} QTY | Ask: ${best_ask_price}, {asks[best_ask_price]} QTY")

# @timer
# def apply_updates(data, books):
#     if data.get("channel") != 'l2_data':
#         return 0, set()

#     events = data["events"]

#     updated_products = set()
#     update_count = 0
#     for event in events:

#         product = event["product_id"]
#         product_book = books[product]
#         updated_products.add(product)

#         for update in event["updates"]:
#             update_count += 1

#             side = update["side"]
#             price = float(update["price_level"])
#             quantity = update["new_quantity"]

#             if side == "bid":
#                 book = product_book["bids"]
#                 heap = product_book["bid_heap"]
#                 order = -price
#             else:
#                 book = product_book["asks"]
#                 heap = product_book["ask_heap"]
#                 order = price
            
#             if quantity == "0":
#                 book.pop(price, None)

#             else:
#                 if price not in book:
#                     heapq.heappush(heap, order)

#                 book[price] = quantity

#     return update_count, updated_products