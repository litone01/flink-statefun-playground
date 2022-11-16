import argparse
import re
import time
import requests
import sys

DELAY = 2
ISLOOP = True
COUNT = 1

def sendPutRequests(url, headers, data):
    count = 0
    while ISLOOP and count < COUNT:
        requests.put(url=url, headers=headers, json=data)
        count += 1
        print(f"sent {count} request")
        sys.stdout.flush()

        time.sleep(DELAY)

def sendGetRequest(url):
    count = 0
    while ISLOOP and count < COUNT:
        response = requests.get(url=url)
        count += 1
        print(f"response: {response.text}")
        sys.stdout.flush()

        time.sleep(DELAY)

# transform the original id which is unique inside its stream to a unique id among all streams
def transformId(originalId, stream_id):
    return '{}-{}'.format(stream_id, originalId)

def restock(stream_id, item_id, quantity):
    transformed_item_id = transformId(item_id, stream_id)
    headers = {
        'Content-Type': 'application/vnd.com.example/RestockItem',
    }
    data = {"itemId": transformed_item_id, "quantity": quantity}
    url = 'http://localhost:8090/com.example/stock/{}'.format(transformed_item_id)

    print(f"[Stream {stream_id}] sending restock event to {url} \n with data {data}")

    sendPutRequests(url, headers, data)

# TODO: a better way to separate the item status of different streams
def itemStatus(stream_id):
    url = 'http://localhost:8091/items-{}'.format(stream_id)
    print(f"[Stream {stream_id}] getting item status from {url}")

    sendGetRequest(url)

    return

def addToCart(stream_id, user_id, quantity, item_id):
    transformed_item_id = transformId(item_id, stream_id)
    transformed_user_id = transformId(user_id, stream_id)
    headers = {
        'Content-Type': 'application/vnd.com.example/AddToCart',
    }
    data = {"userId": transformed_user_id, "quantity": quantity, "itemId": transformed_item_id}
    url = 'http://localhost:8090/com.example/user-shopping-cart/{}'.format(transformed_user_id)
    
    print(f"[Stream {stream_id}] sending addToCart event to {url}")
    sendPutRequests(url, headers, data)

def checkout(stream_id, user_id):
    transformed_user_id = transformId(user_id, stream_id)
    headers = {
        'Content-Type': 'application/vnd.com.example/Checkout',
    }
    data = {"userId": transformed_user_id}
    url = 'http://localhost:8090/com.example/user-shopping-cart/{}'.format(transformed_user_id)

    print(f"[Stream {stream_id}] sending checkout events to {url}")
    sendPutRequests(url, headers, data)

    return

# TODO: a better way to separate the receipt of different streams
def receipt(stream_id):
    url = 'http://localhost:8091/receipts-{}'.format(stream_id)
    print(f"[Stream {stream_id}] getting receipts from {url}")

    sendGetRequest(url)

    return

def main(args):
    if args.restock:
        restock(args.stream_id, args.item_id, args.quantity)
    elif args.item_status:
        itemStatus(args.stream_id)
    elif args.add_to_cart:
        addToCart(args.stream_id, args.user_id, args.quantity, args.item_id)
    elif args.checkout:
        checkout(args.stream_id, args.user_id)
    elif args.receipt:
        receipt(args.stream_id)

    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # event type config
    parser.add_argument('--restock', action='store_true', help='start the restock producer')
    parser.add_argument('--item_status', action='store_true', help='consume the item status from egress')
    parser.add_argument('--add_to_cart', action='store_true', help='start the addToCart producer')
    parser.add_argument('--checkout', action='store_true', help='start the checkout producer')
    parser.add_argument('--receipt', action='store_true', help='consume the receipts from egress')
    # common event config
    # parser.add_argument('--loop', action='store_true', help='true if the producer needs to run forever')
    # event details
    parser.add_argument('--item_id', default='socks', type=str, help='the item id')
    parser.add_argument('--quantity', default=1, type=int, help='the quantity of the item')
    parser.add_argument('--user_id', default='u1', type=str, help='the user id')
    parser.add_argument('--stream_id', default='s1', type=str, help='the stream id')
    args = parser.parse_args()
    
    # run the main function
    main(args)