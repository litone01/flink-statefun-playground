import argparse
import re
from this import d
import time
import requests
import sys

DELAY = 5
ISLOOP = True

def sendPutRequests(url, headers, data):
    while ISLOOP:
        response = requests.put(url=url, headers=headers, data=data)
        time.sleep(DELAY)
        break

def sendGetRequest(url):
    response = requests.get(url=url)
    return response

# transform the original id which is unique inside its stream to a unique id among all streams
def transformId(originalId, streamId):
    return '{}-{}'.format(streamId, originalId)

def restock(streamId, itemId, quantity):
    transformedItemId = transformId(itemId, streamId)
    headers = {
        'Content-Type': 'application/vnd.com.example/RestockItem',
    }
    data = '{"itemId": {}, "quantity": {}}'.format(transformedItemId, quantity)
    url = 'http://localhost:8090/com.example/stock/{}'.format(transformedItemId)

    print(f"[Stream {streamId}] sending restock event to {url}")
    sendPutRequests(url, headers, data)

def addToCart(streamId, userId, quantity, itemId):
    transformedItemId = transformId(itemId, streamId)
    transformedUserId = transformId(userId, streamId)
    headers = {
        'Content-Type': 'application/vnd.com.example/AddToCart',
    }
    data = '{"userId": {}, "quantity": {}, "itemId": {}}'.format(transformedUserId, quantity, transformedItemId)
    url = 'http://localhost:8090/com.example/user-shopping-cart/{}'.format(transformedUserId)
    
    print(f"[Stream {streamId}] sending addToCart event to {url}")
    sendPutRequests(url, headers, data)

def checkout(streamId, userId):
    transformedUserId = transformId(userId, streamId)
    headers = {
        'Content-Type': 'application/vnd.com.example/Checkout',
    }
    data = '{"userId": {}}'.format(transformedUserId)
    url = 'http://localhost:8090/com.example/user-shopping-cart/{}'.format(transformedUserId)

    print(f"[Stream {streamId}] sending checkout events to {url}")
    sendPutRequests(url, headers, data)

    return

# TODO: how to separate the receipt of different streams?
def receipt(streamId):
    url = 'http://localhost:8090/com.example/reciept'
    response = sendGetRequest(url)
    print(f"recipets:\n {response.text}")

    return

def main(args):
    print(args)
    ISLOOP = args.loop

    if args.restock:
        restock(args.streamId, args.itemId, args.quantity)
    elif args.addToCart:
        addToCart(args.streamId, args.userId, args.quantity, args.itemId)
    elif args.checkout:
        checkout(args.streamId, args.userId)
    elif args.receipt:
        receipt(args.streamId)

    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # event type config
    parser.add_argument('--restock', default=False, type=bool, help='start the restock producer')
    parser.add_argument('--add_to_cart', default=False, type=bool, help='start the addToCart producer')
    parser.add_argument('--checkout', default=False, type=bool, help='start the checkout producer')
    parser.add_argument('--receipt', default=False, type=bool, help='start the receipt producer')
    # common event config
    parser.add_argument('--loop', default=False, type=bool, help='true if the producer needs to run forever')
    # event details
    parser.add_argument('--item_id', default='socks', type=str, help='the item id')
    parser.add_argument('--quantity', default=1, type=int, help='the quantity of the item')
    parser.add_argument('--user_id', default='u1', type=str, help='the user id')
    parser.add_argument('--stream_id', default='s1', type=str, help='the stream id')
    args = parser.parse_args()
    
    # run the main function
    main(args)