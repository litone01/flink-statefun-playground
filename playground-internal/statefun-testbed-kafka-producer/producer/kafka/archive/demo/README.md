# Source
From https://github.com/tzulitai/statefun-ffsf-demo

## Running

- Start the application:
```
statefun-ffsf-demo $ docker-compose up
```

- Start consuming checkout receipts:
```
statefun-ffsf-demo $ python demo/consume_receipts.py
```

- Restock some inventory:
```
statefun-ffsf-demo $ python demo/restock_inventory.py <inventory_id> <restock_quantity>
# e.g.
statefun-ffsf-demo $ python demo/restock_inventory.py dope_pants 10
```

- Add some items to user cart:
```
statefun-ffsf-demo $ python demo/user_cart_events.py <user_id> add <inventory_id> <add_quantity>
# e.g.
statefun-ffsf-demo $ python demo/user_cart_events.py gordon add dope_pants 2
```

- Checkout user cart:
```
statefun-ffsf-demo $ python demo/user_cart_events.py <user_id> checkout
# e.g.
statefun-ffsf-demo $ python demo/user_cart_events.py gordon checkout
```

- On checkout, you'll see the corresponding receipt event:
```
user_id: "gordon"
user_cart {
  items {
    key: "dope_pants"
    value: 2
  }
}
```
