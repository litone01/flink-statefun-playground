python3 producer.py --restock --item_id socks --quantity 50 --stream_id s2
sleep 1

python3 producer.py --add_to_cart --user_id u1 --item_id socks --quantity 50 --stream_id s2
sleep 1

python3 producer.py --checkout --user_id u1 --stream_id s2
sleep 1

python3 producer.py --receipt --stream_id s2
echo "done"