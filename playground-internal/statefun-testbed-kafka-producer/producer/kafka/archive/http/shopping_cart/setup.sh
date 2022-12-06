echo "starting producer for restock"
python3 producer.py --path restock.json --topic inventory-restock-events --json_path itemId