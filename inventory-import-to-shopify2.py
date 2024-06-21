import configparser
import json
import os
import shutil
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Your Shopify store credentials
API_KEY = 'your_api_key'
API_SECRET = 'your_api_secret'
SHOP_NAME = os.getenv('SHOP_NAME')
ACCESS_TOKEN = 'your_access_token'
LOCATION_ID = 'your_location_id'


# Retrieve essential information from environment variables
SHOP_NAME = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('VERSION')
inventory_csv_path = os.getenv('INVENTORY_CSV_PATH')
processed_path = os.getenv('PROCESSED_PATH')
missing_barcodes_path = os.getenv('MISSING_BARCODE_FILES_PATH')
LOCATION_ID = 'your_location_id'


def get_inventory_item_id_from_barcode(barcode):
    products_response = requests.get(
        f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/products.json?barcode={barcode}',
        headers={'X-Shopify-Access-Token': ACCESS_TOKEN}
    )
    if products_response.status_code == 200:
        products_data = products_response.json()
        for product in products_data['products']:
            for variant in product['variants']:
                if variant['barcode'] == barcode:
                    return variant['inventory_item_id']
    print(f"Failed to find product with barcode: {barcode}")
    return None


def update_inventory_level(inventory_item_id, location_id, new_quantity):
    inventory_levels_response = requests.get(
        f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/inventory_levels.json?inventory_item_ids={inventory_item_id}',
        headers={'X-Shopify-Access-Token': ACCESS_TOKEN}
    )
    if inventory_levels_response.status_code == 200:
        inventory_levels_data = inventory_levels_response.json()
        available_quantity = inventory_levels_data['inventory_levels'][0]['available']

        # Calculate the adjustment needed
        adjustment_quantity = new_quantity - available_quantity

        # Update the inventory level
        update_response = requests.post(
            f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-04/inventory_levels/adjust.json',
            json={
                'inventory_item_id': inventory_item_id,
                'location_id': location_id,
                'available_adjustment': adjustment_quantity
            },
            headers={'X-Shopify-Access-Token': ACCESS_TOKEN}
        )
        if update_response.status_code == 200:
            print(
                f'Successfully updated inventory for item: {inventory_item_id}')
        else:
            print(f'Failed to update inventory for item: {inventory_item_id}')
    else:
        print(f"Failed to get inventory levels for item: {inventory_item_id}")


def update_inventory_from_csv(csv_path):
    with open(csv_path, newline='') as csvfile:
        inventory_reader = csv.reader(csvfile)
        for row in inventory_reader:
            barcode = row[0]
            new_quantity = int(row[1])

            inventory_item_id = get_inventory_item_id_from_barcode(barcode)
            if inventory_item_id:
                update_inventory_level(
                    inventory_item_id, LOCATION_ID, new_quantity)


# Path to your CSV file
csv_path = 'path_to_your_inventory_file.csv'

# Update the inventory from CSV
update_inventory_from_csv(csv_path)
