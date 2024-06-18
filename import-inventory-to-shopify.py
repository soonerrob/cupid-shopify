import json
import os
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('VERSION')
inventory_csv_path = os.getenv('INVENTORY_CSV_PATH')

# Shopify API URL and Headers
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}

# Initialize single file for missing barcodes
missing_barcodes_filename = f"missing-barcodes-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
# Create the file and open it once to ensure it exists
with open(missing_barcodes_filename, 'w') as file:
    file.write("Barcode\n")

# Cache file paths
inventory_cache_file = 'inventory_cache.json'
location_cache_file = 'location_cache.json'


def load_cache(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_cache(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


def get_primary_location_id():
    locations_cache = load_cache(location_cache_file)
    if locations_cache:
        return locations_cache.get('primary_location')
    response = requests.get(f"{shop_url}/locations.json", headers=headers)
    if response.status_code == 200:
        locations = response.json().get('locations', [])
        if locations:
            primary_location_id = locations[0]['id']
            save_cache(location_cache_file, {
                       'primary_location': primary_location_id})
            return primary_location_id
    print("Failed to fetch locations:", response.text)
    return None


def update_inventory_cache():
    inventory_cache = load_cache(inventory_cache_file)
    page_info = ''
    has_changes = False

    while True:
        response = requests.get(
            f"{shop_url}/products.json?limit=250&fields=id,variants&page_info={page_info}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            products = data.get('products', [])
            for product in products:
                for variant in product.get('variants', []):
                    barcode = variant['barcode']
                    if barcode and barcode.strip():
                        if barcode not in inventory_cache or inventory_cache[barcode] != variant['inventory_item_id']:
                            inventory_cache[barcode] = variant['inventory_item_id']
                            has_changes = True

            page_info = data.get('next', {}).get('page_info', None)
            if not page_info:
                break
        else:
            print(
                f"Failed to fetch data: {response.status_code} - {response.text}")
            break

    if has_changes:
        save_cache(inventory_cache_file, inventory_cache)


def find_inventory_item_id(barcode):
    inventory_cache = load_cache(inventory_cache_file)
    if barcode in inventory_cache:
        return inventory_cache[barcode]
    print(f"Barcode {barcode} not found in cache.")
    return None


def update_inventory_level(inventory_item_id, location_id, quantity):
    url = f"{shop_url}/inventory_levels/set.json"
    payload = {
        "location_id": location_id,
        "inventory_item_id": inventory_item_id,
        "available": quantity
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(
            f"Successfully updated inventory for item {inventory_item_id} at location {location_id} to {quantity}")
    else:
        print(
            f"Failed to update inventory for item {inventory_item_id}: {response.text}")


def log_missing_barcodes(barcode):
    try:
        with open(missing_barcodes_filename, 'a') as file:
            file.write(f"{barcode}\n")
    except Exception as e:
        print(f"Failed to log missing barcode {barcode}: {e}")


def update_inventory_from_csv():
    # Check if the CSV file exists before proceeding
    if not os.path.exists(inventory_csv_path):
        return  # Exit the function silently if there is no file to process
    
    location_id = get_primary_location_id()
    if not location_id:
        print("No valid location ID available. Exiting.")
        return

    inventory_data = pd.read_csv(
        inventory_csv_path, header=None, dtype={0: str})
    for index, row in inventory_data.iterrows():
        barcode, quantity = row[0], int(row[1])
        inventory_item_id = find_inventory_item_id(barcode)
        if inventory_item_id:
            update_inventory_level(inventory_item_id, location_id, quantity)
        else:
            print(f"No inventory item found for barcode {barcode}")
            log_missing_barcodes(barcode)


# First, update the cache with all product variants
update_inventory_cache()

# Then run the inventory update
update_inventory_from_csv()
