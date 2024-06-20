import json
import os
import shutil
import time
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
processed_path = os.getenv('PROCESSED_PATH')

# Ensure the processed directory exists
if not os.path.exists(processed_path):
    os.makedirs(processed_path)

# Shopify API URL and Headers
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
graphql_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}

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
    page_info = None
    has_changes = False

    while True:
        endpoint = f"{shop_url}/products.json?limit=250&fields=id,variants"
        if page_info:
            endpoint += f"&page_info={page_info}"

        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            products = data.get('products', [])
            for product in products:
                for variant in product['variants']:
                    barcode = variant.get('barcode')
                    if barcode and barcode.strip() and (barcode not in inventory_cache or inventory_cache[barcode] != variant['id']):
                        inventory_cache[barcode] = variant['id']
                        has_changes = True

            # Pagination handling using link headers
            link_header = response.headers.get('Link', None)
            if link_header:
                links = {rel[6:-1]: url.split('; ')[0][1:-1] for url, rel in
                         (link.split('; ') for link in link_header.split(', '))}
                page_info = links.get('next', None)
                if not page_info:
                    break
            else:
                break
        else:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            if response.status_code != 429:  # Avoid breaking loop on rate limit errors
                break

    if has_changes:
        save_cache(inventory_cache_file, inventory_cache)
    print("Inventory cache updated. Total barcodes cached:", len(inventory_cache))


def find_inventory_item_id(barcode):
    inventory_cache = load_cache(inventory_cache_file)
    if barcode in inventory_cache:
        return inventory_cache[barcode]
    print(f"Barcode {barcode} not found in cache.")
    return None


def update_inventory_level(inventory_item_id, location_id, quantity):
    mutation = """
    mutation {
      inventorySetOnHandQuantities(
        locationId: "%s",
        itemQuantities: {
          id: "%s",
          availableQuantity: %d
        }
      ) {
        inventoryLevel {
          id
        }
      }
    }
    """ % (location_id, inventory_item_id, quantity)

    while True:
        response = requests.post(graphql_url, headers=headers, json={'query': mutation})
        if response.status_code == 200:
            print(f"Successfully updated inventory for item {inventory_item_id} at location {location_id} to {quantity}")
            break
        elif response.status_code == 429:
            retry_after = response.headers.get('Retry-After', '1')
            try:
                wait_time = int(float(retry_after))
            except ValueError:
                wait_time = 1  # Default to 1 second if there's an issue with the header value
            print(f"Rate limit hit, retrying after {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Failed to update inventory for item {inventory_item_id}: {response.text}")
            break


def update_inventory_from_csv():
    if not os.path.exists(inventory_csv_path):
        print(f"No inventory file found at {inventory_csv_path}")
        return

    location_id = get_primary_location_id()
    if not location_id:
        print("No valid location ID available. Exiting.")
        return

    inventory_data = pd.read_csv(inventory_csv_path, header=None, dtype={0: str, 1: int})
    missing_barcodes = []

    for index, row in inventory_data.iterrows():
        barcode, quantity = row[0], row[1]
        inventory_item_id = find_inventory_item_id(barcode)
        if inventory_item_id:
            print(f"Updating item {barcode} with quantity {quantity}")
            print(f"Successfully updated inventory for item {barcode} at location {location_id} to {quantity}")

        else:
            missing_barcodes.append(barcode)

    if missing_barcodes:
        missing_barcodes_filename = f"missing-barcodes-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
        with open(missing_barcodes_filename, 'w') as file:
            file.write("\n".join(missing_barcodes) + "\n")

    if os.path.exists(inventory_csv_path):
        final_processed_path = os.path.join(processed_path, f"processed-{os.path.basename(inventory_csv_path)}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv")
        shutil.move(inventory_csv_path, final_processed_path)
        print(f"Moved processed file to {final_processed_path}")


# First, update the cache with all product variants
update_inventory_cache()

# Then run the inventory update
update_inventory_from_csv()
