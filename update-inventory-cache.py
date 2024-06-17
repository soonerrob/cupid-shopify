import json
import os

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('VERSION')

# Shopify API URL and Headers
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}

# Cache file path
inventory_cache_file = 'inventory_cache.json'


def load_cache(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}  # Return an empty dictionary if the file does not exist


def save_cache(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


def update_inventory_cache():
    inventory_cache = load_cache(inventory_cache_file)
    page_info = None
    has_changes = False

    while True:
        url = f"{shop_url}/products.json?limit=250&fields=id,variants"
        if page_info:
            url += f"&page_info={page_info}"

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            products = data.get('products', [])
            for product in products:
                for variant in product.get('variants', []):
                    barcode = variant.get('barcode')
                    if barcode and barcode.strip():  # Ensure barcode is not null or empty
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

    # Save the updated cache if there were any changes
    if has_changes:
        save_cache(inventory_cache_file, inventory_cache)
        print("Inventory cache updated successfully.")


if __name__ == "__main__":
    update_inventory_cache()
