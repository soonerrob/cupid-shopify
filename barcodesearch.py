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

# Set the barcode to search for
barcode_to_search = '080225086577'

# Shopify API URL and Headers
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}

def find_product_by_barcode(barcode):
    products = []
    page_info = None
    product_found = False

    while True:
        params = {
            "limit": 250,
        }
        if page_info:
            params['page_info'] = page_info

        response = requests.get(
            f"{shop_url}/products.json",
            headers=headers,
            params=params
        )

        if response.status_code == 200:
            fetched_products = response.json().get('products', [])
            products.extend(fetched_products)
            print(f"Fetched {len(fetched_products)} products, Total fetched: {len(products)}")

            # Check for pagination link header
            link_header = response.headers.get('Link')
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
            break

    for product in products:
        for variant in product['variants']:
            if variant.get('barcode') == barcode:
                print(f"Found product: {product['id']} with variant ID: {variant['id']} and barcode: {barcode}")
                product_found = True
                return

    if not product_found:
        print(f"No product found with barcode {barcode}")

# Search for the product by barcode
find_product_by_barcode(barcode_to_search)
