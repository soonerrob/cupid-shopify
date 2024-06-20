import os
import requests
import csv
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

# Product ID for which to list variant IDs and barcodes
product_id = '9134229586235'  # Replace with your actual product ID

def get_product_variants(product_id):
    endpoint = f"{shop_url}/products/{product_id}.json"
    response = requests.get(endpoint, headers=headers)
    
    if response.status_code == 200:
        product = response.json().get('product', {})
        variants = product.get('variants', [])
        variant_details = [(variant['id'], variant.get('barcode', '')) for variant in variants]
        return variant_details
    else:
        print(f"Failed to fetch product: {response.status_code} - {response.text}")
        return []

def save_variants_to_csv(variant_details, filename='product_variants.csv'):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Variant ID', 'Barcode'])
        for variant_id, barcode in variant_details:
            writer.writerow([variant_id, barcode])
    print(f"Variant details saved to {filename}")

def main():
    variants = get_product_variants(product_id)
    if variants:
        print(f"Total variants found: {len(variants)}")
        save_variants_to_csv(variants)
    else:
        print("No variants found.")

if __name__ == "__main__":
    main()
