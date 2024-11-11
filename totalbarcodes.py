import csv
import json
import os
from time import sleep

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('VERSION')

# Shopify GraphQL API URL
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}


def get_all_barcodes():
    barcodes = set()
    has_next_page = True
    cursor = None

    while has_next_page:
        query = """
        {
            products(first: 250""" + (f', after: "{cursor}"' if cursor else '') + """) {
                edges {
                    node {
                        variants(first: 250) {
                            edges {
                                node {
                                    barcode
                                }
                            }
                        }
                    }
                    cursor
                }
                pageInfo {
                    hasNextPage
                }
            }
        }
        """
        try:
            response = requests.post(
                shop_url, headers=headers, json={'query': query})

            if response.status_code == 200:
                data = response.json()
                products = data['data']['products']['edges']
                for product in products:
                    for variant in product['node']['variants']['edges']:
                        barcode = variant['node']['barcode']
                        if barcode and barcode.strip():
                            barcodes.add(barcode)
                has_next_page = data['data']['products']['pageInfo']['hasNextPage']
                if has_next_page:
                    cursor = products[-1]['cursor']
            else:
                print(
                    f"Failed to fetch data: {response.status_code} - {response.text}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            sleep(2)  # Pause and retry
            continue

    return barcodes


def main():
    barcodes = get_all_barcodes()
    print(f"Total unique barcodes in Shopify store: {len(barcodes)}")

    # Write barcodes to a CSV file
    with open('all_barcodes.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Barcode'])
        for barcode in barcodes:
            writer.writerow([barcode])


if __name__ == "__main__":
    main()
