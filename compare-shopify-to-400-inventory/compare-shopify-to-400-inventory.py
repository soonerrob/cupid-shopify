import configparser
import csv
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from glob import glob

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load configuration from config.ini
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_PATH"))

# Retrieve essential information
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('VERSION')
processed_path = os.getenv('PROCESSED_PATH')
EMAIL_RECIPIENTS = [email.strip()
                    for email in config['EMAIL']['recipients'].split(',')]

# Ensure GRAPHQL_ENDPOINT and LOCATION_ID are set correctly
GRAPHQL_ENDPOINT = f'https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json'

# Ensure this is in the correct format, like "gid://shopify/Location/80616161595"
LOCATION_ID = os.getenv('LOCATION_ID')
location_id = f"gid://shopify/Location/{LOCATION_ID}"

# Paths and Shopify API settings
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}


# Load the whitelist of UPCs


def load_whitelist():
    whitelist_path = os.path.join(os.path.dirname(
        __file__), 'compare-shopify-to-400-inventory-whitelist.txt')

    if not os.path.exists(whitelist_path):
        print("Whitelist file not found. Creating an empty whitelist file.")
        with open(whitelist_path, 'w') as file:
            pass  # Create an empty file

    # Load UPCs from the file, which will be empty if just created
    with open(whitelist_path, 'r') as file:
        return set(line.strip() for line in file if line.strip())


# Load the whitelist into a set for quick lookups
whitelisted_upcs = load_whitelist()

# Query and mutation for inventory management
inventory_item_id_query = """
query getInventoryItemId($barcode: String!) {
  productVariants(first: 1, query: $barcode) {
    edges {
      node {
        inventoryItem {
          id
        }
      }
    }
  }
}
"""

set_inventory_quantity_mutation = '''
mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
  inventorySetOnHandQuantities(input: $input) {
    userErrors {
      field
      message
    }
    inventoryAdjustmentGroup {
      createdAt
      reason
      referenceDocumentUri
      changes {
        name
        delta
      }
    }
  }
}
'''


# Function to get the latest file with the 'ciwdsvcp' prefix


def get_latest_inventory_file():
    files = glob(os.path.join(processed_path, 'ciwdsvcp-*.csv'))
    latest_file = max(files, key=os.path.getctime) if files else None
    return latest_file


# Use the latest file in the main script
inventory_file_path = get_latest_inventory_file()

if inventory_file_path:
    print(f"Processing latest inventory file: {inventory_file_path}")
else:
    print("No inventory files found to process.")
    exit(1)


def get_all_shopify_barcodes():
    shopify_inventory = []
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
                                    sku
                                    barcode
                                    inventoryQuantity
                                    availableForSale
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
        response = requests.post(
            shop_url, headers=headers, json={'query': query})

        if response.status_code == 200:
            data = response.json()
            products = data['data']['products']['edges']
            for product in products:
                for variant in product['node']['variants']['edges']:
                    sku = variant['node']['sku']
                    barcode = variant['node']['barcode']
                    inventory_quantity = variant['node']['inventoryQuantity']
                    available_for_sale = variant['node']['availableForSale']
                    if barcode and barcode.strip() and inventory_quantity > 0 and available_for_sale:
                        shopify_inventory.append({
                            "sku": sku,
                            "barcode": barcode,
                            "inventory_quantity": inventory_quantity
                        })
            has_next_page = data['data']['products']['pageInfo']['hasNextPage']
            if has_next_page:
                cursor = products[-1]['cursor']
        else:
            print(
                f"Failed to fetch data: {response.status_code} - {response.text}")
            break

    return shopify_inventory


def get_inventory_barcodes():
    inventory_barcodes = set()
    with open(inventory_file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            barcode = row[0].strip()
            inventory_barcodes.add(barcode)
    return inventory_barcodes


def set_inventory_to_zero(barcode, original_quantity):
    # print(f"\nAttempting to set inventory to zero for barcode: {barcode}")
    # print(f"Original Quantity: {original_quantity}")

    # # Print and format the LOCATION_ID correctly
    # location_id = f"gid://shopify/Location/{LOCATION_ID}"
    # print(f"Formatted LOCATION_ID: {location_id}")

    # Retrieve the inventory item ID for the barcode
    response = requests.post(
        GRAPHQL_ENDPOINT,
        headers=headers,
        json={
            'query': inventory_item_id_query,
            'variables': {'barcode': barcode}
        }
    )

    if response.status_code != 200:
        print(
            f"Error fetching inventory item ID for barcode {barcode}: {response.text}")
        return

    data = response.json()
    edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
    if not edges:
        print(f"No inventory item found for barcode: {barcode}")
        return

    inventory_item_id = edges[0]['node']['inventoryItem']['id']
    print(f"Inventory Item ID for barcode {barcode}: {inventory_item_id}")

    # Structure the mutation request to set inventory to zero
    query = {
        "input": {
            "reason": "correction",
            "referenceDocumentUri": f"logistics://update/{barcode}",
            "setQuantities": [
                {
                    "inventoryItemId": inventory_item_id,
                    "locationId": location_id,  # Use the formatted location ID here
                    "quantity": 0
                }
            ]
        }
    }

    response = requests.post(
        GRAPHQL_ENDPOINT,
        headers=headers,
        json={
            'query': set_inventory_quantity_mutation,
            'variables': query
        }
    )

    # Log the entire response from the mutation call
    # print(f"Response from inventory update mutation: {response.status_code}")
    # print(f"Response text: {response.text}")

    if response.status_code != 200:
        print(
            f"Error setting inventory to zero for barcode {barcode}: {response.text}")
        return

    data = response.json()
    inventory_set = data.get('data', {}).get(
        'inventorySetOnHandQuantities', {})
    user_errors = inventory_set.get('userErrors', [])

    # Check and log any errors in the mutation
    if user_errors:
        print(f"User errors in inventory adjustment for barcode {barcode}:")
        for error in user_errors:
            print(f"Field: {error['field']} - Message: {error['message']}")
    else:
        print(f"Successfully set inventory to zero for barcode {barcode}")


def send_email(subject, body, to_emails, high_priority=False):
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_password = os.getenv("SMTP_SENDER_PASSWROD")

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(to_emails)
    message["Subject"] = subject

    # Set high importance if specified
    if high_priority:
        message["X-Priority"] = "1"  # High priority
        message["X-MSMail-Priority"] = "High"
        message["Importance"] = "High"

    message.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_emails, message.as_string())
        server.close()
    except Exception as e:
        print(f"Error sending email: {e}")


def main():
    # Fetch barcodes from Shopify
    shopify_inventory = get_all_shopify_barcodes()

    # Fetch barcodes from the inventory CSV file
    inventory_barcodes = get_inventory_barcodes()

    # Filter missing inventory items by excluding those on the whitelist
    missing_inventory = []
    ignored_whitelist = []
    for item in shopify_inventory:
        if item["barcode"] not in inventory_barcodes:
            if item["barcode"] in whitelisted_upcs:
                ignored_whitelist.append(item)
            else:
                missing_inventory.append(item)

    print(
        f"Total items missing in inventory (excluding whitelist): {len(missing_inventory)}")

    # Write missing inventory details to a CSV file
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d-%H-%M-%S')
    missing_inventory_filename = f'missing_inventory-{timestamp}.csv'
    with open(missing_inventory_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['SKU', 'Barcode', 'Original Inventory Quantity'])
        for item in missing_inventory:
            writer.writerow([item['sku'], item['barcode'],
                            item['inventory_quantity']])

    # Set inventory for non-whitelisted missing barcodes to zero
    for item in missing_inventory:
        set_inventory_to_zero(item['barcode'], item['inventory_quantity'])

    # Send an email report with high importance if any items were set to zero
    high_priority = len(missing_inventory) > 0
    subject = "Inventory Comparison Processed"
    body = (
        f"Processed inventory file: {os.path.basename(inventory_file_path)}\n"
        f"Missing inventory file: {missing_inventory_filename}\n"
        f"Total items missing and set to zero (excluding whitelist): {len(missing_inventory)}\n\n"
        "Items set to zero with original quantities:\n"
    )
    if missing_inventory:
        body += "\n".join(
            f"SKU: {item['sku']}, Barcode: {item['barcode']}, Original Quantity: {item['inventory_quantity']}"
            for item in missing_inventory
        )

    if ignored_whitelist:
        body += "\n\nItems ignored due to whitelist:\n" + "\n".join(
            f"SKU: {item['sku']}, Barcode: {item['barcode']}, Inventory Quantity: {item['inventory_quantity']}"
            for item in ignored_whitelist
        )

    send_email(subject, body, EMAIL_RECIPIENTS, high_priority=high_priority)


if __name__ == "__main__":
    main()
