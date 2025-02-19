import configparser
import csv
import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read configuration
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_PATH"))


EMAIL_RECIPIENTS = [email.strip()
                    for email in config['EMAIL']['recipients'].split(',')]

inventory_csv_path = os.getenv('INVENTORY_CSV_PATH')
processed_path = os.getenv('PROCESSED_PATH')
missing_barcodes_path = os.getenv('MISSING_BARCODE_FILES_PATH')

# Shopify API credentials
API_ACCESS_TOKEN = os.getenv('API_ACCESS_TOKEN')
SHOP_NAME = os.getenv('SHOP_NAME')
GRAPHQL_ENDPOINT = f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-07/graphql.json'
LOCATION_ID = 'gid://shopify/Location/80616161595'
LOCKFILE = '/tmp/inventory_update.lock'

# Headers for the Shopify API request
HEADERS = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': API_ACCESS_TOKEN
}

# GraphQL query to get inventory item ID by UPC
QUERY = '''
query ($upc: String!) {
  productVariants(first: 10, query: $upc) {
    edges {
      node {
        inventoryItem {
          id
        }
      }
    }
  }
}
'''

# GraphQL mutation to set inventory on-hand quantities
MUTATION = '''
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


def create_lock_file():
    if os.path.exists(LOCKFILE):
        print("Another instance is running. Exiting...")
        sys.exit()
    else:
        with open(LOCKFILE, 'w') as lock_file:
            lock_file.write(str(os.getpid()))


def remove_lock_file():
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)


def get_inventory_item_id(upc):
    response = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={
                             'query': QUERY, 'variables': {'upc': upc}})
    if response.status_code != 200:
        print(
            f"Error fetching inventory item ID for UPC {upc}: HTTP {response.status_code}")
        return None

    data = response.json()
    edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
    if edges:
        return edges[0].get('node', {}).get('inventoryItem', {}).get('id')
    else:
        print(f"No inventory item found for UPC: {upc}")
        return None
    
    
def get_inventory_item_ids(upc):
    response = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={
        'query': QUERY, 'variables': {'upc': upc}})
    if response.status_code != 200:
        print(f"Error fetching inventory item IDs for UPC {upc}: HTTP {response.status_code}")
        return []

    data = response.json()
    edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
    return [edge['node']['inventoryItem']['id'] for edge in edges]



def update_inventory(upc, quantity):
    inventory_item_ids = get_inventory_item_ids(upc)
    if not inventory_item_ids:
        print(f"No inventory item IDs found for UPC {upc}. Skipping update.")
        missing_barcodes.append(upc)
        return

    for inventory_item_id in inventory_item_ids:
        query = {
            "input": {
                "reason": "correction",
                "referenceDocumentUri": f"logistics://update/{upc}",
                "setQuantities": [
                    {
                        "inventoryItemId": inventory_item_id,
                        "locationId": LOCATION_ID,
                        "quantity": quantity
                    }
                ]
            }
        }

        response = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={
            'query': MUTATION, 'variables': query})
        if response.status_code != 200:
            print(f"Error updating inventory for UPC {upc}: HTTP {response.status_code}")
            continue

        data = response.json()
        inventory_set = data.get('data', {}).get('inventorySetOnHandQuantities', {})
        user_errors = inventory_set.get('userErrors', [])
        if user_errors:
            for error in user_errors:
                print(f"Error updating inventory for UPC {upc}: {error['message']}")
        else:
            # Reintroduce adjustment group logging
            adjustment_group = inventory_set.get('inventoryAdjustmentGroup', {})
            if adjustment_group:
                print(f"Updated inventory for UPC {upc}:")
                print(f"  Reason: {adjustment_group.get('reason')}")
                print(f"  Reference Document URI: {adjustment_group.get('referenceDocumentUri')}")
                for change in adjustment_group.get('changes', []):
                    print(f"  Change: {change.get('name')}, Delta: {change.get('delta')}")
            else:
                print(f"No adjustment group found in the response for UPC {upc}.")



def save_missing_barcodes():
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d-%H-%M-%S')
    os.makedirs(missing_barcodes_path, exist_ok=True)
    file_path = os.path.join(missing_barcodes_path,
                             f'missing-barcodes-{timestamp}.csv')

    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        for upc in missing_barcodes:
            writer.writerow([upc])

    print(f"Missing barcodes saved to {file_path}")
    return file_path


def move_and_rename_csv():
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d-%H-%M-%S')
    os.makedirs(processed_path, exist_ok=True)
    new_file_name = f'ciwdsvcp-{timestamp}.csv'
    new_file_path = os.path.join(processed_path, new_file_name)
    os.rename(inventory_csv_path, new_file_path)

    print(f"CSV file moved and renamed to {new_file_path}")
    return new_file_path


def send_email(subject, body, to_emails):
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_password = os.getenv("SMTP_SENDER_PASSWROD")

    # Set up the MIME
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(to_emails)
    message["Subject"] = subject
    message.attach(MIMEText(body, 'plain'))

    # Connect and send the email
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), 587)
        server.starttls()  # Encrypts the connection
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_emails, message.as_string())
        server.close()
    except Exception as e:
        print(f"Error sending email: {e}")


def main():
    create_lock_file()
    try:
        global missing_barcodes
        missing_barcodes = []

        with open(inventory_csv_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) >= 2:  # Ensure there are at least 2 columns
                    upc = row[0].strip()
                    try:
                        quantity = int(row[1].strip())
                        update_inventory(upc, quantity)
                    except ValueError:
                        print(
                            f"Invalid quantity '{row[1]}' for UPC {upc}. Skipping.")
                else:
                    print("Invalid row in CSV, skipping.")

        missing_barcodes_filename = None
        if missing_barcodes:
            missing_barcodes_filename = save_missing_barcodes()

        final_processed_path = move_and_rename_csv()

        if missing_barcodes_filename:
            missing_barcodes_count = len(missing_barcodes)
            subject = "Shopify Inventory File Processed"
            body = (f"Filename: {os.path.basename(final_processed_path)} has been processed.\n\n"
                    f"Processed file: {os.path.basename(final_processed_path)}\n"
                    f"Missing barcode file: {os.path.basename(missing_barcodes_filename)}\n"
                    f"Total missing barcodes: {missing_barcodes_count}\n")
            if missing_barcodes_count > 0:
                body += "Missing barcodes:\n" + "\n".join(missing_barcodes)
            send_email(subject, body, EMAIL_RECIPIENTS)
    finally:
        remove_lock_file()


if __name__ == "__main__":
    main()
