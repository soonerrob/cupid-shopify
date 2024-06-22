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

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('VERSION')
inventory_csv_path = os.getenv('INVENTORY_CSV_PATH')
processed_path = os.getenv('PROCESSED_PATH')
missing_barcodes_path = os.getenv('MISSING_BARCODE_FILES_PATH')
location_id = os.getenv('LOCATION_ID')

# Load the configuration file
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_PATH"))

EMAIL_RECIPIENTS = [email.strip()
                    for email in config['EMAIL']['recipients'].split(',')]

# Ensure the processed and missing barcode directories exist
if not os.path.exists(processed_path):
    os.makedirs(processed_path)

if not os.path.exists(missing_barcodes_path):
    os.makedirs(missing_barcodes_path)

# Shopify GraphQL API URL and Headers
graphql_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}


def send_email(subject, body, to_emails):
    # Email setup
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


def get_inventory_item_id_from_barcode(barcode):
    query = """
    query ($barcode: String!) {
      productVariants(first: 1, query: $barcode) {
        edges {
          node {
            id
            inventoryItem {
              id
            }
          }
        }
      }
    }
    """
    variables = {
        "barcode": f"barcode:{barcode}"
    }
    response = requests.post(
        graphql_url, headers=headers, json={
            'query': query, 'variables': variables}
    )
    if response.status_code == 200:
        response_data = response.json()
        edges = response_data['data']['productVariants']['edges']
        if edges:
            return edges[0]['node']['inventoryItem']['id']
    print(f"Failed to find product with barcode: {barcode}")
    return None


def update_inventory_level(inventory_item_id, location_id, new_quantity):
    mutation = """
    mutation ($inventoryItemId: ID!, $locationId: ID!, $availableQuantity: Int!) {
      inventoryAdjustQuantity(input: {inventoryItemId: $inventoryItemId, availableQuantity: $availableQuantity, locationId: $locationId}) {
        inventoryLevel {
          id
          available
        }
      }
    }
    """
    variables = {
        "inventoryItemId": inventory_item_id,
        "locationId": location_id,
        "availableQuantity": new_quantity
    }
    while True:
        response = requests.post(
            graphql_url, headers=headers, json={
                'query': mutation, 'variables': variables}
        )
        if response.status_code == 200:
            print(
                f"Successfully updated inventory for item {inventory_item_id} to {new_quantity}")
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
            print(
                f"Failed to update inventory for item {inventory_item_id}: {response.text}")
            break


def update_inventory_from_csv(csv_path):
    if not os.path.exists(csv_path):
        print(f"No inventory file found at {csv_path}")
        send_email("Shopify Inventory Upload Script Error",
                   "No inventory file found at the specified path.", EMAIL_RECIPIENTS)
        return

    inventory_data = pd.read_csv(csv_path, header=None, dtype={0: str, 1: int})
    missing_barcodes = []

    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    final_processed_path = os.path.join(
        processed_path, f"processed-{os.path.basename(csv_path)}-{timestamp}.csv")
    try:
        for index, row in inventory_data.iterrows():
            barcode, quantity = row[0], row[1]
            inventory_item_id = get_inventory_item_id_from_barcode(barcode)
            if inventory_item_id:
                print(f"Updating item {barcode} with quantity {quantity}")
                update_inventory_level(
                    inventory_item_id, location_id, quantity)
            else:
                missing_barcodes.append(barcode)

        if missing_barcodes:
            missing_barcodes_filename = os.path.join(
                missing_barcodes_path, f"missing-barcodes-{timestamp}.csv")
            with open(missing_barcodes_filename, 'w') as file:
                file.write("\n".join(missing_barcodes) + "\n")
            missing_barcodes_count = len(missing_barcodes)
        else:
            missing_barcodes_filename = "No missing barcodes were found."
            missing_barcodes_count = 0

        if os.path.exists(csv_path):
            shutil.move(csv_path, final_processed_path)
            print(f"Moved processed file to {final_processed_path}")

            subject = "Shopify Inventory File Processed"
            body = (f"Filename: {os.path.basename(final_processed_path)} has been processed.\n\n"
                    f"Processed file: {os.path.basename(final_processed_path)}\n"
                    f"Missing barcode file: {os.path.basename(missing_barcodes_filename)}\n"
                    f"Total missing barcodes: {missing_barcodes_count}\n")
            if missing_barcodes_count > 0:
                body += "Missing barcodes:\n" + "\n".join(missing_barcodes)
            send_email(subject, body, EMAIL_RECIPIENTS)
    except Exception as e:
        print(f"An error occurred: {e}")
        send_email("Shopify Inventory Upload Script Error",
                   "An error occurred during the inventory update process.", EMAIL_RECIPIENTS)


# Run the inventory update
update_inventory_from_csv(inventory_csv_path)
