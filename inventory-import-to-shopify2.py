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

# Shopify API URL and Headers
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
graphql_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
headers = {
    "X-Shopify-Access-Token": admin_api_token,
    "Content-Type": "application/json"
}


def send_email(subject, body, to_emails):
    # Email setup
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_password = os.getenv("SMTP_SENDER_PASSWORD")

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
    products_response = requests.get(
        f'{shop_url}/products.json?barcode={barcode}',
        headers=headers
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
        f'{shop_url}/inventory_levels.json?inventory_item_ids={inventory_item_id}',
        headers=headers
    )
    if inventory_levels_response.status_code == 200:
        inventory_levels_data = inventory_levels_response.json()
        available_quantity = inventory_levels_data['inventory_levels'][0]['available']

        # Calculate the adjustment needed
        adjustment_quantity = new_quantity - available_quantity

        # Update the inventory level
        update_response = requests.post(
            f'{shop_url}/inventory_levels/adjust.json',
            json={
                'inventory_item_id': inventory_item_id,
                'location_id': location_id,
                'available_adjustment': adjustment_quantity
            },
            headers=headers
        )
        if update_response.status_code == 200:
            print(
                f'Successfully updated inventory for item: {inventory_item_id}')
        else:
            print(f'Failed to update inventory for item: {inventory_item_id}')
    else:
        print(f"Failed to get inventory levels for item: {inventory_item_id}")


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
