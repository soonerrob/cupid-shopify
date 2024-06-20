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

# Cache file paths
inventory_cache_file = 'inventory_cache.json'
location_cache_file = 'location_cache.json'


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

        # print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")


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
            print(
                f"Failed to fetch data: {response.status_code} - {response.text}")
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
        response = requests.post(
            graphql_url, headers=headers, json={'query': mutation})
        if response.status_code == 200:
            print(
                f"Successfully updated inventory for item {inventory_item_id} at location {location_id} to {quantity}")
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


def update_inventory_from_csv():
    if not os.path.exists(inventory_csv_path):
        print(f"No inventory file found at {inventory_csv_path}")
        # send_email("Shopify Inventory Upload Script Error",
        #            "No inventory file found at the specified path.", EMAIL_RECIPIENTS)
        return

    location_id = get_primary_location_id()
    if not location_id:
        print("No valid location ID available. Exiting.")
        # send_email("Shopify Inventory Upload Script Error",
        #            "No valid location ID available.", EMAIL_RECIPIENTS)
        return

    inventory_data = pd.read_csv(
        inventory_csv_path, header=None, dtype={0: str, 1: int})
    missing_barcodes = []

    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    final_processed_path = os.path.join(
        processed_path, f"processed-{os.path.basename(inventory_csv_path)}-{timestamp}.csv")
    try:
        for index, row in inventory_data.iterrows():
            barcode, quantity = row[0], row[1]
            inventory_item_id = find_inventory_item_id(barcode)
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

        if os.path.exists(inventory_csv_path):
            shutil.move(inventory_csv_path, final_processed_path)
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


# First, update the cache with all product variants
update_inventory_cache()

# Then run the inventory update
update_inventory_from_csv()
