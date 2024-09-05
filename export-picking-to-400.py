import os
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pandas as pd
import shopify
from dateutil.parser import parse
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

# Load environment variables
load_dotenv()

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')  # The unique name of your Shopify store
# Your Shopify Admin API access token
admin_api_token = os.getenv('API_ACCESS_TOKEN')

# Directory where the CSV file will be saved
EXPORT_DIR = "export-picking-to-400-archive"

# Ensure the export directory exists
if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)

# Configure the Shopify API session
api_version = os.getenv('EXPORT_ORDER_API_VERSION')
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/"
shopify.ShopifyResource.set_site(shop_url)
shopify.ShopifyResource.activate_session(shopify.Session(
    f"{shop_name}.myshopify.com", api_version, admin_api_token))

# SMB connection configuration
SERVER_NAME = os.getenv("IBM_SERVER_NAME")
SHARE_NAME = os.getenv("IBM_SHARE_NAME")
USERNAME = os.getenv("IBM_USERNAME")
PASSWORD = os.getenv("IBM_PASSWORD")
DOMAIN = ''

# Global variables
ENABLE_TAGGING = False 

def add_tag_to_order(order, tag):
    """
    Adds a tag to an order if it's not already present and saves the order.

    Parameters:
    - order: The Shopify order object to be tagged.
    - tag: The tag string to be added to the order.
    """
    # Check if the tag is not already in the order's tags
    if tag not in order.tags:
        order.tags += f", {tag}"  # Append the tag to the existing tags
        order.save()  # Save the order with the new tag

def save_to_smb(file_content, server_name, share_name, smb_path, filename, username, password, domain=''):
    conn = SMBConnection(username, password, "client_machine", server_name, domain=domain, use_ntlm_v2=True, is_direct_tcp=True)
    
    if not conn.connect(server_name, 445):
        raise ConnectionError(f"Unable to connect to the server: {server_name}")

    # Using BytesIO to create a file-like object in memory and then upload it to the SMB share
    with BytesIO(file_content.encode('utf-8')) as file:
        conn.storeFile(share_name, os.path.join(smb_path, filename), file)
    
    print(f"Report saved as: {filename}")

def fetch_and_export_orders():
    sixty_days_ago = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    orders = shopify.Order.find(created_at_min=sixty_days_ago, status="any")

    if not orders:
        print("No orders found within the specified time range.")
        return

    orders_data = []
    for order in orders:
        print(f"Checking order: {order.id}")
        print(f"Financial Status: {order.financial_status}, Fulfillment Status: {order.fulfillment_status}, Cancelled At: {order.cancelled_at}")

        if order.cancelled_at is not None or (order.financial_status and order.financial_status.lower() != "paid") or (order.fulfillment_status and order.fulfillment_status.lower() != "unfulfilled"):
            print(f"Skipping order: {order.id} due to status")
            continue
        if "Picking" in order.tags:
            print(f"Skipping order: {order.id} due to existing 'Picking' tag")
            continue

        for item in order.line_items:
            order_date = parse(order.created_at).strftime("%Y-%m-%d")
            barcode = "Unavailable"

            try:
                variant = shopify.Variant.find(item.variant_id)
                if variant and hasattr(variant, 'barcode'):
                    barcode = variant.barcode
            except Exception as e:
                print(f"Failed to fetch variant for line item: {e}")

            orders_data.append({
                'Order Number': order.order_number,
                'SKU': barcode,
                'Quantity': item.quantity,
                'Order Date': order_date,
            })

        if ENABLE_TAGGING:
            add_tag_to_order(order, "Picking")

    if orders_data:
        df = pd.DataFrame(orders_data)
        csv_content = df.to_csv(index=False, header=False)
        
        # Generate a timestamped filename for the local path
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        local_filename = f"ciword-{timestamp}.csv"
        local_export_path = os.path.join(EXPORT_DIR, local_filename)
        
        # Save to local path with timestamped filename
        df.to_csv(local_export_path, index=False, header=False)
        print(f"Orders exported successfully to {local_export_path}")

        # Save to SMB with static filename
        smb_filename = "ciword.csv"
        save_to_smb(csv_content, SERVER_NAME, SHARE_NAME, '', smb_filename, USERNAME, PASSWORD, DOMAIN)
    else:
        print("No new orders to export.")

fetch_and_export_orders()
