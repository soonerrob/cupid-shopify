import os

import pandas as pd
import shopify
from dotenv import load_dotenv

load_dotenv()

# Environment variables
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
api_version = os.getenv('EXPORT_SPECIFIC_ORDER_API_VERSION')
EXPORT_PATH = os.getenv('EXPORT_SPECIFIC_ORDER_PATH', './specific-order.csv')

# Configure the Shopify API session
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/"
shopify.ShopifyResource.set_site(shop_url)
session = shopify.Session(
    f"{shop_name}.myshopify.com",
    api_version,
    admin_api_token
)
shopify.ShopifyResource.activate_session(session)

# Variable for the order number
order_id = '67136'  # Replace this with the specific order ID you want to pull

# Function to retrieve a specific order


def retrieve_specific_order(order_id):
    order = shopify.Order.find(order_id)
    return order.attributes


# Retrieve the order
order_data = retrieve_specific_order(order_id)

# Save the order data to a CSV file


def save_order_to_csv(order_data, file_path):
    df = pd.json_normalize(order_data)
    df.to_csv(file_path, index=False)


save_order_to_csv(order_data, EXPORT_PATH)
print(f"Order {order_id} data saved to {EXPORT_PATH}")
