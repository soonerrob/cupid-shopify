import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import shopify
from dateutil.parser import parse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')  # The unique name of your Shopify store
# Your Shopify Admin API access token
admin_api_token = os.getenv('API_ACCESS_TOKEN')
# Path where the CSV file will be saved
EXPORT_PATH = os.getenv('EXPORT_PATH_ORDERS_FULL')

# Configure the Shopify API session
api_version = os.getenv('EXPORT_ORDER_API_VERSION')
shop_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/"
shopify.ShopifyResource.set_site(shop_url)
shopify.ShopifyResource.activate_session(shopify.Session(
    f"{shop_name}.myshopify.com", api_version, admin_api_token))


def add_tag_to_order(order, tag):
    """
    Adds a tag to an order if it's not already present and saves the order.

    Parameters:
    - order: The Shopify order object to be tagged.
    - tag: The tag string to be added to the order.
    """
    if tag not in order.tags:
        order.tags += f", {tag}"  # Append the tag to the existing tags
        order.save()  # Save the order with the new tag


def custom_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    if hasattr(obj, 'attributes'):
        return obj.attributes
    return str(obj)


def fetch_and_export_orders():
    sixty_days_ago = (datetime.now(timezone.utc) -
                      timedelta(days=7)).isoformat()
    orders = shopify.Order.find(created_at_min=sixty_days_ago, status="any")

    if not orders:
        print("No orders found within the specified time range.")
        return

    orders_data = []
    for order in orders:
        if order.cancelled_at is not None or order.financial_status != "paid":
            continue
        if "Downloaded" in order.tags:
            continue

        order_data = order.attributes

        # Extracting line item details separately
        line_items = order_data.pop('line_items', [])
        for item in line_items:
            item_data = item.attributes
            variant_id = item_data.get('variant_id')
            barcode = "Unavailable"
            try:
                variant = shopify.Variant.find(variant_id)
                if variant and hasattr(variant, 'barcode'):
                    barcode = variant.barcode
            except Exception as e:
                print(f"Failed to fetch variant for line item: {e}")

            item_data['barcode'] = barcode
            item_data['order_id'] = order_data['id']
            orders_data.append(item_data)

        # Adding remaining order data without line items
        orders_data.append(order_data)

        # Uncomment the following line to add the "Downloaded" tag
        # add_tag_to_order(order, "Downloaded")

    if orders_data:
        # Export to CSV
        df = pd.json_normalize(orders_data)
        df.to_csv(EXPORT_PATH, index=False)

        # Export to JSON
        json_path = os.path.splitext(EXPORT_PATH)[0] + '.json'
        with open(json_path, 'w') as json_file:
            json.dump(orders_data, json_file, indent=4,
                      default=custom_serializer)

        print(f"Orders exported successfully to {EXPORT_PATH} and {json_path}")
    else:
        print("No new orders to export.")


fetch_and_export_orders()
