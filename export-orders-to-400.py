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
EXPORT_PATH = os.getenv('EXPORT_PATH')

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
    # Check if the tag is not already in the order's tags
    if tag not in order.tags:
        order.tags += f", {tag}"  # Append the tag to the existing tags
        order.save()  # Save the order with the new tag


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

        # Uncomment the following line to add the "Downloaded" tag
        # add_tag_to_order(order, "Downloaded")

    if orders_data:
        df = pd.DataFrame(orders_data)
        df.to_csv(EXPORT_PATH, index=False, header=False)
        print(f"Orders exported successfully to {EXPORT_PATH}")
    else:
        print("No new orders to export.")


fetch_and_export_orders()
