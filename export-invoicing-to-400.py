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
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')

# Directory where the CSV file will be saved
EXPORT_DIR = "export-invoicing-to-400-archive"
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
POPULATE_TOTALS_ON_FIRST_ROW_ONLY = True
ENABLE_TAGGING = True  # Control tagging functionality
SMB_FILENAME = "CupidWebSales.csv"  # Global filename
ORDER_TAG = "Invoicing"  # Global tag for invoicing
DAYS_TO_GO_BACK = 6  # Global variable to set the number of days to go back
ENABLE_PADDING = True  # Enable or disable padding up to 25KB
PADDING_CHARACTER = ' '  # Character to use for padding


def add_tag_to_order(order, tag):
    if tag not in order.tags:
        order.tags += f", {tag}"
        order.save()


def save_to_smb(file_content, server_name, share_name, smb_filename, username, password, domain=''):
    conn = SMBConnection(username, password, "client_machine", server_name,
                         domain=domain, use_ntlm_v2=True, is_direct_tcp=True)

    if not conn.connect(server_name, 445):
        raise ConnectionError(
            f"Unable to connect to the server: {server_name}")

    try:
        # Only add padding if ENABLE_PADDING is True
        if ENABLE_PADDING:
            file_size = len(file_content.encode('utf-8'))
            min_size = 25 * 1024  # 25KB
            if file_size < min_size:
                padding_size = min_size - file_size
                # Add the specified padding character
                file_content += PADDING_CHARACTER * padding_size

        # Write content to a file using pysmb
        with BytesIO(file_content.encode('utf-8')) as file:
            conn.storeFile(share_name, smb_filename, file)
        print(f"Report saved as: {smb_filename} on SMB share")

    except Exception as e:
        print(f"Error while saving to SMB: {e}")
    finally:
        conn.close()


def fetch_and_export_orders():
    # Using the global DAYS_TO_GO_BACK to calculate the date to go back
    start_date = (datetime.now(timezone.utc) -
                  timedelta(days=DAYS_TO_GO_BACK)).isoformat()

    orders_data = []
    next_page_url = None

    while True:
        if next_page_url:
            # Continue fetching from the next page
            orders = shopify.Order.find(from_=next_page_url)
        else:
            # Fetch the first page of orders
            orders = shopify.Order.find(
                created_at_min=start_date, status="any", limit=50)

        if not orders:
            print("No more orders found within the specified time range.")
            break

        for order in orders:
            # Only proceed if the order is 'paid' and 'unfulfilled'
            # if order.cancelled_at is not None or (order.financial_status and order.financial_status.lower() != "paid"):
            if order.cancelled_at is not None or (order.financial_status and order.financial_status.lower() != "paid") or (order.fulfillment_status and order.fulfillment_status.lower() != "unfulfilled"):
                continue

            # If tagging is disabled, process all orders regardless of tags
            if not ENABLE_TAGGING:
                pass  # Do nothing, process all orders
            else:
                # Skip the order only if tagging is enabled and the order has the 'Invoicing' tag
                if ORDER_TAG in order.tags:
                    continue

            first_row = True
            order_export_data = []  # Temp storage for each order's line items
            for item in order.line_items:
                order_date = parse(order.created_at).strftime("%Y-%m-%d")
                barcode = "Unavailable"
                line_item_tax = 0.0
                vat_amount = 0.0
                vat_rate = 0.0

                try:
                    variant = shopify.Variant.find(item.variant_id)
                    if variant:
                        barcode = getattr(variant, 'barcode', "Unavailable")
                except Exception as e:
                    print(
                        f"Failed to fetch variant for line item in order {order.order_number}, item: {item.title}. Error: {e}")

                for tax_line in item.tax_lines:
                    line_item_tax += float(tax_line.price)
                    if tax_line.title.lower() == 'vat':
                        vat_amount += float(tax_line.price)
                        vat_rate = tax_line.rate

                shipping_charged = sum(float(getattr(line, 'price', 0))
                                       for line in order.shipping_lines)
                shipping_tax = sum(float(
                    tax_line.price) for line in order.shipping_lines for tax_line in line.tax_lines)

                row_data = {
                    'Order Number': order.order_number,
                    'Order Date': order_date,
                    'Financial Status': order.financial_status,
                    'Fulfillment Status': 'unfulfilled',
                    # 'Fulfillment Status': order.fulfillment_status,
                    'Currency': order.currency if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '',
                    'Subtotal Price': order.subtotal_price if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'Total Discounts': order.total_discounts if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'Total Tax': order.total_tax if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'Total Shipping Charged': shipping_charged if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'Total Shipping Tax': shipping_tax if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'Total VAT Amount': vat_amount if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'VAT Rate': vat_rate if first_row or not POPULATE_TOTALS_ON_FIRST_ROW_ONLY else '0.00',
                    'SKU': barcode,
                    'Quantity': item.quantity,
                    'Line Item Price': item.price,
                    'Line Item Tax': line_item_tax,
                    'Discount Allocations': sum(float(alloc.amount) for alloc in item.discount_allocations),
                    'Duties': sum(float(duty.price) for duty in item.duties)
                }

                if any(value != '' and value != 0 for value in row_data.values()):
                    order_export_data.append(row_data)
                first_row = False

            # Only add the order if it contains line items to export
            if order_export_data:
                orders_data.extend(order_export_data)

                # Only tag the order if tagging is enabled
                if ENABLE_TAGGING:
                    add_tag_to_order(order, ORDER_TAG)

        # Get the next page URL for pagination
        next_page_url = orders.next_page_url
        if not next_page_url:
            break

    if orders_data:
        df = pd.DataFrame(orders_data)
        df.fillna('', inplace=True)

        # Generate CSV content
        csv_content = df.to_csv(
            index=False, lineterminator='\n', encoding='utf-8')

        # Save to SMB
        save_to_smb(csv_content, SERVER_NAME, SHARE_NAME,
                    SMB_FILENAME, USERNAME, PASSWORD, DOMAIN)

        # Save to local path with timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        local_filename = f"orders-for-invoicing-{timestamp}.csv"
        local_export_path = os.path.join(EXPORT_DIR, local_filename)

        with open(local_export_path, 'w', newline='') as f:
            f.write(csv_content)

        print(f"Orders exported successfully to {local_export_path}")

    else:
        print("No new orders to export.")


# Run the script
fetch_and_export_orders()
