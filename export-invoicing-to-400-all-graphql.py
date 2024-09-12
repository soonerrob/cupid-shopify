import os
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pandas as pd
import requests
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

# SMB connection configuration
SERVER_NAME = os.getenv("IBM_SERVER_NAME")
SHARE_NAME = os.getenv("IBM_SHARE_NAME")
USERNAME = os.getenv("IBM_USERNAME")
PASSWORD = os.getenv("IBM_PASSWORD")
DOMAIN = ''

# Global variables
POPULATE_TOTALS_ON_FIRST_ROW_ONLY = True
ENABLE_TAGGING = False  # Control tagging functionality
SMB_FILENAME = "CupidWebAccounting.csv"  # Global filename
ORDER_TAG = "Accounting"  # Global tag for invoicing
DAYS_TO_GO_BACK = 1  # Number of days to go back
# Adjust based on your local time zone
local_timezone = timezone(timedelta(hours=-7))
start_date = (datetime.now(local_timezone) - timedelta(days=DAYS_TO_GO_BACK)
              ).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def add_tag_to_order(order, tag):
    if tag not in order.tags:
        order.tags += f", {tag}"
        order.save()


# Function to send GraphQL query to Shopify with pagination


def fetch_orders_from_graphql(cursor=None):
    url = f"https://{shop_name}.myshopify.com/admin/api/2024-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": admin_api_token
    }

    after_clause = f', after: "{cursor}"' if cursor else ""
    query = f'''
    {{
      orders(first: 250, query: "created_at:>\\\"{start_date}\\\"" {after_clause}) {{
        edges {{
          node {{
            name
            createdAt
            displayFinancialStatus
            displayFulfillmentStatus
            currencyCode
            subtotalPrice
            totalDiscounts
            totalTax
            totalShippingPrice
            shippingLines(first: 250) {{
              nodes {{
                taxLines {{
                  price
                }}
              }}
            }}
            lineItems(first: 250) {{
              edges {{
                node {{
                  variant {{
                    barcode
                  }}
                  quantity
                  originalUnitPrice
                  taxLines(first: 250) {{
                    price
                  }}
                  discountAllocations {{
                    allocatedAmount {{
                      amount
                    }}
                  }}
                  duties {{
                    price {{
                      shopMoney {{
                        amount
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
          cursor
        }}
        pageInfo {{
          hasNextPage
        }}
      }}
    }}
    '''

    response = requests.post(url, json={"query": query}, headers=headers)
    response_json = response.json()

    if 'data' not in response_json:
        print("Response error: ", response_json)
        raise Exception("No data in response.")

    return response_json['data']['orders']


# Function to save the data to SMB


def save_to_smb(file_content, server_name, share_name, smb_filename, username, password, domain=''):
    conn = SMBConnection(username, password, "client_machine", server_name,
                         domain=domain, use_ntlm_v2=True, is_direct_tcp=True)

    if not conn.connect(server_name, 445):
        raise ConnectionError(
            f"Unable to connect to the server: {server_name}")

    try:
        file_size = len(file_content.encode('utf-8'))
        min_size = 25 * 1024  # 25KB
        if file_size < min_size:
            padding_size = min_size - file_size
            file_content += ' ' * padding_size

        with BytesIO(file_content.encode('utf-8')) as file:
            conn.storeFile(share_name, SMB_FILENAME, file)
        print(f"Report saved as: {SMB_FILENAME} on SMB share")

    except Exception as e:
        print(f"Error while saving to SMB: {e}")
    finally:
        conn.close()

# Function to fetch and export all orders using pagination


def fetch_and_export_orders():
    all_orders = []
    cursor = None
    has_next_page = True

    while has_next_page:
        result = fetch_orders_from_graphql(cursor)
        all_orders.extend(result['edges'])
        has_next_page = result['pageInfo']['hasNextPage']
        if has_next_page:
            cursor = result['edges'][-1]['cursor']

    processed_data = []
    for order in all_orders:
        node = order['node']
        first_row = True
        shipping_tax = sum(float(
            tax_line['price']) for line in node['shippingLines']['nodes'] for tax_line in line['taxLines'])

        for item in node['lineItems']['edges']:
            line_item = item['node']
            line_item_tax = sum(float(tax['price'])
                                for tax in line_item['taxLines'])

            row_data = {
                'Order Number': node['name'].replace("#", ""),
                'Order Date': parse(node['createdAt']).strftime("%Y-%m-%d"),
                'Financial Status': node['displayFinancialStatus'],
                'Fulfillment Status': node['displayFulfillmentStatus'],
                'Currency': node['currencyCode'] if first_row else '',
                'Subtotal Price': node['subtotalPrice'] if first_row else '',
                'Total Discounts': node['totalDiscounts'] if first_row else '',
                'Total Tax': node['totalTax'] if first_row else '',
                'Total Shipping Charged': node['totalShippingPrice'] if first_row else '',
                'Total Shipping Tax': shipping_tax if first_row else '',
                'Total VAT Amount': '',  # Empty field for VAT Amount
                'VAT Rate': '',  # Empty field for VAT Rate
                'SKU': line_item['variant']['barcode'],
                'Quantity': line_item['quantity'],
                'Line Item Price': line_item['originalUnitPrice'],
                'Line Item Tax': line_item_tax,
                'Discount Allocations': sum(float(alloc['allocatedAmount']['amount']) for alloc in line_item['discountAllocations']),
                'Duties': sum(float(duty['price']['shopMoney']['amount']) for duty in line_item['duties'])
            }
            processed_data.append(row_data)
            first_row = False

        if ENABLE_TAGGING:
            add_tag_to_order(node, ORDER_TAG)

    df = pd.DataFrame(processed_data)
    df.fillna('', inplace=True)

    # Save to SMB
    csv_content = df.to_csv(index=False, lineterminator='\n', encoding='utf-8')
    save_to_smb(csv_content, SERVER_NAME, SHARE_NAME,
                SMB_FILENAME, USERNAME, PASSWORD, DOMAIN)

    # Save locally
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    local_filename = f"orders-for-accounting-{timestamp}.csv"
    local_export_path = os.path.join(EXPORT_DIR, local_filename)

    with open(local_export_path, 'w', newline='') as f:
        f.write(csv_content)

    print(f"Orders exported successfully to {local_export_path}")


# Run the script
fetch_and_export_orders()
