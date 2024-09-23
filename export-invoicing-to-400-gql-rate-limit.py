import os
import time
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
SMB_FILENAME = "CupidWebSales-rate-limit.csv"  # Global filename
ORDER_TAG = "Invoicing"  # Global tag for invoicing
DAYS_TO_GO_BACK = 14  # Global variable to set the number of days to go back
ENABLE_PADDING = True  # Enable or disable padding up to 25KB
PADDING_CHARACTER = ' '  # Character to use for padding

# Function to add tag to orders


def add_tag_to_order(order, tag):
    mutation_query = f'''
    mutation {{
        tagsAdd(id: "{order['id']}", tags: ["{tag}"]) {{
            userErrors {{
                field
                message
            }}
        }}
    }}
    '''
    url = f"https://{shop_name}.myshopify.com/admin/api/2024-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": admin_api_token
    }
    response = requests.post(
        url, json={"query": mutation_query}, headers=headers)
    return response.json()

# Function to save the data to SMB


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

        with BytesIO(file_content.encode('utf-8')) as file:
            conn.storeFile(share_name, smb_filename, file)
        print(f"Report saved as: {smb_filename} on SMB share")

    except Exception as e:
        print(f"Error while saving to SMB: {e}")
    finally:
        conn.close()

# Function to send GraphQL query to Shopify with pagination


def fetch_orders_from_graphql(cursor=None):
    start_date = (datetime.now(timezone.utc) -
                  timedelta(days=DAYS_TO_GO_BACK)).isoformat()
    after_clause = f', after: "{cursor}"' if cursor else ""
    query = f'''
    {{
      orders(first: 250, query: "created_at:>\\\"{start_date}\\\"" {after_clause}) {{
        edges {{
          node {{
            id
            name
            tags
            createdAt
            displayFinancialStatus
            displayFulfillmentStatus
            currencyCode
            subtotalPrice
            totalDiscounts
            totalTax
            totalShippingPrice
            shippingLines(first: 10) {{
              nodes {{
                discountAllocations {{
                  allocatedAmount {{
                    amount
                  }}
                }}
                taxLines {{
                  price
                }}
              }}
            }}
            lineItems(first: 50) {{
              edges {{
                node {{
                  variant {{
                    barcode
                  }}
                  quantity
                  originalUnitPrice
                  taxLines {{
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

    url = f"https://{shop_name}.myshopify.com/admin/api/2024-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": admin_api_token
    }

    print(f"Sending query with cursor: {cursor}")
    response = requests.post(url, json={"query": query}, headers=headers)
    print(f"Response status code: {response.status_code}")

    response_json = response.json()

    if 'errors' in response_json and 'THROTTLED' in response_json['errors'][0]['message']:
        print("Throttled! Waiting for 60 seconds to allow the bucket to refill.")
        time.sleep(60)  # Directly sleep for 60 seconds
        return fetch_orders_from_graphql(cursor)

    if 'data' not in response_json:
        # Log the problematic response for debugging
        print(f"Problematic response at cursor {cursor}: {response_json}")
        raise Exception("No data in response.")

    print(
        f"Query successful. Orders retrieved: {len(response_json['data']['orders']['edges'])}")
    return response_json['data']['orders']


# Adjust fetch_and_export_orders function to reflect the new logic

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

        # Apply the 'paid' and 'unfulfilled' filters only if tagging is enabled
        if ENABLE_TAGGING:
            if node['displayFinancialStatus'] != 'PAID' or node['displayFulfillmentStatus'] != 'UNFULFILLED':
                continue  # Skip orders that don't meet the criteria

        first_row = True
        # Safely calculate the shipping tax only if shippingLines exist
        shipping_tax = sum(float(
            tax_line['price']) for line in node.get('shippingLines', {}).get('nodes', []) for tax_line in line.get('taxLines', []))

        # Use totalDiscounts for merchandise discounts at the order level
        total_merchandise_discount = float(node.get('totalDiscounts', '0.00'))

        # Safely calculate total shipping discounts only if shippingLines exist
        total_shipping_discount = sum(float(
            alloc['allocatedAmount']['amount']) for line in node.get('shippingLines', {}).get('nodes', []) for alloc in line.get('discountAllocations'))

        # Safely check if lineItems exist before processing
        if 'lineItems' in node:
            for item in node['lineItems']['edges']:
                line_item = item['node']
                # Debugging statement
                print(f"Processing line item: {line_item}")
                line_item_tax = sum(float(tax['price'])
                                    for tax in line_item['taxLines'])

                # Set default barcode value
                barcode = "Unavailable"

                # Check if the variant and barcode exist
                if line_item.get('variant'):
                    barcode = line_item['variant'].get(
                        'barcode', "Unavailable")

                # Calculate discount allocations for this specific line item
                discount_allocations = sum(float(
                    alloc['allocatedAmount']['amount']) for alloc in line_item['discountAllocations'])

                row_data = {
                    'Order Number': node['name'].replace("#", ""),
                    'Order Date': parse(node['createdAt']).strftime("%Y-%m-%d"),
                    'Financial Status': node['displayFinancialStatus'],
                    'Fulfillment Status': node['displayFulfillmentStatus'],
                    'Currency': node['currencyCode'] if first_row else '',
                    'Subtotal Price': node['subtotalPrice'] if first_row else '0.00',
                    'Total Merchandise Discounts': total_merchandise_discount if first_row else '0.00',
                    'Total Shipping Discounts': total_shipping_discount if first_row else '0.00',
                    'Total Tax': node['totalTax'] if first_row else '0.00',
                    'Total Shipping Charged': node['totalShippingPrice'] if first_row else '0.00',
                    'Total Shipping Tax': shipping_tax if first_row else '0.00',
                    'Total VAT Amount': '0.00',  # Empty field for VAT Amount
                    'VAT Rate': '0.00',  # Empty field for VAT Rate
                    'SKU': barcode,  # Use the default value if barcode is missing
                    'Quantity': line_item['quantity'],
                    'Line Item Price': line_item['originalUnitPrice'],
                    'Line Item Tax': line_item_tax,
                    # Add new column for line item discounts
                    'Discount Allocations': discount_allocations,
                    'Duties': sum(float(duty['price']['shopMoney']['amount']) for duty in line_item['duties'])
                }
                processed_data.append(row_data)
                first_row = False
        else:
            # Debugging statement
            print(f"No line items found for order {node['name']}")

        if ENABLE_TAGGING:
            add_tag_to_order(node, ORDER_TAG)

    # Convert processed data into DataFrame
    # Debugging statement
    print(f"Processed data count: {len(processed_data)}")
    df = pd.DataFrame(processed_data)

    print(df.head())  # Print first few rows to verify data

    # Define the order of the columns
    column_order = [
        'Order Number', 'Order Date', 'Financial Status', 'Fulfillment Status', 'Currency',
        'Subtotal Price', 'Total Merchandise Discounts', 'Total Tax', 'Total Shipping Charged',
        'Total Shipping Discounts', 'Total Shipping Tax', 'Total VAT Amount', 'VAT Rate',
        'SKU', 'Quantity', 'Line Item Price', 'Line Item Tax', 'Discount Allocations',
        'Duties'
    ]

    # Ensure all required columns are present in the DataFrame, fill missing ones with default values
    for col in column_order:
        if col not in df.columns:
            df[col] = '0.00'  # Fill missing columns with default values

    # Save to SMB
    csv_content = df.to_csv(index=False, columns=column_order,
                            lineterminator='\n', encoding='utf-8')
    save_to_smb(csv_content, SERVER_NAME, SHARE_NAME,
                SMB_FILENAME, USERNAME, PASSWORD, DOMAIN)

    # Save locally
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    local_filename = f"orders-for-invoicing-{timestamp}.csv"
    local_export_path = os.path.join(EXPORT_DIR, local_filename)

    with open(local_export_path, 'w', newline='') as f:
        f.write(csv_content)

    print(f"Orders exported successfully to {local_export_path}")


# Run the script
fetch_and_export_orders()
