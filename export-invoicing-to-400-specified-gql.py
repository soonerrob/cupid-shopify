import os
from datetime import datetime
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
SMB_FILENAME = "CupidWebSales-70838.csv"  # Global filename
ORDER_TAG = "Invoicing"  # Global tag for invoicing

# List of specific order names (IDs) to fetch
ORDER_NAMES = [
    "#70838"
]


def add_tag_to_order(order, tag):
    tags = order.get('tags', [])
    if tag not in tags:
        tags.append(tag)
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

# Function to send GraphQL query to Shopify to fetch specific orders


def fetch_orders_from_graphql():
    url = f"https://{shop_name}.myshopify.com/admin/api/2024-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": admin_api_token
    }

    # Create the GraphQL query to fetch the specific orders
    order_filter = " OR ".join([f'name:{order}' for order in ORDER_NAMES])
    query = f'''
    {{
      orders(first: 250, query: "{order_filter}") {{
        edges {{
          node {{
            id
            name
            tags  
            createdAt
            displayFinancialStatus
            displayFulfillmentStatus
            currencyCode
            totalDiscounts
            totalTax
            totalShippingPrice
            shippingLines(first: 100) {{
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
            lineItems(first: 100) {{
              edges {{
                node {{
                  variant {{
                    barcode
                  }}
                  currentQuantity
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
            transactions(first: 10) {{
              amountSet {{
                presentmentMoney {{
                  amount
                  currencyCode
                }}
              }}
              fees {{
                amount {{
                  amount
                  currencyCode
                }}
              }}
              status
            }}
          }}
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

# Function to fetch and export specific orders using the list


def fetch_and_export_orders():
    all_orders = fetch_orders_from_graphql()

    processed_data = []
    for order in all_orders['edges']:
        node = order['node']
        first_row = True

        # Calculate shipping tax and round to 2 decimal places
        shipping_tax = round(sum(
            float(tax_line['price']) for line in node['shippingLines']['nodes']
            for tax_line in line['taxLines']
        ), 2)

        # Calculate total merchandise discounts for active line items (currentQuantity > 0)
        total_merchandise_discount = sum(
            sum(float(alloc['allocatedAmount']['amount'])
                for alloc in line_item['node']['discountAllocations'])
            for line_item in node['lineItems']['edges']
            if line_item['node']['currentQuantity'] > 0
        )

        # Calculate subtotal for active line items (currentQuantity > 0)
        subtotal = sum(
            (float(line_item['node']['originalUnitPrice']) * line_item['node']['currentQuantity']) -
            sum(float(alloc['allocatedAmount']['amount'])
                for alloc in line_item['node']['discountAllocations'])
            for line_item in node['lineItems']['edges']
            if line_item['node']['currentQuantity'] > 0
        )

        # Calculate total tax for active line items (currentQuantity > 0) and round to 2 decimal places
        total_tax = round(sum(
            sum(float(tax['price']) for tax in line_item['node']['taxLines'])
            for line_item in node['lineItems']['edges']
            if line_item['node']['currentQuantity'] > 0
        ), 2)

        # Initialize the transaction fee calculation
        transaction_fee = 0.0
        for transaction in node['transactions']:
            if transaction['status'] == 'SUCCESS':
                for fee in transaction.get('fees', []):
                    transaction_fee += float(fee['amount']['amount'])

        total_shipping_discount = sum(float(alloc['allocatedAmount']['amount'])
                                      for line in node['shippingLines']['nodes'] for alloc in line['discountAllocations'])

        # Loop through each line item and process only those with currentQuantity > 0
        for item in node['lineItems']['edges']:
            line_item = item['node']

            # Skip line items with currentQuantity == 0
            if line_item['currentQuantity'] == 0:
                continue

            # Calculate line item tax and discount allocations
            line_item_tax = round(
                sum(float(tax['price']) for tax in line_item['taxLines']), 2)

            discount_allocations = sum(float(
                alloc['allocatedAmount']['amount']) for alloc in line_item['discountAllocations'])

            # Set default barcode value
            barcode = line_item['variant'].get(
                'barcode', "Unavailable") if line_item.get('variant') else "Unavailable"

            row_data = {
                'Order Number': node['name'].replace("#", ""),
                'Order Date': parse(node['createdAt']).strftime("%Y-%m-%d"),
                'Financial Status': node['displayFinancialStatus'],
                'Fulfillment Status': node['displayFulfillmentStatus'],
                'Currency': node['currencyCode'] if first_row else '',
                'Subtotal Price': f"{round(subtotal, 2):.2f}" if first_row else '0.00',
                'Total Merchandise Discounts': total_merchandise_discount if first_row else '0.00',
                'Total Shipping Discounts': total_shipping_discount if first_row else '0.00',
                'Total Tax': round(total_tax + shipping_tax, 2) if first_row else '0.00',
                'Total Shipping Charged': node['totalShippingPrice'] if first_row else '0.00',
                'Total Shipping Tax': shipping_tax if first_row else '0.00',
                'Total VAT Amount': '0.00',
                'VAT Rate': '0.00',
                'SKU': barcode,
                'Quantity': line_item['currentQuantity'],
                'Line Item Price': line_item['originalUnitPrice'],
                'Line Item Tax': line_item_tax,
                'Discount Allocations': discount_allocations,
                'Duties': sum(float(duty['price']['shopMoney']['amount']) for duty in line_item['duties']),
                'Transaction Fee': transaction_fee if first_row else '0.00'
            }
            processed_data.append(row_data)
            first_row = False  # After the first row, clear first_row-specific fields

        # Optional tagging for orders
        if ENABLE_TAGGING:
            add_tag_to_order(node, ORDER_TAG)

    df = pd.DataFrame(processed_data)
    df.fillna('', inplace=True)

    # Define the order of the columns to match the other script
    column_order = [
        'Order Number', 'Order Date', 'Financial Status', 'Fulfillment Status', 'Currency',
        'Subtotal Price', 'Total Merchandise Discounts', 'Total Tax', 'Total Shipping Charged',
        'Total Shipping Discounts', 'Total Shipping Tax', 'Total VAT Amount', 'VAT Rate',
        'SKU', 'Quantity', 'Line Item Price', 'Line Item Tax', 'Discount Allocations',
        'Duties', 'Transaction Fee'
    ]

    # Save to SMB
    csv_content = df.to_csv(index=False, columns=column_order,
                            lineterminator='\n', encoding='utf-8')
    save_to_smb(csv_content, SERVER_NAME, SHARE_NAME,
                SMB_FILENAME, USERNAME, PASSWORD, DOMAIN)

    # Save locally
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    local_filename = f"orders-for-invoicing-spec-{timestamp}.csv"
    local_export_path = os.path.join(EXPORT_DIR, local_filename)

    with open(local_export_path, 'w', newline='') as f:
        f.write(csv_content)

    print(f"Orders exported successfully to {local_export_path}")


# Run the script
fetch_and_export_orders()
