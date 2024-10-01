import os
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pandas as pd
import requests
from dateutil.parser import parse
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

# Load environment variables from .env file
load_dotenv()

# Retrieve essential information from environment variables
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')

# Directory where the CSV file will be saved
EXPORT_DIR = os.getenv('INVOICING_EXPORT_PATH')

# Raise an error if the export directory is not set
if not EXPORT_DIR:
    raise ValueError("INVOICING_EXPORT_PATH not set in environment variables")

# Create the export directory if it doesn't exist
if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)

# SMB connection configuration
SERVER_NAME = os.getenv("IBM_SERVER_NAME")
SHARE_NAME = os.getenv("IBM_SHARE_NAME")
USERNAME = os.getenv("IBM_USERNAME")
PASSWORD = os.getenv("IBM_PASSWORD")
DOMAIN = ''

# Global configuration variables
# Whether to populate totals only on the first row
POPULATE_TOTALS_ON_FIRST_ROW_ONLY = True
ENABLE_TAGGING = True  # Control tagging functionality
SMB_FILENAME = "CupidWebSales.csv"  # Global filename for SMB storage
ORDER_TAG = "Invoicing"  # Global tag to be applied to orders
DAYS_TO_GO_BACK = 6  # Number of days to look back for orders
ENABLE_PADDING = True  # Enable or disable padding for SMB file size
PADDING_CHARACTER = ' '  # Character to use for padding


def add_tag_to_order(order, tag):
    """
    Adds a specific tag to a given order by sending a mutation request
    to Shopify's GraphQL API.

    Args:
        order (dict): A dictionary representing an order node from Shopify.
        tag (str): The tag to add to the order.

    Returns:
        dict: The response from the Shopify API.
    """
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


def save_to_smb(file_content, server_name, share_name, smb_filename, username, password, domain=''):
    """
    Saves a file to an SMB share with optional padding for a minimum file size.

    Args:
        file_content (str): The content of the file to be saved.
        server_name (str): The name of the SMB server.
        share_name (str): The name of the share on the SMB server.
        smb_filename (str): The name of the file to be saved on the SMB share.
        username (str): The username for SMB authentication.
        password (str): The password for SMB authentication.
        domain (str, optional): The domain for SMB authentication. Defaults to ''.

    Raises:
        ConnectionError: If unable to connect to the SMB server.
    """
    conn = SMBConnection(username, password, "client_machine", server_name,
                         domain=domain, use_ntlm_v2=True, is_direct_tcp=True)

    if not conn.connect(server_name, 445):
        raise ConnectionError(
            f"Unable to connect to the server: {server_name}")

    try:
        if ENABLE_PADDING:
            file_size = len(file_content.encode('utf-8'))
            min_size = 25 * 1024  # 25KB
            if file_size < min_size:
                padding_size = min_size - file_size
                file_content += PADDING_CHARACTER * padding_size

        with BytesIO(file_content.encode('utf-8')) as file:
            conn.storeFile(share_name, smb_filename, file)
        print(f"Report saved as: {smb_filename} on SMB share")

    except Exception as e:
        print(f"Error while saving to SMB: {e}")
    finally:
        conn.close()


def fetch_orders_from_graphql(cursor=None):
    """
    Fetches orders from Shopify's GraphQL API with pagination support.

    Args:
        cursor (str, optional): A cursor for paginating through the orders. Defaults to None.

    Returns:
        dict: A dictionary containing the fetched orders and pagination info.
    """
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
    response = requests.post(url, json={"query": query}, headers=headers)
    response_json = response.json()

    if 'data' not in response_json:
        print("Response error: ", response_json)
        raise Exception("No data in response.")

    return response_json['data']['orders']


def fetch_and_export_orders():
    """
    Fetches orders from Shopify's GraphQL API, processes the data,
    and exports the results to both an SMB share and a local CSV file.
    Orders are tagged and filtered based on financial and fulfillment statuses.

    Raises:
        Exception: If any errors occur during data retrieval or export.
    """
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

        # Apply filters based on financial and fulfillment status
        if ENABLE_TAGGING:
            if node['displayFinancialStatus'] != 'PAID' or node['displayFulfillmentStatus'] != 'UNFULFILLED':
                continue  # Skip orders that don't meet the criteria

            if ORDER_TAG in node.get('tags', []):
                print(
                    f"Order {node['name']} already has the tag '{ORDER_TAG}', skipping.")
                continue

        first_row = True
        shipping_tax = sum(float(
            tax_line['price']) for line in node['shippingLines']['nodes'] for tax_line in line['taxLines'])

        total_merchandise_discount = float(node.get('totalDiscounts', '0.00'))
        total_shipping_discount = sum(float(alloc['allocatedAmount']['amount'])
                                      for line in node['shippingLines']['nodes'] for alloc in line['discountAllocations'])

        for item in node['lineItems']['edges']:
            line_item = item['node']
            line_item_tax = sum(float(tax['price'])
                                for tax in line_item['taxLines'])

            barcode = "Unavailable"
            if line_item.get('variant'):
                barcode = line_item['variant'].get('barcode', "Unavailable")

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
                'Total VAT Amount': '0.00',
                'VAT Rate': '0.00',
                'SKU': barcode,
                'Quantity': line_item['quantity'],
                'Line Item Price': line_item['originalUnitPrice'],
                'Line Item Tax': line_item_tax,
                'Discount Allocations': discount_allocations,
                'Duties': sum(float(duty['price']['shopMoney']['amount']) for duty in line_item['duties'])
            }
            processed_data.append(row_data)
            first_row = False

        if ENABLE_TAGGING:
            add_tag_to_order(node, ORDER_TAG)

    df = pd.DataFrame(processed_data)
    df.fillna('', inplace=True)

    column_order = [
        'Order Number', 'Order Date', 'Financial Status', 'Fulfillment Status', 'Currency',
        'Subtotal Price', 'Total Merchandise Discounts', 'Total Tax', 'Total Shipping Charged',
        'Total Shipping Discounts', 'Total Shipping Tax', 'Total VAT Amount', 'VAT Rate',
        'SKU', 'Quantity', 'Line Item Price', 'Line Item Tax', 'Discount Allocations',
        'Duties'
    ]

    csv_content = df.to_csv(index=False, columns=column_order,
                            lineterminator='\n', encoding='utf-8')
    save_to_smb(csv_content, SERVER_NAME, SHARE_NAME,
                SMB_FILENAME, USERNAME, PASSWORD, DOMAIN)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    local_filename = f"orders-for-invoicing-{timestamp}.csv"
    local_export_path = os.path.join(EXPORT_DIR, local_filename)

    with open(local_export_path, 'w', newline='') as f:
        f.write(csv_content)

    print(f"Orders exported successfully to {local_export_path}")


# Run the script to fetch and export orders
fetch_and_export_orders()
