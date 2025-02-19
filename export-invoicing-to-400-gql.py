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

# Add more prefixes as needed seperated by commas
EXCLUDED_TAG_PREFIXES = ["reship"]


def is_holiday_today(holiday_file=None):
    """
    Checks if today's date matches any date in the holiday file.

    Args:
        holiday_file (str): Path to the file containing holiday dates.

    Returns:
        bool: True if today is a holiday, False otherwise.
    """
    if not holiday_file:
        # Default to the same directory as the script
        holiday_file = os.path.join(os.path.dirname(__file__), 'holidays.txt')

    if not os.path.exists(holiday_file):
        print(f"Holiday file '{holiday_file}' not found. Proceeding as usual.")
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    try:
        with open(holiday_file, 'r') as file:
            holidays = [line.strip() for line in file if line.strip()]
        return today in holidays
    except Exception as e:
        print(f"Error reading holiday file: {e}")
        return False


def should_exclude_order(order_tags):
    """
    Excludes orders that have tags starting with any of the specified prefixes.

    Args:
        order_tags (list): A list of tags associated with an order.

    Returns:
        bool: True if the order should be excluded, False otherwise.
    """
    return any(tag.startswith(prefix) for prefix in EXCLUDED_TAG_PREFIXES for tag in order_tags)


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
                rate
                flatFee {{
                  amount
                  currencyCode
                }}
              }}
              status  # Add status to the query
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

    # Check if today is a holiday
    if is_holiday_today():
        print("Today is a holiday. Skipping execution.")
        exit(0)  # Exit gracefully if it's a holiday

    all_orders = []
    cursor = None
    has_next_page = True
    orders_to_tag = []

    while has_next_page:
        result = fetch_orders_from_graphql(cursor)
        all_orders.extend(result['edges'])
        has_next_page = result['pageInfo']['hasNextPage']
        if has_next_page:
            cursor = result['edges'][-1]['cursor']

    processed_data = []
    for order in all_orders:
        node = order['node']

        # Exclude orders with tags matching EXCLUDED_TAG_PREFIXES
        if should_exclude_order(node.get('tags', [])):
            print(f"Order {node['name']} has excluded tags, skipping.")
            continue

        # Apply filters based on financial and fulfillment status
        if ENABLE_TAGGING:
            # Skip orders that aren't paid, are on hold, or are already fulfilled
            if (node['displayFinancialStatus'] != 'PAID' or 
                node['displayFulfillmentStatus'] == 'ON_HOLD' or 
                node['displayFulfillmentStatus'] != 'UNFULFILLED'):
                # print(f"Order {node['name']} skipped - Status: {node['displayFinancialStatus']}, Fulfillment: {node['displayFulfillmentStatus']}")
                continue

            if ORDER_TAG in node.get('tags', []):
                print(
                    f"Order {node['name']} already has the tag '{ORDER_TAG}', skipping.")
                continue
            
            orders_to_tag.append(node)

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

        # Calculate subtotal for active line items (currentQuantity > 0), excluding discounts
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

        # Find the accepted transaction
        transaction_fee = 0.0
        for transaction in node['transactions']:
            # Consider all successful transactions with fees
            if transaction['status'] == 'SUCCESS':
                fees = transaction.get('fees', [])
                for fee in fees:
                    transaction_fee += float(fee['amount']['amount'])

        total_shipping_discount = sum(float(alloc['allocatedAmount']['amount'])
                                      for line in node['shippingLines']['nodes'] for alloc in line['discountAllocations'])

        for item in node['lineItems']['edges']:
            line_item = item['node']

            # Skip line items with currentQuantity == 0
            if line_item.get('currentQuantity', 0) == 0:
                continue

            # Calculate line item tax and round to 2 decimal places
            line_item_tax = round(
                sum(float(tax['price']) for tax in line_item['taxLines']), 2)

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
        'Duties', 'Transaction Fee'
    ]

    try:
        # Generate CSV content
        csv_content = df.to_csv(index=False, columns=column_order,
                                lineterminator='\n', encoding='utf-8')
        
        # Try to save to SMB first
        save_to_smb(csv_content, SERVER_NAME, SHARE_NAME,
                    SMB_FILENAME, USERNAME, PASSWORD, DOMAIN)

        # If SMB save is successful, save locally
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        local_filename = f"orders-for-invoicing-{timestamp}.csv"
        local_export_path = os.path.join(EXPORT_DIR, local_filename)

        with open(local_export_path, 'w', newline='') as f:
            f.write(csv_content)

        print(f"Orders exported successfully to {local_export_path}")

        # Only tag orders if all exports were successful
        if ENABLE_TAGGING:
            for order in orders_to_tag:
                add_tag_to_order(order, ORDER_TAG)
            print(f"Successfully tagged {len(orders_to_tag)} orders")

    except Exception as e:
        print(f"Error during export process: {e}")
        raise  # Re-raise the exception to ensure the script exits with an error

# Run the script to fetch and export orders
if __name__ == "__main__":
    try:
        fetch_and_export_orders()
    except Exception as e:
        print(f"Script failed with error: {e}")
        exit(1)  # Ensure non-zero exit code on failure