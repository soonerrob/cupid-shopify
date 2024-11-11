import csv
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Global Variables
INPUT_FOLDER = 'input'
PROCESSED_FOLDER = 'processed'
INPUT_FILE_NAME = 'closeouts2.xlsx'  # Default input file name, can be changed

# Ensure the folders exist
os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# Full paths for input and output
INPUT_FILE = os.path.join(INPUT_FOLDER, INPUT_FILE_NAME)
INPUT_FILENAME = os.path.splitext(os.path.basename(INPUT_FILE))[0]
NOT_FOUND_FILE = os.path.join(
    PROCESSED_FOLDER, f'{INPUT_FILENAME}-not_found.csv')
PROGRESS_FILE = os.path.join(
    PROCESSED_FOLDER, f'{INPUT_FILENAME}-progress.csv')


# Shopify API credentials
API_ACCESS_TOKEN = os.getenv('API_ACCESS_TOKEN')
SHOP_NAME = os.getenv('SHOP_NAME')
GRAPHQL_ENDPOINT = f'https://{SHOP_NAME}.myshopify.com/admin/api/2024-10/graphql.json'

HEADERS = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': API_ACCESS_TOKEN
}

# Mutation for updating cost per item
MUTATION = '''
mutation inventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    inventoryItem {
      id
      unitCost {
        amount
      }
    }
    userErrors {
      message
    }
  }
}
'''

# Query to fetch inventory items based on style-color SKU (without size)
QUERY = '''
query ($sku: String!) {
  productVariants(first: 10, query: $sku) {
    edges {
      node {
        sku
        inventoryItem {
          id
        }
      }
    }
  }
}
'''

# Rate limit tracking
POINTS_PER_SECOND = 100
points_available = 100
last_request_time = time.time()

# Function to throttle API requests based on rate limits


def check_rate_limit(query_cost):
    global points_available, last_request_time

    current_time = time.time()
    time_passed = current_time - last_request_time
    points_available = min(
        POINTS_PER_SECOND, points_available + time_passed * POINTS_PER_SECOND)

    if points_available < query_cost:
        wait_time = (query_cost - points_available) / POINTS_PER_SECOND
        print(f"Rate limit exceeded, sleeping for {wait_time:.2f} seconds...")
        time.sleep(wait_time)

    points_available -= query_cost
    last_request_time = time.time()

# Function to update the unit cost for a specific inventory item


def update_cost_per_item(inventory_item_id, cost):
    mutation = {
        "query": MUTATION,
        "variables": {
            "id": inventory_item_id,
            "input": {
                "cost": float(cost),  # Ensure cost is a float
            }
        }
    }

    # Send the request to Shopify
    check_rate_limit(10)
    response = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json=mutation)

    # Try to handle JSON decoding errors gracefully
    try:
        data = response.json()
        # Only print success message and any user errors
        if response.status_code == 200 and 'data' in data and not data['data']['inventoryItemUpdate']['userErrors']:
            # Commented out to reduce verbosity
            # print(f"Successfully updated cost for inventory item {inventory_item_id} to {cost}")
            pass
        else:
            print(
                f"Failed to update cost for inventory item {inventory_item_id}: {data.get('errors', 'Unknown error')}")
            if 'data' in data and 'inventoryItemUpdate' in data['data']:
                for error in data['data']['inventoryItemUpdate']['userErrors']:
                    print(f"User Error: {error['message']}")
    except ValueError:
        print(
            f"Failed to decode JSON response from Shopify for inventory item {inventory_item_id}.")


# Function to read input file based on file extension
def read_input_file(file_path):
    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext == '.csv':
        return pd.read_csv(file_path)
    elif file_ext == '.xlsx':
        return pd.read_excel(file_path)
    else:
        raise ValueError(
            f"Unsupported file format: {file_ext}. Only .csv and .xlsx are supported.")


# Function to load already processed SKUs from the progress file
def load_processed_skus():
    processed_skus = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                processed_skus.add(row[0].strip())
    return processed_skus


# Set to track processed SKUs in the current run
current_run_processed_skus = set()

# Function to save processed SKUs in the progress file


def save_progress(style_color):
    # Ensure we only save progress once per style-color combination
    if style_color not in current_run_processed_skus:
        # print(f"Saving progress for: {style_color}")
        current_run_processed_skus.add(style_color)

        # Split style_color from the right to preserve hyphens in style names
        parts = style_color.rsplit('-', 1)  # Split only on the last hyphen

        if len(parts) == 2:
            style = parts[0]  # First part is the full style
            color = parts[1]  # Second part is the color
        else:
            style = style_color  # Fallback if splitting fails
            color = ''           # No color if there are no two parts

        # Append to progress file
        with open(PROGRESS_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([style, color])


# Function to process XLSX file and update costs
def process_file(file):
    df = read_input_file(file)  # Use the new read_input_file function
    processed_skus = load_processed_skus()  # Load already processed SKUs
    total_rows = len(df)

    # Notify total rows at start
    print(f"Starting processing of {total_rows} rows...")

    # Calculate rows per 10% increment
    # Ensure at least 1 row per increment
    rows_per_10_percent = max(total_rows // 10, 1)

    for index, row in df.iterrows():
        if 'STYLE' in row and 'CLRCDE' in row and 'COST' in row and 'DIM' in row:
            style = str(row['STYLE']).strip()
            color = str(row['CLRCDE']).strip()
            dim = str(row['DIM']).strip() if pd.notna(
                row['DIM']) else ''  # Ensure DIM is available

            # Rule 1: Strip leading 'W' if it exists
            if style.startswith('W'):
                style = style[1:]

            # Rule 2: Strip trailing 'W' if it exists
            if style.endswith('W'):
                style = style[:-1]

            # Rule 3: Remove trailing 'WC' if it exists
            if style.endswith('WC'):
                style = style[:-2]

            # Rule 4: Replace '-1F' or '-2F' with '-5'
            if style.endswith('-1F') or style.endswith('-2F'):
                style = style[:-3] + '-5'

            # Rule 5: Remove '-1' or '-2' at the end
            elif style.endswith('-1') or style.endswith('-2'):
                style = style[:-2]

            # Ensure the color code is 3 digits
            color = color.zfill(3)

            # Build the style-color (without size)
            style_color = f"{style}-{color}"
            cost = str(row['COST']).strip()

            # Skip already processed items
            if style_color in processed_skus:
                continue

            # Get all inventory items matching the style-color
            inventory_items = get_inventory_items_by_style_color_and_exact_dim(
                style_color, dim)

            if inventory_items:
                # Update the cost for each variant that ends with the specified DIM (e.g., 'B')
                for item in inventory_items:
                    update_cost_per_item(item['inventory_item_id'], cost)
                    # Save progress after each successful update
                    save_progress(style_color)
            else:
                save_not_found_result(style_color, dim, cost)

        # Print progress every 10%
        if (index + 1) % rows_per_10_percent == 0 or index + 1 == total_rows:
            percent_complete = ((index + 1) / total_rows) * 100
            print(
                f"Processed {index + 1} of {total_rows} rows ({percent_complete:.1f}% complete)")

    print(f"Processing completed for {total_rows} rows.")


# Function to fetch inventory items by style-color and filter by exact dimension


def get_inventory_items_by_style_color_and_exact_dim(style_color, dim):
    # Query for all variants of the style-color (ignores size initially)
    query = {
        "query": QUERY,
        "variables": {
            "sku": style_color
        }
    }

    # print(f"Querying Shopify for style-color: {style_color}")
    check_rate_limit(10)
    response = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json=query)
    data = response.json()

    if response.status_code != 200 or 'errors' in data:
        print(
            f"Error fetching inventory items for style-color {style_color}: {data}")
        return None

    inventory_items = []
    edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
    if edges:
        # Iterate through each variant and find those that end with the specified exact DIM (e.g., B, C, etc.)
        for edge in edges:
            sku = edge['node']['sku']
            # Split SKU into parts to match the last part of the SKU with the exact DIM
            sku_parts = sku.split('-')
            # Exact match for the DIM (ignoring size)
            if len(sku_parts) > 2 and sku_parts[-1].endswith(dim):
                # print(f"Found matching SKU: {sku} with inventory item ID: {edge['node']['inventoryItem']['id']}")
                inventory_items.append({
                    'sku': edge['node']['sku'],
                    'inventory_item_id': edge['node']['inventoryItem']['id']
                })
    else:
        # print(f"No variants found for style-color {style_color}.")
        pass

    return inventory_items

# Function to save not found results immediately


# Function to save not found results immediately
def save_not_found_result(style_color, dim, cost):
    # Ensure the same structure as the input file
    headers = ['CUSTNO', 'CUSTNM', 'STYLE', 'CLRCDE',
               'SIZE', 'DIM', 'COST', 'GSELL', 'NSELL']

    # Split style_color from the right to preserve hyphens in style names
    parts = style_color.rsplit('-', 1)  # Split only on the last hyphen

    if len(parts) == 2:
        style = parts[0]  # First part is the full style
        color = parts[1]  # Second part is the color
    else:
        style = style_color  # Fallback, if splitting fails
        color = ''           # No color if there are no two parts

    # Construct the not_found entry with the same structure as the input file
    not_found_row = ['', '', style, color, '',
                     dim, cost, '', '']  # Blank other fields

    # Check if file exists and write headers if it doesn't
    file_exists = os.path.isfile(NOT_FOUND_FILE)

    with open(NOT_FOUND_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            # Write the headers first if the file is new
            writer.writerow(headers)
        writer.writerow(not_found_row)


# Main function to process the file
def main():
    print(f"Starting the process for input file: {INPUT_FILE}")
    process_file(INPUT_FILE)  # INPUT_FILE was the intended variable
    print(
        f"Cost update completed. Files created:\n- {NOT_FOUND_FILE}\n- {PROGRESS_FILE}")


if __name__ == "__main__":
    main()
