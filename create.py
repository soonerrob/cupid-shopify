import pandas as pd

# Specify the number of orders to process, for example, all orders
orders_to_process = 1  # Use -1 for all orders

# Define the path to the magento_files subfolder
folder_path = 'magento_files/'

# Load Magento export files from the magento_files subfolder
orders_df = pd.read_csv(folder_path + 'sales_order.csv',
                        dtype=str, low_memory=False, on_bad_lines='skip')
addresses_df = pd.read_csv(folder_path + 'sales_order_address.csv',
                           dtype=str, low_memory=False, on_bad_lines='skip')
items_df = pd.read_csv(folder_path + 'sales_order_item.csv',
                       dtype=str, low_memory=False, on_bad_lines='skip')
payments_df = pd.read_csv(folder_path + 'sales_order_payment.csv',
                          dtype=str, low_memory=False, on_bad_lines='skip')


# Debugging: Print column names to ensure they exist
print("Orders columns:", orders_df.columns.tolist())
print("Addresses columns:", addresses_df.columns.tolist())


# Merge orders with addresses (assuming 'entity_id' is the common key and it's unique)
merged_df = pd.merge(orders_df, addresses_df, on='entity_id', how='left')

# Define the Shopify column mappings here
# This is a hypothetical mapping - adjust the mappings to your actual data
shopify_column_mappings = {
    'increment_id': 'Name',  # Shopify's "Name" is the human-readable order identifier
    'created_at': 'Created at',  # The date and time when the order was created
    'base_grand_total': 'Total',  # The total price of the order
    # Use your logic to set 'paid', 'partially_paid', 'refunded', etc.
    'base_total_paid': 'Financial Status',
    # Use your logic to set 'fulfilled', 'partial', 'unfulfilled', etc.
    'status': 'Fulfillment Status',
    'customer_email': 'Email',
    'billing_name': 'Billing Name',  # Combine billing first name and last name
    'shipping_name': 'Shipping Name',  # Combine shipping first name and last name
    'base_total_tax': 'Taxes',  # The total taxes charged on the order
    'base_shipping_amount': 'Shipping',  # The total shipping charges
    # Add additional mappings as needed
}

# Rename the columns in merged_df using the mapping
shopify_orders = merged_df.rename(columns=shopify_column_mappings)

# Format dates to match Shopify's expected format
shopify_orders['Created at'] = pd.to_datetime(
    shopify_orders['Created at']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Now handle the additional columns that require data transformation
# For example, combine first name and last name into a full name for billing and shipping
shopify_orders['Billing Name'] = merged_df['billing_firstname'] + \
    ' ' + merged_df['billing_lastname']
shopify_orders['Shipping Name'] = merged_df['shipping_firstname'] + \
    ' ' + merged_df['shipping_lastname']

# Transform other data as required by Shopify, such as line items
# Here you'll write the logic to convert your items data into Shopify's line items format

# Assuming you have a 'line_items' DataFrame prepared, you would merge it here
# This is a placeholder for illustrative purposes; your actual implementation will vary
# shopify_orders = pd.merge(shopify_orders, line_items, on='Name', how='left')

# Select only the number of orders specified
if orders_to_process != -1:
    shopify_orders = shopify_orders.iloc[:orders_to_process]

# Save the DataFrame to a CSV file ready for import
output_file = 'shopify_orders_import.csv'
shopify_orders.to_csv(output_file, index=False)

print(f"Shopify import CSV created successfully and saved to {output_file}.")
