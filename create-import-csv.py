import pandas as pd

# Specify the number of orders to process
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
print("Items columns:", items_df.columns.tolist())
print("Payments columns:", payments_df.columns.tolist())

# Merge orders with addresses (assuming 'entity_id' is the common key and it's unique)
merged_df = pd.merge(orders_df, addresses_df, on='entity_id', how='left')

# Further merges with items and payments can go here
# For example:
# merged_df = pd.merge(merged_df, items_df, on='entity_id', how='left')
# merged_df = pd.merge(merged_df, payments_df, on='entity_id', how='left')

# Define the Shopify column mappings here
# Adjust the mappings to your actual data
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

# Debugging: Print the DataFrame to check if the columns are correctly renamed
print("Shopify orders columns after rename:", shopify_orders.columns.tolist())

# Format dates to match Shopify's expected format (if needed)
shopify_orders['Created at'] = pd.to_datetime(
    shopify_orders['Created at']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Handle the order lines separately since they could be more complex
# Here you might need a loop or a groupby operation to transform the items_df into line items per order
# Then, merge or concatenate these line items with the shopify_orders dataframe

# shopify_orders['Billing Name'] = merged_df['billing_firstname'] + \
#     ' ' + merged_df['billing_lastname']
# shopify_orders['Shipping Name'] = merged_df['shipping_firstname'] + \
#     ' ' + merged_df['shipping_lastname']

# Select only the required number of orders
if orders_to_process != -1:
    shopify_orders = shopify_orders.iloc[:orders_to_process]

# Save the DataFrame to a CSV file ready for import
output_file = 'shopify_orders_import.csv'
shopify_orders.to_csv(output_file, index=False)

print(f"Shopify import CSV created successfully and saved to {output_file}.")
