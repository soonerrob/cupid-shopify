import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# Load environment variables from .env file
load_dotenv()

# Shopify GraphQL API credentials
shop_name = os.getenv('SHOP_NAME')
admin_api_token = os.getenv('API_ACCESS_TOKEN')
export_path = os.getenv('EXPORT_PAYMENTS_PATH')

# Global date variables
start_date = "2024-09-01"  # Replace with your desired start date
end_date = "2024-09-30"  # Replace with your desired end date


def fetch_payouts():
    """
    Fetches Shopify payouts within the specified date range.
    """
    all_payouts = []
    has_next_page = True
    after_cursor = None

    while has_next_page:
        # Adjust the query with pagination support
        after_clause = f', after: "{after_cursor}"' if after_cursor else ""
        query = f'''
        {{
          shopifyPaymentsAccount {{
            payouts(first: 100{after_clause}) {{
              edges {{
                node {{
                  id
                  issuedAt
                  net {{
                    amount
                    currencyCode
                  }}
                  gross {{
                    amount
                  }}
                  status
                  summary {{
                    adjustmentsGross {{
                      amount
                    }}
                    chargesGross {{
                      amount
                    }}
                    refundsFeeGross {{
                      amount
                    }}
                    reservedFundsGross {{
                      amount
                    }}
                    chargesFee {{
                      amount
                    }}
                    retriedPayoutsGross {{
                      amount
                    }}
                  }}
                }}
                cursor
              }}
              pageInfo {{
                hasNextPage
                endCursor
              }}
            }}
          }}
        }}
        '''

        url = f"https://{shop_name}.myshopify.com/admin/api/2024-07/graphql.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": admin_api_token
        }

        # Perform the API request
        response = requests.post(url, json={"query": query}, headers=headers)

        # Debugging: Print response status and content
        # print(f"Response Status Code: {response.status_code}")
        # print(f"Response Content: {response.text}")

        # Process the response
        response_data = response.json()

        if 'data' not in response_data:
            raise Exception("No data returned from Shopify.")

        payouts = response_data['data']['shopifyPaymentsAccount']['payouts']['edges']
        all_payouts.extend(payouts)

        # Pagination control
        page_info = response_data['data']['shopifyPaymentsAccount']['payouts']['pageInfo']
        has_next_page = page_info['hasNextPage']
        after_cursor = page_info['endCursor']

    # Filter payouts by date range
    filtered_payouts = []
    for payout in all_payouts:
        payout_date = datetime.strptime(
            payout['node']['issuedAt'], "%Y-%m-%dT%H:%M:%S%z").date()
        if datetime.strptime(start_date, "%Y-%m-%d").date() <= payout_date <= datetime.strptime(end_date, "%Y-%m-%d").date():
            filtered_payouts.append(payout)

    if not filtered_payouts:
        print(f"No payouts found between {start_date} and {end_date}.")

    return filtered_payouts


def fetch_balance_transactions(payout_id):
    """
    Fetches balance transactions and filters by the specific payout.
    """
    transactions = []
    has_next_page = True
    after_cursor = None

    while has_next_page:
        # Modify the query only to include 'after' if a valid cursor exists
        after_clause = f', after: "{after_cursor}"' if after_cursor else ""
        query = f'''
        {{
          shopifyPaymentsAccount {{
            balanceTransactions(first: 100{after_clause}) {{
              edges {{
                node {{
                  transactionDate
                  type
                  associatedOrder {{
                    name
                  }}
                  amount {{
                    amount
                  }}
                  fee {{
                    amount
                  }}
                  net {{
                    amount
                  }}
                  associatedPayout {{
                    id
                  }}
                }}
                cursor
              }}
              pageInfo {{
                hasNextPage
                endCursor
              }}
            }}
          }}
        }}
        '''
        # Query Shopify's GraphQL API
        url = f"https://{shop_name}.myshopify.com/admin/api/2024-07/graphql.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": admin_api_token
        }

        response = requests.post(url, json={"query": query}, headers=headers)
        response_data = response.json()

        # Debugging: Print the response content
        # print("Response Content:", response_data)

        if 'data' not in response_data or not response_data['data']['shopifyPaymentsAccount']:
            raise Exception(
                f"No transaction data returned for payout {payout_id}. Response: {response_data}")

        balance_transactions = response_data['data']['shopifyPaymentsAccount']['balanceTransactions'].get(
            'edges', [])

        for transaction in balance_transactions:
            if transaction['node']['associatedPayout'] and transaction['node']['associatedPayout']['id'] == payout_id:
                transactions.append(transaction['node'])

        # Check if there's more to fetch
        page_info = response_data['data']['shopifyPaymentsAccount']['balanceTransactions'].get(
            'pageInfo', {})
        has_next_page = page_info.get('hasNextPage', False)
        after_cursor = page_info.get('endCursor', None)

    return transactions


def create_excel(payouts, output_file):
    workbook = Workbook()

    # Sheet 1: Payout Overview
    sheet1 = workbook.active
    sheet1.title = "Payout Overview"

    headers = ["Payout Date", "Payout ID", "Status", "Charges", "Refunds", "Adjustments",
               "Reserved Funds", "Fees", "Retired Amount", "Total", "Currency"]
    sheet1.append(headers)

    for payout in payouts:
        node = payout['node']
        summary = node['summary']
        payout_id = node['id'][-10:]

        # Create hyperlink for the payout date column
        payout_date = datetime.strptime(
            node["issuedAt"], "%Y-%m-%dT%H:%M:%S%z").strftime("%m/%d/%Y")
        hyperlink = f'=HYPERLINK("#\'Payout {payout_id}\'!A1", "{payout_date}")'

        # Append the row with the hyperlink
        row = [
            hyperlink,
            payout_id,
            node['status'],
            float(summary['chargesGross']['amount']
                  ) if summary['chargesGross'] else 0.00,
            float(summary['refundsFeeGross']['amount']
                  ) if summary['refundsFeeGross'] else 0.00,
            float(summary['adjustmentsGross']['amount']
                  ) if summary['adjustmentsGross'] else 0.00,
            float(summary['reservedFundsGross']['amount']
                  ) if summary['reservedFundsGross'] else 0.00,
            float(summary['chargesFee']['amount']
                  ) if summary['chargesFee'] else 0.00,
            float(summary['retriedPayoutsGross']['amount']
                  ) if summary['retriedPayoutsGross'] else 0.00,
            float(node['net']['amount']),
            node['net']['currencyCode']
        ]
        sheet1.append(row)

        # Style the hyperlink as blue and underlined
        cell = sheet1.cell(row=sheet1.max_row, column=1)
        cell.font = Font(color="0000FF", underline="single")

    # Format all columns with numbers as numbers
    for row in sheet1.iter_rows(min_row=2, min_col=3, max_col=11):
        for cell in row:
            cell.number_format = '#,##0.00'

    for payout in payouts:
        node = payout['node']
        summary = node['summary']
        payout_id = node['id'][-10:]

        transactions = fetch_balance_transactions(node['id'])

        clean_title = f"Payout {payout_id}"
        payout_sheet = workbook.create_sheet(title=clean_title)

        # Add payout summary info at the top
        payout_sheet.append(["Payout Date:", datetime.strptime(
            node['issuedAt'], "%Y-%m-%dT%H:%M:%S%z").strftime("%m/%d/%Y")])
        payout_sheet.append(["Payout ID:", payout_id])
        payout_sheet.append(
            ["Total:", f"{node['net']['amount']} {node['net']['currencyCode']}"])
        payout_sheet.append([])  # Blank row

        # Charges, Refunds, Adjustments with total calculation
        payout_sheet.append(["", "Gross", "Fees", "Total"])
        payout_sheet.append(["Charges", summary['chargesGross']['amount'],
                             summary['chargesFee']['amount'], float(summary['chargesGross']['amount']) - float(summary['chargesFee']['amount'])])
        payout_sheet.append(["Refunds", summary['refundsFeeGross']
                            ['amount'], "0.00", summary['refundsFeeGross']['amount']])
        payout_sheet.append(["Adjustments", summary['adjustmentsGross']
                            ['amount'], "0.00", summary['adjustmentsGross']['amount']])
        payout_sheet.append([])  # Blank row

        # Add transaction details
        payout_sheet.append(["Transaction Date", "Type",
                            "Order", "Amount", "Fee", "Net", "Currency"])
        for transaction in transactions:
            transaction_row = [
                datetime.strptime(
                    transaction['transactionDate'], "%Y-%m-%dT%H:%M:%S%z").strftime("%m/%d/%Y"),
                transaction['type'],
                transaction['associatedOrder']['name'] if transaction['associatedOrder'] else "N/A",
                float(transaction['amount']['amount']
                      ) if transaction['amount'] else 0.00,
                float(transaction['fee']['amount']
                      ) if transaction['fee'] else 0.00,
                float(transaction['net']['amount']
                      ) if transaction['net'] else 0.00,
                node['net']['currencyCode']
            ]
            payout_sheet.append(transaction_row)

        # Format all number fields as numbers
        for row in payout_sheet.iter_rows(min_row=5, min_col=2, max_col=4):
            for cell in row:
                cell.number_format = '#,##0.00'

        for row in payout_sheet.iter_rows(min_row=9, min_col=4, max_col=6):
            for cell in row:
                cell.number_format = '#,##0.00'

    # Ensure export path exists
    if not os.path.exists(export_path):
        os.makedirs(export_path)

    # Save the Excel file with a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_with_timestamp = os.path.join(
        export_path, f"Shopify_Payouts_{start_date}_to_{end_date}_{timestamp}.xlsx")
    workbook.save(output_file_with_timestamp)


if __name__ == "__main__":
    payouts = fetch_payouts()
    output_file = f"Shopify_Payouts_{start_date}_to_{end_date}.xlsx"
    create_excel(payouts, output_file)
    print(f"Payout data exported to {output_file}")
