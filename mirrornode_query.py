import argparse
import requests
import json
import csv
import pandas as pd
import sys
from datetime import datetime
from pandas import json_normalize
from urllib.parse import urljoin


# Setup command line argument parsing
parser = argparse.ArgumentParser(description='Fetch and filter transactions for a specific Hedera account.')
parser.add_argument('account_id', type=str, help='The ID of the account in format 0.0.x')
parser.add_argument('--filter_account', type=str, help='The ID of the account to filter for in format 0.0.x')
parser.add_argument('--match_string', type=str, help='String to match in CSV file')
parser.add_argument('--max_limit', type=int, default=100, help='Maximum number of records to fetch')

args = parser.parse_args()


# If match_string is not provided, use filter_account value
if args.match_string is None:
    args.match_string = args.filter_account

base_url = 'https://mainnet-public.mirrornode.hedera.com/api/v1/accounts/'
url = base_url + args.account_id

max_limit = args.max_limit
params = {
    'limit': min(100, max_limit) if max_limit else 100,
    'order': 'desc',
    'transactiontype': 'cryptotransfer'
}

# Get the initial data
response = requests.get(url, params=params)

# List to hold all fetched data
all_data = []

# While we haven't fetched all requested records
while (max_limit is None or len(all_data) < max_limit) and response.status_code == 200:
    data = response.json()
    filtered_data = []

    # If 'data' is a dictionary containing a key 'transactions' that holds a list of dictionaries
    if 'transactions' in data:
        for record in data['transactions']:
            # Check if specific_account is in any of the transfers, if filter_account was provided
            transfers = record.get('transfers', [])
            if args.filter_account is None or any(transfer.get('account', None) == args.filter_account for transfer in transfers):
                # Format transfer amounts
                for transfer in transfers:
                    amount = transfer.get('amount', 0) / 10**8
                    transfer['amount'] = "{:,.8f}".format(amount)
                # Create a new dictionary with only the desired fields
                timestamp = float(record.get('consensus_timestamp', 0))
                readable_timestamp = datetime.fromtimestamp(timestamp).isoformat()
                filtered_record = {
                    'consensus_timestamp': readable_timestamp,
                    'transaction_id': record.get('transaction_id', None),
                    'transfers': transfers
                }
                filtered_data.append(filtered_record)

    # Add the filtered data to all_data
    all_data.extend(filtered_data)

    # Check if we have a 'next' link
    if 'links' in data and 'next' in data['links']:
        next_url = urljoin(base_url, data['links']['next'])

        # Calculate how many more records we need to fetch
        remaining_records = max_limit - len(all_data) if max_limit else None
        params['limit'] = min(100, remaining_records) if remaining_records else 100

        # Fetch the next chunk of data
        response = requests.get(next_url, params=params)
    else:
        # If there's no 'next' link, we're done fetching data
        break

# Normalize JSON data
df = json_normalize(all_data, record_path='transfers', meta=['consensus_timestamp', 'transaction_id'])

# Reorder columns
cols = df.columns.tolist()
cols = cols[-2:] + cols[:-2]
df = df[cols]

# Save DataFrame to CSV
csv_file = args.account_id + '.csv'
df.to_csv(csv_file, index=False)

print(f'Unfiltered CSV file has been saved as {csv_file}')

# Filter csv based on match_string if provided
if args.match_string:
    output_file = "filtered_" + csv_file
    with open(csv_file, 'r') as csv_in, open(output_file, 'w', newline='') as csv_out:
        reader = csv.reader(csv_in)
        writer = csv.writer(csv_out)

        # Copy header row
        header = next(reader)
        writer.writerow(header)

        # Write rows where a cell matches the string exactly
        for row in reader:
            if any(cell == args.match_string for cell in row):
                writer.writerow(row)

    print(f'Filtered CSV file has been saved as {output_file}')
