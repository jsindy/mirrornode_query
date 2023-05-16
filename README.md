# Hedera Account Transactions Fetcher and Filter Script

This script fetches and filters transactions of a specific Hedera account using the Hedera Mirror Node API. It then normalizes the JSON data, transforms it into a CSV format, and filters the data based on a provided string.

## Features

- Fetches transactions from Hedera Mirror Node API
- Filters transactions based on provided account ID
- Converts JSON data into a CSV format
- Filters CSV data based on a provided string
- Supports fetching data in chunks and a maximum limit of fetched records

## Requirements

Python 3.6 or later is required to run this script. Required Python packages include:

- requests
- pandas
- argparse

These can be installed by running `pip install -r requirements.txt`.

## Usage

To run the script, use the following command:

```
python mirrornode_query.py <account_id> [--filter_account FILTER_ACCOUNT] [--max_limit MAX_LIMIT]
```

## Output
The script creates a CSV file with the following columns:

- consensus_timestamp
- transaction_id
- account
- amount


