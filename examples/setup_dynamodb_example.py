#!/usr/bin/env python3
"""Example script to create a DynamoDB table and populate it with sample data.

This script demonstrates how to:
1. Create a DynamoDB table compatible with the batch processing system
2. Populate it with sample news/market data
3. Verify the data was inserted correctly

Usage:
    python examples/setup_dynamodb_example.py --table-name my-test-table --region us-east-1
"""

import argparse
import boto3
from datetime import datetime, timezone, timedelta
import random
from typing import Dict, Any, List


def create_table(table_name: str, region: str = "us-east-1") -> None:
    """Create a DynamoDB table with the appropriate schema."""
    dynamodb = boto3.resource("dynamodb", region_name=region)

    print(f"Creating DynamoDB table: {table_name}")

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},  # Partition key
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "N"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "timestamp-index",
                    "KeySchema": [
                        {"AttributeName": "timestamp", "KeyType": "HASH"}
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",  # On-demand billing
        )

        # Wait for table to be created
        print("Waiting for table to be created...")
        table.wait_until_exists()
        print(f"✓ Table {table_name} created successfully!")

    except dynamodb.meta.client.exceptions.ResourceInUseException:
        print(f"Table {table_name} already exists. Skipping creation.")


def generate_sample_news_items(count: int = 10) -> List[Dict[str, Any]]:
    """Generate sample news/market data items."""
    sample_headlines = [
        "Fed announces interest rate decision",
        "Tech stocks surge on earnings beat",
        "Oil prices drop amid supply concerns",
        "Dollar strengthens against major currencies",
        "Unemployment rate hits new low",
        "Housing market shows signs of cooling",
        "Inflation data comes in higher than expected",
        "Central bank signals policy shift",
        "GDP growth exceeds forecasts",
        "Market volatility increases on geopolitical tensions",
        "Consumer confidence index reaches 5-year high",
        "Bond yields climb to decade highs",
        "Retail sales disappoint in latest report",
        "Manufacturing PMI shows expansion",
        "Trade deficit widens in latest quarter",
    ]

    items = []
    now = datetime.now(timezone.utc)

    for i in range(count):
        # Create items spread across the last 24 hours
        item_time = now - timedelta(hours=random.uniform(0, 24))

        item = {
            "id": f"news-{i+1:04d}",
            "timestamp": int(item_time.timestamp()),
            "summary": random.choice(sample_headlines),
            "source": random.choice(["Reuters", "Bloomberg", "WSJ", "FT", "CNBC"]),
            "category": random.choice(
                ["markets", "economy", "policy", "commodities", "forex"]
            ),
            "sentiment": random.choice(["positive", "negative", "neutral"]),
            "importance": random.randint(1, 5),
        }
        items.append(item)

    return items


def generate_sample_market_data(count: int = 10) -> List[Dict[str, Any]]:
    """Generate sample market data items."""
    symbols = ["SPY", "QQQ", "IWM", "GLD", "TLT", "UUP", "USO", "DIA", "EEM", "VTI"]

    items = []
    now = datetime.now(timezone.utc)

    for i, symbol in enumerate(symbols[:count]):
        item_time = now - timedelta(hours=random.uniform(0, 6))
        base_price = random.uniform(50, 500)

        item = {
            "id": f"market-{symbol}-{i+1:04d}",
            "timestamp": int(item_time.timestamp()),
            "symbol": symbol,
            "lastprice": round(base_price, 2),
            "changepct": round(random.uniform(-5, 5), 2),
            "highprice": round(base_price * 1.02, 2),
            "lowprice": round(base_price * 0.98, 2),
            "volume": random.randint(1000000, 100000000),
            "rsi14": round(random.uniform(30, 70), 2),
            "sma20": round(base_price * random.uniform(0.95, 1.05), 2),
        }
        items.append(item)

    return items


def populate_table(
    table_name: str, items: List[Dict[str, Any]], region: str = "us-east-1"
) -> None:
    """Populate the DynamoDB table with sample items."""
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    print(f"\nInserting {len(items)} items into {table_name}...")

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    print(f"✓ Successfully inserted {len(items)} items!")


def verify_table_data(table_name: str, region: str = "us-east-1") -> None:
    """Verify the data in the table."""
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    print(f"\nVerifying data in {table_name}...")

    # Scan table to count items
    response = table.scan(Select="COUNT")
    count = response["Count"]

    print(f"✓ Table contains {count} items")

    # Show a sample item
    response = table.scan(Limit=1)
    if response["Items"]:
        print("\nSample item:")
        for key, value in response["Items"][0].items():
            print(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(
        description="Set up a DynamoDB table with sample data for batch processing"
    )
    parser.add_argument(
        "--table-name",
        default="batch-test-table",
        help="Name of the DynamoDB table to create (default: batch-test-table)",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)",
    )
    parser.add_argument(
        "--data-type",
        choices=["news", "market", "both"],
        default="both",
        help="Type of sample data to generate (default: both)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of items to create per data type (default: 10)",
    )
    parser.add_argument(
        "--skip-create",
        action="store_true",
        help="Skip table creation (only populate data)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("DynamoDB Batch Processing Example Setup")
    print("=" * 60)

    # Create table
    if not args.skip_create:
        create_table(args.table_name, args.region)

    # Generate and insert data
    all_items = []

    if args.data_type in ["news", "both"]:
        news_items = generate_sample_news_items(args.count)
        all_items.extend(news_items)
        print(f"\n✓ Generated {len(news_items)} news items")

    if args.data_type in ["market", "both"]:
        market_items = generate_sample_market_data(args.count)
        all_items.extend(market_items)
        print(f"✓ Generated {len(market_items)} market data items")

    if all_items:
        populate_table(args.table_name, all_items, args.region)
        verify_table_data(args.table_name, args.region)

    print("\n" + "=" * 60)
    print("Setup complete! You can now run the batch processing system:")
    print(f"\n  python -m batch.main --table {args.table_name} --hours 24 --test")
    print("\nTo run a full batch (not a test):")
    print(f"  python -m batch.main --table {args.table_name} --hours 24")
    print("=" * 60)


if __name__ == "__main__":
    main()
