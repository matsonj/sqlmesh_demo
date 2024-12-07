import dlt
import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Iterator, Dict, Any
import os
#from dlt.destinations.motherduck import motherduck_config

def read_symbols() -> Iterator[str]:
    """Generator that reads and yields stock symbols from a file."""
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, '..', 'scripts', 'symbols.txt')
    try:
        with open(file_path, 'r') as file:
            for symbol in file:
                symbol = symbol.strip()
                if symbol:  # Skip empty lines
                    yield symbol
    except FileNotFoundError:
        print(f"File {file_path} not found.")

@dlt.resource(
    primary_key="Symbol",
    write_disposition="replace"
)
def stock_info_resource() -> Iterator[Dict[str, Any]]:
    """Resource that fetches stock information for each symbol."""
    for symbol in read_symbols():
        try:
            stock = yf.Ticker(symbol)
            # Validate symbol by checking if we can get history
            if not stock.history(period='1d').empty:
                info = stock.info
                info['Symbol'] = symbol  # Add symbol column
                yield info
            else:
                print(f"Invalid symbol: {symbol}")
        except Exception as e:
            print(f"Error processing symbol {symbol}: {e}")

def load_stock_info_pipeline():
    """Creates and runs the stock info pipeline."""
    # Initialize the pipeline with MotherDuck destination
    pipeline = dlt.pipeline(
        pipeline_name='stock_info',
        destination='motherduck',
        dataset_name='stock_data',
        # credentials=motherduck_config(
        #     service_token=os.getenv('MOTHERDUCK_TOKEN')
        # )
    )

    # Load data using the stock info resource
    load_info = pipeline.run(stock_info_resource())
    
    # Print outcome
    print(load_info)

if __name__ == "__main__":
    load_stock_info_pipeline()
