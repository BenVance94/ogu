import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path

def setup_directories():
    """Create necessary directories if they don't exist"""
    base_dir = Path('orders')
    directories = ['raw', 'clean', 'results']
    for directory in directories:
        (base_dir / directory).mkdir(parents=True, exist_ok=True)
    return True

def clean_fidelity_csv(file_path):
    """Clean and process Fidelity CSV file"""
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Clean up column names and remove leading/trailing whitespace
    df.columns = df.columns.str.strip()
    
    # Extract only needed columns
    needed_columns = ['Settlement Date', 'Symbol', 'Description', 'Quantity', 'Price', 'Action']
    
    if not all(col in df.columns for col in needed_columns):
        print("\nAvailable columns:", df.columns.tolist())
        raise ValueError(f"Missing required columns in file: {file_path}")
    
    # Create cleaned dataframe with only needed columns
    cleaned_df = pd.DataFrame()
    
    # Process each column
    cleaned_df['Settlement Date'] = pd.to_datetime(df['Settlement Date'])
    cleaned_df['Symbol'] = df['Symbol'].str.strip()
    cleaned_df['Stock Name'] = df['Description'].str.strip()
    cleaned_df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
    cleaned_df['Price'] = pd.to_numeric(df['Price'], errors='coerce').round(2)
    cleaned_df['Total Amount'] = (cleaned_df['Quantity'] * cleaned_df['Price']).round(2)
    
    # Clean up Action column and simplify to BUY/SELL only
    cleaned_df['Transaction Type'] = df['Action'].apply(
        lambda x: 'SELL' if 'SOLD' in str(x) else 'BUY' if 'BOUGHT' in str(x) else None
    )
    
    # Remove any rows where Transaction Type is None (neither BUY nor SELL)
    cleaned_df = cleaned_df.dropna(subset=['Transaction Type'])
    
    # Sort by date
    cleaned_df = cleaned_df.sort_values('Settlement Date', ascending=False)
    
    return cleaned_df

def save_transactions_summary(df, input_filename):
    """Save cleaned transactions to CSV files"""
    # Get base filename without extension
    base_filename = Path(input_filename).stem
    
    # Save cleaned data
    clean_file = Path('orders') / 'clean' / f'clean_{base_filename}.csv'
    df.to_csv(clean_file, index=False, float_format='%.2f')  # Format floats to 2 decimal places
    print(f"\nCleaned data saved to: {clean_file}")
    
    # Format the output for results
    output_df = df[[
        'Settlement Date', 
        'Symbol', 
        'Stock Name', 
        'Transaction Type',
        'Quantity',
        'Price',
        'Total Amount'
    ]]
    
    # Format currency columns with 2 decimal places
    output_df['Price'] = output_df['Price'].apply(lambda x: f"${x:.2f}")
    output_df['Total Amount'] = output_df['Total Amount'].apply(lambda x: f"${x:.2f}")
    
    # Save formatted results
    results_file = Path('orders') / 'results' / f'summary_{base_filename}.csv'
    output_df.to_csv(results_file, index=False)
    print(f"Summary saved to: {results_file}")
    
    # Display first few rows
    print("\nMost recent transactions:")
    print(output_df.head().to_string())
    
    return clean_file, results_file

def process_raw_files():
    """Process all CSV files in the raw directory"""
    raw_dir = Path('orders') / 'raw'
    processed_files = []
    
    # Check if raw directory exists and has files
    if not raw_dir.exists():
        raise ValueError("'orders/raw' directory not found. Please create it and add your CSV files.")
    
    csv_files = list(raw_dir.glob('*.csv'))
    if not csv_files:
        raise ValueError("No CSV files found in 'orders/raw' directory.")
    
    # Process each CSV file
    for file_path in csv_files:
        try:
            print(f"\nProcessing {file_path}...")
            df = clean_fidelity_csv(file_path)
            clean_file, results_file = save_transactions_summary(df, file_path.name)
            
            # Print basic statistics
            print("\n=== Transaction Statistics ===")
            print(f"Total number of transactions: {len(df)}")
            print(f"Number of unique stocks: {len(df['Symbol'].unique())}")
            
            # Transaction count by type
            type_counts = df['Transaction Type'].value_counts()
            print("\nTransactions by type:")
            for type_name, count in type_counts.items():
                print(f"{type_name}: {count}")
            
            processed_files.append({
                'input': str(file_path),
                'clean': clean_file,
                'results': results_file,
                'transactions': len(df)
            })
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            continue
    
    return processed_files

def main():
    try:
        # Setup directory structure
        setup_directories()
        
        # Process all files in raw directory
        print("Starting portfolio data processing...")
        processed_files = process_raw_files()
        
        # Print summary of processing
        print("\n=== Processing Summary ===")
        for file_info in processed_files:
            print(f"\nInput file: {file_info['input']}")
            print(f"Clean data: {file_info['clean']}")
            print(f"Summary: {file_info['results']}")
            print(f"Transactions processed: {file_info['transactions']}")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("Please ensure your CSV files are in the 'orders/raw' directory and try again.")

if __name__ == "__main__":
    main() 