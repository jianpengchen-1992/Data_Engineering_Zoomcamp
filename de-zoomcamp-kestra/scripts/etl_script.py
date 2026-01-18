import pandas as pd
import sys

# Kestra injects variables via template logic or we pass them as args
# We will simulate reading a CSV and printing stats

def process_data(input_file):
    print(f"Reading file: {input_file}")
    
    # Simulate reading data
    # In real life: df = pd.read_csv(input_file)
    df = pd.DataFrame({
        'VendorID': [1, 2, 1, 2],
        'total_amount': [10.5, 20.0, 15.0, 40.0],
        'passenger_count': [1, 1, 2, 3]
    })
    
    print("Data Schema:")
    print(df.info())
    
    print(f"Total Rows: {len(df)}")
    print(f"Average Amount: {df['total_amount'].mean()}")
    
    # Data Cleaning Logic (Example)
    df = df[df['passenger_count'] > 0]
    print("Cleaned data - dropped empty passenger trips")
    
    # Save to a processed file
    output_file = "processed_data.csv"
    df.to_csv(output_file, index=False)
    print(f"Saved processed data to {output_file}")

if __name__ == "__main__":
    # We expect the filename as the first argument
    if len(sys.argv) > 1:
        process_data(sys.argv[1])
    else:
        print("No input file provided")