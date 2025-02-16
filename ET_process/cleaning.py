import pandas as pd

def check_and_clean_csv(input_file="tmp/messy_interaction_data.csv", output_file="tmp/cleaned_interaction_data.csv"):
    # Read the CSV file
    df = pd.read_csv(input_file)

    print("🔍 Checking for messy data...\n")

    # **1. Check for Missing Values**
    print("1️⃣ Checking for missing values:")
    print(df.isnull().sum(), "\n")  # Count missing values in each column

    # **2. Check Data Types**
    print("2️⃣ Checking data types before cleaning:")
    print(df.dtypes, "\n")

    # **3. Check if 'timestamp' has invalid formats**
    print("3️⃣ Checking timestamp format issues:")
    invalid_timestamps = df[~df['timestamp'].astype(str).str.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', na=True)]
    print(invalid_timestamps[['timestamp']], "\n")

    print("✅ Cleaning the data...\n")

    # **Cleaning Steps**
    df['user_id'].fillna(-1, inplace=True)  # Replace missing user_id with -1
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')  # Convert product_id to numeric
    df['product_id'].fillna(-1, inplace=True)  # Replace NaN product_id with -1
    df.dropna(subset=['action'], inplace=True)  # Drop rows where 'action' is missing

    # Convert 'timestamp' to datetime and handle errors
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')  
    df.dropna(subset=['timestamp'], inplace=True)  # Drop invalid timestamps

    print("🎯 Final Data Types After Cleaning:")
    print(df.dtypes, "\n")

    # Save cleaned data
    df.to_csv(output_file, index=False)
    print(f"✅ Cleaned CSV file '{output_file}' created successfully!")

# Run function
check_and_clean_csv()
