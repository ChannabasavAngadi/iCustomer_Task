import pandas as pd

def transform_data(input_file="tmp/cleaned_interaction_data.csv", output_file="tmp/transformed_interaction_data.csv"):
    # Read the cleaned CSV file
    df = pd.read_csv(input_file)

    print("📊 Transforming Data...\n")

    # **1. Calculate Interactions Per User**
    user_interactions = df.groupby('user_id')['interaction_id'].count().reset_index()
    user_interactions.columns = ['user_id', 'user_interaction_count']

    # **2. Calculate Interactions Per Product**
    product_interactions = df.groupby('product_id')['interaction_id'].count().reset_index()
    product_interactions.columns = ['product_id', 'product_interaction_count']

    # **3. Merge Counts into Main DataFrame**
    df = df.merge(user_interactions, on='user_id', how='left')
    df = df.merge(product_interactions, on='product_id', how='left')

    # **4. Create a Single Interaction Count Column**
    df['interaction_count'] = df['user_interaction_count'] + df['product_interaction_count']


    print("✅ Data Transformation Completed!\n")

    # Save the transformed data
    df.to_csv(output_file, index=False)
    print(f"📂 Transformed data saved to '{output_file}'")

# Run function
transform_data()
