import sqlite3
import pandas as pd

def load_to_sqlite(db_name="loading_process/interactions.db", input_file="tmp/transformed_interaction_data.csv"):
    # Read the transformed data
    df = pd.read_csv(input_file)

    print("📂 Connecting to SQLite database...\n")
    
    # Connect to SQLite (creates a new file if it doesn’t exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create a table for user interactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_interactions (
            interaction_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            action TEXT,
            timestamp TEXT,
            user_interaction_count INTEGER,
            product_interaction_count INTEGER ,
            interaction_count INTEGER       )
    """)

    print("✅ Table created successfully!\n")

    # Insert DataFrame into SQLite table
    df.to_sql("user_interactions", conn, if_exists="replace", index=False)

    print("🚀 Data loaded successfully into SQLite!\n")

    cursor.execute("SELECT COUNT(*) FROM user_interactions")
    print(f"📊 Total rows in 'user_interactions' table: {cursor.fetchone()[0]}")
    

    # Commit and close connection
    conn.commit()
    conn.close()
    print(f"📊 SQLite Database '{db_name}' updated with transformed data!")

# Run function
load_to_sqlite()
