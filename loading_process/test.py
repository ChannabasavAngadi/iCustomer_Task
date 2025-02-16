import sqlite3
import pandas as pd

# Database connection (reuse existing database)
DB_NAME = "loading_process\interactions.db"
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

# Step 3: Queries for Data Retrieval
queries = {
    "Total interactions per day": """
        SELECT DATE(timestamp) AS interaction_date, COUNT(*) AS total_interactions
        FROM user_interactions
        GROUP BY interaction_date
        ORDER BY interaction_date;
    """,
    
    "Top 5 users by interactions": """
        SELECT user_id, SUM(user_interaction_count) AS total_interactions
        FROM user_interactions
        WHERE user_id != -1
        GROUP BY user_id
        ORDER BY total_interactions DESC
        LIMIT 5;
    """,
    
    "Most interacted products": """
        SELECT product_id, SUM(product_interaction_count) AS total_interactions
        FROM user_interactions
        WHERE product_id != -1
        GROUP BY product_id
        ORDER BY total_interactions DESC
        LIMIT 5;
    """
}

# Execute and display query results
for desc, query in queries.items():
    print(f"\n{desc}")
    result = cursor.execute(query).fetchall()
    for row in result:
        print(row)

# Close connection
conn.close()
