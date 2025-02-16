import csv
import random
from datetime import datetime, timedelta

def create_messy_csv(filename="tmp/messy_interaction_data.csv", num_rows=100):
    header = ['interaction_id', 'user_id', 'product_id', 'action', 'timestamp']
    actions = ['view', 'click', 'purchase', None]  # Some actions will be missing

    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)

        for i in range(1, num_rows + 1):
            user_id = random.choice([random.randint(1, 100), None])  # Some user_id values missing
            product_id = random.choice([random.randint(1, 20), "unknown"])  # Some product_id as string
            action = random.choice(actions)  
            timestamp = random.choice([
                (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S'),  
                "invalid_date",  # Incorrect timestamp format
                None  # Missing timestamp
            ])
            
            writer.writerow([i, user_id, product_id, action, timestamp])

    print(f"Messy CSV file '{filename}' created.")

# Run function
create_messy_csv()
