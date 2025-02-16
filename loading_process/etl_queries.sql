-- Schema creation
CREATE TABLE IF NOT EXISTS interactions (
    interaction_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    product_id INTEGER,
    action TEXT,
    timestamp TEXT,
    user_interaction_count INTEGER,
    product_interaction_count INTEGER ,
    interaction_count INTEGER
);

-- Data retrieval queries
-- 1. Total interactions per day
SELECT DATE(timestamp) AS interaction_date, COUNT(*) AS total_interactions
FROM interactions
GROUP BY interaction_date
ORDER BY interaction_date;

-- 2. Top 5 users by the number of interactions
SELECT user_id, SUM(user_interaction_count) AS total_interactions
FROM interactions
WHERE user_id != -1
GROUP BY user_id
ORDER BY total_interactions DESC
LIMIT 5;

-- 3. Most interacted products
SELECT product_id, SUM(product_interaction_count) AS total_interactions
FROM interactions
WHERE product_id != -1
GROUP BY product_id
ORDER BY total_interactions DESC
LIMIT 5;
