CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country VARCHAR(2),      -- e.g., 'US', 'IN', 'JP'
    device_os VARCHAR(10),   -- e.g., 'iOS', 'Android', 'Web'
    segment VARCHAR(20),     -- e.g., 'High_Value', 'New', 'Churned'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create an index on country to speed up analytical queries later
CREATE INDEX idx_country ON users(country);