import psycopg2
import uuid
import json
import random
from faker import Faker
from psycopg2.extras import execute_values

# Configuration matches your docker-compose.yml
DB_CONFIG = {
    "dbname": "omnistream_db",
    "user": "admin",
    "password": "password123",
    "host": "localhost",
    "port": "5432",
}

NUM_USERS = 10000
OUTPUT_FILE = "user_ids.json"  # Save in root for the other script to find

fake = Faker()


def generate_users(n):
    """Generates a list of tuples for batch insertion."""
    users = []
    user_ids = []

    print(f"Generating {n} fake users...")

    countries = ["US", "IN", "UK", "CA", "JP", "DE", "BR"]
    os_types = ["iOS", "Android", "Web"]
    segments = ["High_Value", "Casual", "New_User", "Churn_Risk"]

    for _ in range(n):
        uid = str(uuid.uuid4())
        profile = (
            uid,
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            random.choice(countries),
            random.choice(os_types),
            random.choice(segments),
        )
        users.append(profile)
        user_ids.append(uid)

    return users, user_ids


def seed_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        users_data, user_ids = generate_users(NUM_USERS)

        # Fast Batch Insert
        insert_query = """
            INSERT INTO users (user_id, first_name, last_name, email, country, device_os, segment)
            VALUES %s
        """

        print("Inserting into Postgres...")
        execute_values(cur, insert_query, users_data)

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Database Seeded Successfully.")

        # Save IDs for the Traffic Generator
        with open(OUTPUT_FILE, "w") as f:
            json.dump(user_ids, f)
        print(f"✅ User IDs saved to {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    seed_database()
