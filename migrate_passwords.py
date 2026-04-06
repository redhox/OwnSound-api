import json
import os
from passlib.context import CryptContext

# Configuration
DB_PATH = "database.json"
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    with open(DB_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    users = data.get("users", {})
    count = 0
    for user_id, user in users.items():
        password = user.get("password")
        if not password:
            continue
        
        # Check if already hashed (bcrypt hashes start with $2b$ or $2a$)
        if password.startswith("$2b$") or password.startswith("$2a$"):
            continue
            
        # Hash it
        user["password"] = pwd_context.hash(password)
        count += 1
        print(f"Migrated password for user: {user.get('username')}")

    if count > 0:
        with open(DB_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Successfully migrated {count} passwords.")
    else:
        print("No passwords needed migration.")

if __name__ == "__main__":
    migrate()
