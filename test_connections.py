"""
test_connections.py

Purpose: confirms that MinIO, PostgreSQL and MongoDB
are all running and reachable from Python.

Run this script whenever you restart your Docker stack
to verify everything is healthy before starting work.
"""

import os
from dotenv import load_dotenv
from minio import Minio
import psycopg
from pymongo import MongoClient

# ── Load environment variables ──────────────────────────
load_dotenv()


# ── Test 1: MinIO ────────────────────────────────────────
def test_minio():
    print("\n--- Testing MinIO connection ---")

    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    buckets = client.list_buckets()
    print(f"Connected to MinIO successfully!")
    print(f"Existing buckets: {[b.name for b in buckets]}")
    return True


# ── Test 2: PostgreSQL ───────────────────────────────────
def test_postgres():
    print("\n--- Testing PostgreSQL connection ---")

    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"Connected to PostgreSQL successfully!")
    print(f"Version: {version[0]}")

    cursor.close()
    conn.close()
    return True


# ── Test 3: MongoDB ──────────────────────────────────────
def test_mongodb():
    print("\n--- Testing MongoDB connection ---")

    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB")]

    result = client.admin.command("ping")
    print(f"Connected to MongoDB successfully!")
    print(f"Ping response: {result}")

    client.close()
    return True


# ── Run all tests ────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Financial Data Lakehouse — Connection Tests")
    print("=" * 50)

    results = {}

    try:
        results["MinIO"] = test_minio()
    except Exception as e:
        results["MinIO"] = False
        print(f"MinIO FAILED: {e}")

    try:
        results["PostgreSQL"] = test_postgres()
    except Exception as e:
        results["PostgreSQL"] = False
        print(f"PostgreSQL FAILED: {e}")

    try:
        results["MongoDB"] = test_mongodb()
    except Exception as e:
        results["MongoDB"] = False
        print(f"MongoDB FAILED: {e}")

    print("\n" + "=" * 50)
    print("  Results Summary")
    print("=" * 50)
    for service, status in results.items():
        icon = "✓" if status else "✗"
        print(f"  {icon}  {service}: {'PASSED' if status else 'FAILED'}")

    print("=" * 50)

    all_passed = all(results.values())
    if all_passed:
        print("\n  All connections healthy. Ready to build!")
    else:
        print("\n  Some connections failed. Check Docker stack.")
