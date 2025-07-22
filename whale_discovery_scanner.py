#!/usr/bin/env python3
print("🚀 MINIMAL WHALE SCANNER STARTING...")

import os
print("✅ OS imported")

import requests
print("✅ Requests imported")

import psycopg as psycopg2
print("✅ Psycopg imported")

# Test environment variables
db_url = os.getenv('DB_URL')
print(f"📊 DB_URL exists: {bool(db_url)}")
if db_url:
    print(f"📊 DB_URL first 30 chars: {db_url[:30]}")

# Test database connection
try:
    if db_url:
        conn = psycopg2.connect(db_url)
        print("✅ Database connection successful!")
        conn.close()
    else:
        print("❌ No DB_URL found")
except Exception as e:
    print(f"❌ Database connection failed: {e}")

print("🎉 MINIMAL TEST COMPLETE!")

# Keep service running
import time
print("💤 Keeping service alive...")
while True:
    time.sleep(60)
    print("💤 Still running...")
