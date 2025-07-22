#!/usr/bin/env python3

# Force output flushing
import sys
import os
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("🔍 DEBUG: Script starting...", flush=True)

try:
    print("🔍 DEBUG: About to import os", flush=True)
    import os
    print("✅ OS imported successfully", flush=True)
    
    print("🔍 DEBUG: About to import requests", flush=True)
    import requests
    print("✅ Requests imported successfully", flush=True)
    
    print("🔍 DEBUG: About to import psycopg", flush=True)
    import psycopg as psycopg2
    print("✅ Psycopg imported successfully", flush=True)
    
    print("🔍 DEBUG: All imports successful", flush=True)
    
    # Check environment variables
    print("🔍 DEBUG: Checking environment variables", flush=True)
    db_url = os.getenv('DB_URL')
    print(f"📊 DB_URL exists: {bool(db_url)}", flush=True)
    
    if db_url:
        print(f"📊 DB_URL first 30 chars: {db_url[:30]}...", flush=True)
    else:
        print("❌ DB_URL is None", flush=True)
    
    print("🎉 DEBUG VERSION COMPLETE!", flush=True)
    
    # Keep running
    import time
    print("💤 Keeping service alive...", flush=True)
    while True:
        time.sleep(60)
        print("💤 Still running...", flush=True)
        
except Exception as e:
    print(f"❌ ERROR: {e}", flush=True)
    import traceback
    traceback.print_exc()
    print("❌ Script failed!", flush=True)
