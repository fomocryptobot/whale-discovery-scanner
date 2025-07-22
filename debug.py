#!/usr/bin/env python3
import os

print("🔍 DEBUG: Current working directory:")
print(os.getcwd())

print("\n🔍 DEBUG: Contents of /opt/render/project:")
try:
    for item in os.listdir('/opt/render/project'):
        print(f"  - {item}")
except Exception as e:
    print(f"  ❌ Error: {e}")

print("\n🔍 DEBUG: Contents of /opt/render/project/src:")
try:
    for item in os.listdir('/opt/render/project/src'):
        print(f"  - {item}")
except Exception as e:
    print(f"  ❌ Error: {e}")

print("\n🔍 DEBUG: Does whale_discovery_scanner.py exist?")
file_path = '/opt/render/project/src/whale_discovery_scanner.py'
print(f"  File path: {file_path}")
print(f"  Exists: {os.path.exists(file_path)}")

if os.path.exists('/opt/render/project/src'):
    print("\n🔍 DEBUG: All files in src directory:")
    for root, dirs, files in os.walk('/opt/render/project/src'):
        for file in files:
            print(f"  - {os.path.join(root, file)}")
