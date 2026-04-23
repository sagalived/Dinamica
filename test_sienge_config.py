#!/usr/bin/env python3
"""Test script to validate Sienge credentials are loaded."""

from dotenv import load_dotenv
import os

load_dotenv()

sienge_url = os.getenv("SIENGE_BASE_URL")
sienge_name = os.getenv("SIENGE_ACCESS_NAME")
sienge_token = os.getenv("SIENGE_TOKEN")

print("✓ Environment Variables Loaded:")
print(f"  - SIENGE_BASE_URL: {sienge_url}")
print(f"  - SIENGE_ACCESS_NAME: {sienge_name}")
print(f"  - SIENGE_TOKEN: {sienge_token[:20] if sienge_token else 'NOT SET'}...")

if sienge_url and sienge_name and sienge_token:
    print("\n✅ All Sienge credentials configured!")
else:
    print("\n❌ Missing Sienge credentials in .env")
