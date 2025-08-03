#!/usr/bin/env python3
"""
Quick test script for your specific SQLite Cloud API key setup
"""

import time
from datetime import datetime

# Your connection string
CONNECTION_STRING = "sqlitecloud://csqqfgp8hk.g4.sqlite.cloud:8860/fifi.sqlite?apikey=24p3XmE4EZDbG7evCbPxskGJxGCOd7S96lmeUGV7b1o"

def test_api_key_connection():
    print("🔑 TESTING YOUR SQLITE CLOUD API KEY CONNECTION")
    print("=" * 60)
    
    try:
        import sqlitecloud
        print("✅ sqlitecloud library available")
    except ImportError:
        print("❌ sqlitecloud library not available")
        print("💡 Install with: pip install sqlitecloud")
        return False
    
    print(f"\n🔗 Connection String Analysis:")
    print(f"   Host: csqqfgp8hk.g4.sqlite.cloud")
    print(f"   Port: 8860")
    print(f"   Database: fifi.sqlite")
    print(f"   Auth: API Key (length: {len('24p3XmE4EZDbG7evCbPxskGJxGCOd7S96lmeUGV7b1o')} chars)")
    
    try:
        print(f"\n🔄 Attempting connection...")
        start_time = time.time()
        
        # Connect using API key
        conn = sqlitecloud.connect(CONNECTION_STRING)
        connection_time = (time.time() - start_time) * 1000
        print(f"✅ Connection established! ({connection_time:.1f}ms)")
        
        # Test basic query
        print(f"🧪 Testing basic query...")
        result = conn.execute("SELECT 1 as test").fetchone()
        if result and result[0] == 1:
            print(f"✅ Basic query successful: {result}")
        else:
            print(f"❌ Basic query failed: {result}")
            return False
        
        # Test database info
        print(f"📋 Getting database information...")
        try:
            db_list = conn.execute("PRAGMA database_list").fetchall()
            print(f"✅ Database info retrieved: {len(db_list)} databases accessible")
        except Exception as db_error:
            print(f"⚠️ Database info query failed: {db_error}")
        
        # **CRITICAL TEST: Write Permissions**
        print(f"🔐 Testing WRITE permissions (this is likely the issue)...")
        try:
            test_table = f"write_test_{int(time.time())}"
            
            # Create table
            conn.execute(f"CREATE TABLE IF NOT EXISTS {test_table} (id INTEGER, timestamp TEXT)")
            print(f"✅ CREATE TABLE successful")
            
            # Insert data
            test_time = datetime.now().isoformat()
            conn.execute(f"INSERT INTO {test_table} (id, timestamp) VALUES (?, ?)", (1, test_time))
            print(f"✅ INSERT successful")
            
            # Read data back
            data = conn.execute(f"SELECT * FROM {test_table} WHERE id = 1").fetchone()
            if data:
                print(f"✅ SELECT successful: {data}")
            else:
                print(f"❌ SELECT returned no data")
            
            # Clean up
            conn.execute(f"DROP TABLE {test_table}")
            print(f"✅ DROP TABLE successful")
            
            print(f"🎉 WRITE PERMISSIONS CONFIRMED! Your API key has full access.")
            
        except Exception as write_error:
            print(f"❌ WRITE PERMISSION TEST FAILED: {write_error}")
            print(f"\n💡 SOLUTION FOUND:")
            
            if "writing data" in str(write_error).lower():
                print(f"   🔍 Error Type: API Key lacks WRITE permissions")
                print(f"   🛠️  Fix Steps:")
                print(f"      1. Go to https://sqlitecloud.io/dashboard")
                print(f"      2. Navigate to your 'fifi.sqlite' database")
                print(f"      3. Go to 'API Keys' section")
                print(f"      4. Find your current API key")
                print(f"      5. Ensure it has 'Write' or 'Full Access' permissions")
                print(f"      6. If not, create a new API key with proper permissions")
                print(f"      7. Update your environment variable")
            else:
                print(f"   🔍 Unexpected write error: {write_error}")
            
            conn.close()
            return False
        
        # Test sessions table (if it exists)
        print(f"📊 Checking for sessions table...")
        try:
            sessions_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone()
            if sessions_check:
                count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
                active_count = conn.execute("SELECT COUNT(*) FROM sessions WHERE active = 1").fetchone()
                print(f"✅ Sessions table exists: {count[0]} total, {active_count[0]} active")
            else:
                print(f"ℹ️ Sessions table doesn't exist yet (will be created automatically)")
        except Exception as sessions_error:
            print(f"⚠️ Sessions table check failed: {sessions_error}")
        
        conn.close()
        print(f"✅ Connection closed successfully")
        print(f"\n🎉 ALL TESTS PASSED! Your API key setup is working correctly.")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        
        # Provide specific guidance
        error_str = str(e).lower()
        print(f"\n💡 DIAGNOSIS:")
        
        if "api" in error_str or "key" in error_str:
            print(f"   🔍 Issue: API Key problem")
            print(f"   🛠️  Solutions:")
            print(f"      • Verify the API key is correct")
            print(f"      • Check if the API key has expired")
            print(f"      • Ensure the API key belongs to the correct database")
        elif "timeout" in error_str or "connection" in error_str:
            print(f"   🔍 Issue: Network connectivity")
            print(f"   🛠️  Solutions:")
            print(f"      • Check your internet connection")
            print(f"      • Verify the hostname: csqqfgp8hk.g4.sqlite.cloud")
            print(f"      • Try again in a few minutes")
        elif "permission" in error_str or "access" in error_str:
            print(f"   🔍 Issue: Permission denied")
            print(f"   🛠️  Solutions:")
            print(f"      • Check API key permissions in SQLite Cloud dashboard")
            print(f"      • Ensure database is not in read-only mode")
        else:
            print(f"   🔍 Issue: Unknown error")
            print(f"   🛠️  Solution: Check SQLite Cloud service status")
        
        return False

if __name__ == "__main__":
    success = test_api_key_connection()
    
    if success:
        print(f"\n✅ NEXT STEP: Deploy the updated FastAPI code with API key support")
    else:
        print(f"\n❌ FIX THE ISSUES ABOVE FIRST, then deploy the updated FastAPI code")
