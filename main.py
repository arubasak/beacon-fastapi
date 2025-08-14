from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import json
import sqlite3
import threading
import copy
import httpx
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict
import io
import html
import re
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import lightgrey
import base64

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# CRITICAL: Lightweight FastAPI app initialization - NO BLOCKING OPERATIONS
app = FastAPI(title="FiFi Emergency API - Async Operations", version="3.6.0-final-fix-v4") # Version bumped for clarity

# CRITICAL: Proper CORS to handle OPTIONS requests quickly
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://fifi-eu.streamlit.app", "*"],  # Add your specific domains
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,  # Cache preflight for 1 hour to reduce OPTIONS requests
)

@app.options("/emergency-save")
async def emergency_save_options():
    """CRITICAL: Ultra-fast OPTIONS handler to prevent 504 timeouts"""
    return {"status": "ok"}

@app.options("/cleanup-expired-sessions")
async def cleanup_options():
    """Fast OPTIONS handler for cleanup endpoint"""
    return {"status": "ok"}

# Configuration from environment variables (accessed during startup)
SQLITE_CLOUD_CONNECTION = os.getenv("SQLITE_CLOUD_CONNECTION")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ENABLED = all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN])

# Graceful fallback for optional imports
SQLITECLOUD_AVAILABLE = False
try:
    import sqlitecloud
    SQLITECLOUD_AVAILABLE = True
    logger.info("✅ sqlitecloud SDK detected and available (at module load time).")
except ImportError:
    logger.warning("❌ SQLiteCloud SDK not available. Emergency beacon will use local SQLite fallback.")

# Declare managers globally, but initialize them in the startup event
db_manager: 'ResilientDatabaseManager' = None
pdf_exporter: 'PDFExporter' = None
zoho_manager: 'ZohoCRMManager' = None

# CRITICAL FIX: All heavy initialization now in async startup event
@app.on_event("startup")
async def startup_event():
    """
    CRITICAL FIX: Defer all heavy initialization to the startup event.
    This ensures `main.py` loads instantly, and the application starts
    accepting requests quickly. Any blocking I/O is handled in `asyncio.to_thread`.
    """
    global db_manager, pdf_exporter, zoho_manager # pylint: disable=global-statement
    
    logger.info("🚀 FastAPI startup event triggered - Initializing managers asynchronously...")

    # Initialize PDF exporter first (lightweight)
    pdf_exporter = PDFExporter()
    
    # Initialize DB Manager. Its connection logic is lazy and async.
    db_manager = ResilientDatabaseManager(SQLITE_CLOUD_CONNECTION)
    
    # Initialize Zoho Manager. Its HTTP client is async.
    zoho_manager = ZohoCRMManager(pdf_exporter)

    # Optional: Perform a QUICK, AGGRESSIVELY TIMED-OUT database connection test to ensure it's ready.
    # This prevents startup from hanging. If it fails within 1 second, the app starts anyway,
    # and db_manager falls back to memory mode, preserving responsiveness.
    try:
        logger.info("Attempting initial database connection test during startup (max 1s)...")
        # Test connection, but force it to complete or raise TimeoutError within 1 second.
        # This prevents Cloud Run startup timeouts.
        await asyncio.wait_for(db_manager.test_connection(), timeout=1) 
        logger.info("Initial DB connection test in startup completed successfully (or fell back to memory within 1s).")
    except asyncio.TimeoutError:
        logger.warning("Initial DB connection test in startup timed out. Proceeding with application startup, DB will be in fallback memory mode.")
        # Ensure db_manager is in memory mode if it timed out during startup test
        await asyncio.to_thread(db_manager._fallback_to_memory_sync) 
    except Exception as e:
        logger.error(f"Error during initial DB connection test in startup: {e}", exc_info=True)
        # Ensure db_manager is in memory mode on any error during startup test
        await asyncio.to_thread(db_manager._fallback_to_memory_sync) 

    logger.info("✅ FastAPI startup initialization complete. App is ready to receive requests.")
    logger.info(f"🔧 CONFIG CHECK (after startup):")
    logger.info(f"SQLite Cloud: {'SET' if SQLITE_CLOUD_CONNECTION else 'MISSING'}")
    logger.info(f"Zoho Enabled: {ZOHO_ENABLED}")


# Models (UPDATED FOR COMPATIBILITY)
class EmergencySaveRequest(BaseModel):
    session_id: str

    reason: str
    timestamp: Optional[int] = None

class UserType(Enum):
    GUEST = "guest"
    EMAIL_VERIFIED_GUEST = "email_verified_guest"
    REGISTERED_USER = "registered_user"

class BanStatus(Enum):
    NONE = "none"
    ONE_HOUR = "1hour"
    TWENTY_FOUR_HOUR = "24hour"
    EVASION_BLOCK = "evasion_block"

@dataclass
class UserSession:
    session_id: str
    user_type: UserType = UserType.GUEST
    email: Optional[str] = None
    full_name: Optional[str] = None
    zoho_contact_id: Optional[str] = None
    active: bool = True
    wp_token: Optional[str] = None
    messages: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    timeout_saved_to_crm: bool = False
    fingerprint_id: Optional[str] = None
    fingerprint_method: Optional[str] = None
    visitor_type: str = "new_visitor"
    recognition_response: Optional[str] = None
    daily_question_count: int = 0
    total_question_count: int = 0
    last_question_time: Optional[datetime] = None
    question_limit_reached: bool = False
    ban_status: BanStatus = BanStatus.NONE
    ban_start_time: Optional[datetime] = None
    ban_end_time: Optional[datetime] = None # Changed to datetime to match fifi.py (isoformat will convert it to string in DB)
    ban_reason: Optional[str] = None
    evasion_count: int = 0
    current_penalty_hours: int = 0
    escalation_level: int = 0
    email_addresses_used: List[str] = field(default_factory=list)
    email_switches_count: int = 0
    browser_privacy_level: Optional[str] = None
    registration_prompted: bool = False
    registration_link_clicked: bool = False
    display_message_offset: int = 0
    # NEW FIELDS FROM FIFI.PY FOR COMPATIBILITY:
    reverification_pending: bool = False
    pending_user_type: Optional[UserType] = None
    pending_email: Optional[str] = None
    pending_full_name: Optional[str] = None
    pending_zoho_contact_id: Optional[str] = None
    pending_wp_token: Optional[str] = None

# Utility functions (unchanged)
def safe_json_loads(data: Optional[str], default_value: Any = None) -> Any:
    if data is None or data == "":
        return default_value
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Failed to decode JSON data: {str(data)[:100]}...")
        return default_value

def is_session_ending_reason(reason: str) -> bool:
    """Determines if an emergency save reason should end the session immediately"""
    session_ending_keywords = [
        'beforeunload', 'unload', 'close', 'refresh', 'timeout', 
        'parent_beforeunload', 'browser_close', 'tab_close', 
        'window_close', 'page_refresh', 'browser_refresh',
        'session_timeout', 'inactivity'
    ]
    
    reason_lower = reason.lower()
    return any(keyword in reason_lower for keyword in session_ending_keywords)

# CRITICAL FIX: Lazy Database Manager - ASYNC/THREAD-POOLED
class ResilientDatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock() # Still need a lock for the internal connection object
        self.conn = None
        self.connection_string = connection_string
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=2)
        self._connection_attempts = 0
        self._max_connection_attempts = 3  # REDUCED for faster timeout
        self._last_socket_error = None
        self._consecutive_socket_errors = 0
        self._auth_method = None
        self.db_type = "memory" # Start in memory mode by default
        self.local_sessions = {} # For in-memory fallback
        self._initialized_schema = False # Flag indicates if persistent schema has been created in the current DB
        self._initialization_attempted_in_session = False # Initialize the new attribute

        logger.info("🔄 ResilientDatabaseManager initialized (LAZY ASYNC - NO BLOCKING I/O in constructor)")
        if connection_string:
            self._analyze_connection_string()
        else:
            logger.info("ℹ️ No SQLite Cloud connection string provided. Will use lazy local/memory fallback.")

    def _analyze_connection_string(self):
        """Analyze connection string without connecting - NO I/O"""
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.connection_string)
            query_params = parse_qs(parsed.query)
            
            if 'apikey' in query_params:
                self._auth_method = "API_KEY"
                # FIXED: Access the actual value from the parsed query list
                apikey = query_params['apikey'][0] 
                logger.debug(f"🔑 Detected API Key authentication (key length: {len(apikey)} chars)")
            elif parsed.username and parsed.password:
                self._auth_method = "USERNAME_PASSWORD"
                logger.debug(f"🔑 Detected Username/Password authentication (user: {parsed.username})")
            else:
                self._auth_method = "UNKNOWN"
                logger.warning("⚠️ Could not determine authentication method from connection string")
                
        except Exception as e:
            logger.error(f"❌ Failed to analyze connection string: {e}")
            self._auth_method = "PARSE_ERROR"

    async def _ensure_connection_with_timeout(self, max_wait_seconds: int = 30):
        """CRITICAL: Time-limited connection establishment to prevent 504 timeouts for the calling async task."""
        start_time = datetime.now()
        
        # 1. If connection is already healthy AND non-memory, reuse.
        if self.conn and self.db_type != "memory" and await asyncio.to_thread(self._check_connection_health_sync):
            logger.debug("✅ Persistent connection healthy, reusing.")
            # Ensure schema is considered initialized if we successfully reuse a persistent connection
            if not self._initialized_schema:
                try:
                    await asyncio.to_thread(self._init_complete_database_sync) # Ensure schema exists if not yet
                    self._initialized_schema = True
                    logger.info("✅ Ensured schema initialized on reuse.")
                except Exception as e:
                    logger.error(f"❌ Schema check on reuse failed: {e}. Forcing re-connection.", exc_info=True)
                    self.conn = None # Invalidate so it tries to re-connect
                    self.db_type = "memory" # Reset to memory mode
                    self._initialized_schema = False # Reset schema flag
            if self.conn: # If schema check/init didn't fail
                return True

        # 2. If already in memory mode and no time left for a proper connection attempt, just return True.
        if self.db_type == "memory" and (datetime.now() - start_time).total_seconds() >= max_wait_seconds:
            logger.warning("⏰ Already in memory mode and ran out of time for new connection attempt. Sticking to memory.")
            return True

        # 3. If a connection attempt was recently made and failed, stick to memory for a bit
        # This prevents endless rapid retries on a truly broken connection.
        if self._initialization_attempted_in_session and \
           (datetime.now() - self._last_health_check if self._last_health_check else timedelta(0)).total_seconds() < 5:
             logger.warning("⚠️ Recent initialization attempt detected, sticking to current (likely memory) mode for speed")
             return True

        # If we reach here, we need to establish a new connection.
        self._initialization_attempted_in_session = True # Mark that an attempt is being made
        self._last_health_check = datetime.now() # Reset health check timer for this attempt

        # Close any existing unhealthy/stale connection
        if self.conn:
            logger.warning("⚠️ Existing connection unhealthy or being re-established. Closing.")
            try:
                await asyncio.to_thread(self.conn.close)
            except Exception as e:
                logger.debug(f"⚠️ Error closing old DB connection: {e}")
            self.conn = None
            self.db_type = "memory" # Temporarily assume memory until new connection established
        
        # IMPORTANT: Reset schema flag before attempting a new persistent connection.
        # This is crucial for the "no such table" fix.
        self._initialized_schema = False 

        # Attempt Cloud connection
        cloud_connection_successful = False
        if self.connection_string and SQLITECLOUD_AVAILABLE:
            try:
                await self._attempt_quick_cloud_connection_async(max_wait_seconds - (datetime.now() - start_time).total_seconds())
                if self.conn and self.db_type == "cloud":
                    cloud_connection_successful = True
                else:
                    # Explicitly log if _attempt_quick_cloud_connection_async finished but didn't result in 'cloud' db_type
                    logger.error(f"❌ SQLite Cloud connection attempt finished, but db_type is '{self.db_type}' (expected 'cloud'). Connection string might be invalid or other issue. Debugging further...")
            except Exception as e:
                # Change from debug to error here to make it visible
                logger.error(f"❌ SQLite Cloud connection attempt failed in _ensure_connection (caught exception): {e}", exc_info=True)
                self.conn = None # Ensure connection is reset on failure here too
                self.db_type = "memory" # Ensure db_type is memory if cloud fails
        else:
            logger.info(f"ℹ️ Skipping SQLite Cloud connection attempt. Connection string set: {bool(self.connection_string)}, SDK available: {SQLITECLOUD_AVAILABLE}.")


        # Fallback to local SQLite if cloud failed or not configured, and still no persistent connection
        if not cloud_connection_successful: # Only try local if cloud wasn't successful
            logger.info("☁️ Cloud connection failed/unavailable, trying local SQLite...")
            await asyncio.to_thread(self._attempt_local_connection_sync) # This sets self.conn and self.db_type
            if self.db_type != "file":
                 logger.error("❌ Local SQLite fallback connection also failed. Defaulting to in-memory.")

        # If a non-memory connection is now established (either cloud or local), ensure schema is initialized
        if self.conn and self.db_type != "memory":
            try:
                # Always attempt schema initialization on a new non-memory connection or when coming from memory mode.
                # CREATE TABLE IF NOT EXISTS makes this idempotent and safe to run multiple times.
                await asyncio.to_thread(self._init_complete_database_sync)
                self._initialized_schema = True # Set True only if persistent schema init succeeded
                logger.info("✅ Database schema initialized successfully.")
            except Exception as e:
                logger.error(f"❌ Schema initialization failed: {e}. Falling back to memory.", exc_info=True)
                await asyncio.to_thread(self._fallback_to_memory_sync) # This will set db_type="memory" and _initialized_schema=True.
                return True # Schema init failed, forced to memory, so return True (processed)

        # Final check: if still no successful persistent connection, ensure we're in memory mode.
        if not self.conn or self.db_type == "memory":
            logger.warning("🚨 No persistent connection could be established. Falling back to in-memory storage.")
            await asyncio.to_thread(self._fallback_to_memory_sync) # This sets db_type="memory" and _initialized_schema=True.

        return True # Connection established (or fallback to memory)

    async def _attempt_quick_cloud_connection_async(self, max_wait_seconds: float):
        """OPTIMIZED: Quick connection attempt with reduced retries for background processing"""
        # Ensure max_wait_seconds is not negative or zero
        max_wait_seconds = max(1.0, max_wait_seconds) 
        max_attempts = min(self._max_connection_attempts, 2) # Limit to 2 attempts for speed
        
        for attempt in range(max_attempts):
            if max_wait_seconds <= 0:
                logger.warning("⏰ No time left for cloud connection attempts")
                return
                
            try:
                logger.info(f"🔄 QUICK SQLite Cloud connection attempt {attempt + 1}/{max_attempts}")
                
                if self.conn: # Close any previous partial connection
                    try:
                        await asyncio.to_thread(self.conn.close)
                    except Exception as e:
                        logger.debug(f"Error closing previous connection: {e}")
                    self.conn = None
                
                connection_start_attempt = datetime.now()
                # ADOPTED FROM FIFI.PY: Removed 'timeout' argument here
                self.conn = await asyncio.to_thread(sqlitecloud.connect, self.connection_string)
                
                # Test the connection to confirm it's truly open
                test_result = await asyncio.to_thread(self.conn.execute, "SELECT 1 as connection_test")
                # FIXED: Correctly access the value from the tuple returned by fetchone()
                test_result_fetched = await asyncio.to_thread(test_result.fetchone)
                
                if test_result_fetched and test_result_fetched[0] == 1: 
                    logger.info(f"✅ QUICK SQLite Cloud connection established using {self._auth_method}!")
                    self.db_type = "cloud"
                    self._connection_attempts = 0 # Reset for next cycle
                    self._consecutive_socket_errors = 0
                    return # Success, exit
                else:
                    raise Exception(f"Connection test failed - unexpected result: {test_result_fetched}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ QUICK SQLite Cloud connection attempt {attempt + 1} failed: {error_msg}")
                
                # Ensure connection is closed on failure
                if self.conn:
                    try:
                        await asyncio.to_thread(self.conn.close)
                    except Exception as close_e:
                        logger.debug(f"Error closing failed cloud connection: {close_e}")
                    self.conn = None
                
                # Calculate remaining time for next attempt
                elapsed_this_attempt = (datetime.now() - connection_start_attempt).total_seconds()
                max_wait_seconds -= elapsed_this_attempt
                
                if attempt < max_attempts - 1 and max_wait_seconds > 0.5: # Ensure some time left for sleep and next attempt
                    await asyncio.sleep(min(2, max_wait_seconds / (max_attempts - attempt))) # Distribute remaining time

        logger.error(f"❌ All {max_attempts} QUICK SQLite Cloud connection attempts failed")
        self.db_type = "memory" # Fallback to memory if all quick cloud attempts fail

    def _attempt_local_connection_sync(self):
        """Fallback to local SQLite (synchronous version for to_thread)"""
        try:
            logger.info("🔄 Attempting local SQLite connection as fallback...")
            self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
            
            test_result = self.conn.execute("SELECT 1 as test_local").fetchone()
            # FIXED: Correctly access the value from the tuple returned by fetchone()
            if test_result and test_result[0] == 1:
                logger.info(f"✅ Local SQLite connection established! Test result: {test_result}")
                self.db_type = "file"
            else:
                raise Exception(f"Local connection test failed: {test_result}")
                
        except Exception as e:
            logger.error(f"❌ Local SQLite connection failed: {e}", exc_info=True)
            self._fallback_to_memory_sync()

    def _fallback_to_memory_sync(self):
        """Fallback to in-memory storage (synchronous version for to_thread)"""
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                logger.debug(f"Error closing connection before memory fallback: {e}")
        self.conn = None
        self.db_type = "memory"
        self.local_sessions = {} # Clear any partial local state
        # In memory, schema is recreated on each run, so consider it "initialized" for memory logic
        self._initialized_schema = True 
        logger.warning("⚠️ Operating in in-memory mode due to connection issues")

    def _init_complete_database_sync(self):
        """Initialize database schema with all columns upfront (synchronous version for to_thread)"""
        # --- NEW IMPORT FOR SQLITECLOUD EXCEPTION TYPE ---
        try:
            from sqlitecloud.exceptions import SQLiteCloudOperationalError as SQLiteCloudOperationalError # Import specific exception
        except ImportError:
            SQLiteCloudOperationalError = type('DummySQLiteCloudOperationalError', (sqlite3.OperationalError,), {}) # Fallback to sqlite3 type if not available
        # --- END NEW IMPORT ---

        try:
            if hasattr(self.conn, 'row_factory'): 
                self.conn.row_factory = None

            logger.info("🏗️ Creating database schema...")
            
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    user_type TEXT DEFAULT 'guest',
                    email TEXT,
                    full_name TEXT,
                    zoho_contact_id TEXT,
                    created_at TEXT DEFAULT '',
                    last_activity TEXT DEFAULT '',
                    messages TEXT DEFAULT '[]',
                    active INTEGER DEFAULT 1,
                    wp_token TEXT,
                    timeout_saved_to_crm INTEGER DEFAULT 0,
                    fingerprint_id TEXT,
                    fingerprint_method TEXT,
                    visitor_type TEXT DEFAULT 'new_visitor',
                    daily_question_count INTEGER DEFAULT 0,
                    total_question_count INTEGER DEFAULT 0,
                    last_question_time TEXT,
                    question_limit_reached INTEGER DEFAULT 0,
                    ban_status TEXT DEFAULT 'none',
                    ban_start_time TEXT,
                    ban_end_time TEXT,
                    ban_reason TEXT,
                    evasion_count INTEGER DEFAULT 0,
                    current_penalty_hours INTEGER DEFAULT 0,
                    escalation_level INTEGER DEFAULT 0,
                    email_addresses_used TEXT DEFAULT '[]',
                    email_switches_count INTEGER DEFAULT 0,
                    browser_privacy_level TEXT,
                    registration_prompted INTEGER DEFAULT 0,
                    registration_link_clicked INTEGER DEFAULT 0,
                    recognition_response TEXT,
                    display_message_offset INTEGER DEFAULT 0,
                    -- NEW: Re-verification fields from fifi.py for compatibility
                    reverification_pending INTEGER DEFAULT 0,
                    pending_user_type TEXT,
                    pending_email TEXT,
                    pending_full_name TEXT,
                    pending_zoho_contact_id TEXT,
                    pending_wp_token TEXT
                )
            ''')
            
            # Add new columns if they don't exist (for existing databases)
            # This logic should create them if they weren't in an older version of the DB
            new_columns_to_add = [
                ("display_message_offset", "INTEGER DEFAULT 0"),
                ("reverification_pending", "INTEGER DEFAULT 0"),
                ("pending_user_type", "TEXT"),
                ("pending_email", "TEXT"),
                ("pending_full_name", "TEXT"),
                ("pending_zoho_contact_id", "TEXT"),
                ("pending_wp_token", "TEXT")
            ]
            for col_name, col_type in new_columns_to_add:
                try:
                    self.conn.execute(f"ALTER TABLE sessions ADD COLUMN {col_name} {col_type}")
                    logger.info(f"✅ Added {col_name} column to existing database for compatibility.")
                except (sqlite3.OperationalError, SQLiteCloudOperationalError) as e: # Catch both SQLite3 and SQLiteCloud specific errors
                    # Catch specific error when column already exists
                    if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower():
                        logger.debug(f"Column {col_name} already exists, skipping ALTER TABLE.")
                    else:
                        raise e # Re-raise for other operational errors that are not about duplicate columns
                except Exception as e:
                    # This fallback should ideally not be hit for duplicate column errors anymore.
                    # It will catch other unexpected errors during ALTER TABLE.
                    logger.error(f"❌ Error adding column {col_name}: {e}", exc_info=True)
                    raise # Critical failure for schema consistency


            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_session_lookup ON sessions(session_id, active)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fingerprint_id ON sessions(fingerprint_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON sessions(email)")
            
            self.conn.commit()
            logger.info("✅ Database schema and indexes created successfully.")
            
        except Exception as e:
            logger.error(f"❌ Database schema creation failed: {e}", exc_info=True)
            raise

    def _check_connection_health_sync(self) -> bool:
        """Enhanced health check with socket error detection (synchronous version for to_thread)"""
        if self.db_type == "memory":
            return True
            
        if not self.conn:
            logger.debug("❌ No database connection object available")
            return False
            
        now = datetime.now()
        if (self._last_health_check and 
            now - self._last_health_check < self._health_check_interval and
            self._consecutive_socket_errors == 0):
            return True # Use cached health status
            
        try:
            logger.debug(f"🔍 Performing health check...")
            # Use a quick, non-blocking operation
            result = self.conn.execute("SELECT 1 as health_check").fetchone()
            
            # FIXED: Correctly check value from tuple
            if result and result[0] == 1: 
                self._last_health_check = now
                self._consecutive_socket_errors = 0
                logger.debug("✅ Database health check passed")
                return True
            else:
                logger.error(f"❌ Database health check failed - unexpected result: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            self.conn = None # Invalidate connection on error
            return False

    def _is_socket_error(self, error: Exception) -> bool:
        """Detect socket-related errors"""
        error_str = str(error).lower()
        socket_indicators = [
            "reading command length from the socket",
            "incomplete response from server",
            "cannot read the command length",
            "socket",
            "connection reset",
            "connection aborted",
            "broken pipe",
            "network is unreachable",
            "connection timed out",
            "no route to host"
        ]
        return any(indicator in error_str for indicator in socket_indicators)

    async def _execute_with_socket_retry_async(self, query: str, params: tuple = None, max_retries: int = 2):
        """Execute query with automatic socket error retry - REDUCED retries for speed"""
        for attempt in range(max_retries):
            try:
                # Ensure connection is ready before executing query (with timeout)
                # Use a specific timeout for this internal step
                await self._ensure_connection_with_timeout(15) 
                
                if self.db_type == "memory":
                    raise Exception("Cannot execute SQL when in-memory mode is active for DB operations")
                
                if not self.conn:
                    raise Exception("No database connection available after _ensure_connection_with_timeout")
                
                # Execute the actual database operation in a thread
                if params:
                    result = await asyncio.to_thread(self.conn.execute, query, params)
                else:
                    result = await asyncio.to_thread(self.conn.execute, query)
                
                if self._consecutive_socket_errors > 0:
                    logger.info(f"✅ Query executed successfully after {self._consecutive_socket_errors} previous socket errors")
                    self._consecutive_socket_errors = 0
                
                return result
                
            except Exception as e:
                is_socket_error = self._is_socket_error(e)
                
                if is_socket_error:
                    self._consecutive_socket_errors += 1
                    logger.error(f"🔌 Socket error during query execution (attempt {attempt + 1}/{max_retries}): {e}")
                    
                    if attempt < max_retries - 1:
                        self.conn = None # Invalidate current connection
                        wait_time = 1
                        logger.info(f"⏳ Retrying query in {wait_time} seconds due to socket error...")
                        await asyncio.sleep(wait_time)
                        continue
                else:
                    logger.error(f"❌ Non-socket error during query execution: {e}")
                
                if attempt == max_retries - 1 or not is_socket_error:
                    # If max retries reached or it's not a socket error, re-raise the exception
                    raise

    async def quick_status_check(self) -> Dict[str, Any]:
        """OPTIMIZED: Ultra-fast status check without triggering long initialization"""
        try:
            return {
                "timestamp": datetime.now(),
                "auth_method": self._auth_method,
                "connection_attempts": self._connection_attempts,
                "socket_errors": self._consecutive_socket_errors,
                "last_socket_error": self._last_socket_error.isoformat() if self._last_socket_error else None,
                "initialization_attempted_in_session": self._initialization_attempted_in_session,
                "db_type": self.db_type,
                "status": "quick_check_only",
                "message": "Quick status without full initialization"
            }
        except Exception as e:
            return {
                "status": "quick_check_failed",
                "error": str(e),
                "timestamp": datetime.now()
            }

    async def test_connection(self) -> Dict[str, Any]:
        """Comprehensive connection test - LAZY INITIALIZATION ON FIRST CALL WITH TIMEOUT"""
        try:
            # This method itself might be called with asyncio.wait_for from startup.
            # So, the internal _ensure_connection_with_timeout also needs its own aggressive timeout.
            success = await self._ensure_connection_with_timeout(5) # Short timeout for test_connection itself
            if not success:
                return {
                    "status": "initialization_timeout",
                    "type": self.db_type,
                    "message": "Database initialization timed out, using fallback mode",
                    "timestamp": datetime.now()
                }
                
            result = {
                "timestamp": datetime.now(),
                "auth_method": self._auth_method,
                "connection_attempts": self._connection_attempts,
                "socket_errors": self._consecutive_socket_errors,
                "last_socket_error": self._last_socket_error.isoformat() if self._last_socket_error else None
            }
            
            if self.db_type == "memory":
                return {
                    **result,
                    "status": "healthy",
                    "type": "memory",
                    "session_count": len(self.local_sessions),
                    "message": "Running in non-persistent memory mode"
                }
            
            if not self.conn:
                return {
                    **result,
                    "status": "failed",
                    "type": self.db_type,
                    "message": "No database connection available after ensure_connection"
                }
            
            try:
                basic_result_cursor = await self._execute_with_socket_retry_async("SELECT 1 as connectivity_test", max_retries=1) # Quick test
                # FIXED: Correctly access the value from the tuple returned by fetchone()
                basic_result_fetched = await asyncio.to_thread(basic_result_cursor.fetchone)
                if not basic_result_fetched or basic_result_fetched[0] == 1:
                    raise Exception(f"Connectivity test failed: {basic_result_fetched}")
                
                try:
                    sessions_check_cursor = await self._execute_with_socket_retry_async("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'", max_retries=1)
                    sessions_check_fetched = await asyncio.to_thread(sessions_check_cursor.fetchone)
                    if sessions_check_fetched:
                        count_result_cursor = await self._execute_with_socket_retry_async("SELECT COUNT(*) FROM sessions", max_retries=1)
                        count_result_fetched = await asyncio.to_thread(count_result_cursor.fetchone)
                        active_count_result_cursor = await self._execute_with_socket_retry_async("SELECT COUNT(*) FROM sessions WHERE active = 1", max_retries=1)
                        active_count_result_fetched = await asyncio.to_thread(active_count_result_cursor.fetchone)
                        result["sessions_table"] = {
                            "exists": True,
                            # FIXED: Access count from tuple element 0
                            "total_count": count_result_fetched[0] if count_result_fetched else 0,
                            "active_count": active_count_result_fetched[0] if active_count_result_fetched else 0
                        }
                    else:
                        result["sessions_table"] = {"exists": False}
                except Exception as sessions_error:
                    result["sessions_table"] = f"CHECK_FAILED: {str(sessions_error)}"
                
                return {
                    **result,
                    "status": "healthy",
                    "type": self.db_type,
                    "message": f"Connected to {self.db_type} database using {self._auth_method} - all tests passed"
                }
                
            except Exception as test_error:
                logger.error(f"❌ Database functionality test failed: {test_error}", exc_info=True)
                return {
                    **result,
                    "status": "connection_ok_functionality_failed",
                    "type": self.db_type,
                    "message": f"Connection established but functionality test failed: {str(test_error)}",
                    "error_type": type(test_error).__name__
                }
                
        except Exception as e:
            logger.error(f"❌ Connection test completely failed: {e}", exc_info=True)
            return {
                "status": "critical_failure",
                "type": getattr(self, 'db_type', 'unknown'),
                "auth_method": getattr(self, '_auth_method', 'unknown'),
                "message": f"Connection test failed with critical error: {str(e)}",
                "connection_attempts": getattr(self, '_connection_attempts', 0),
                "error_type": type(e).__name__
            }

    async def load_session(self, session_id: str) -> Optional[UserSession]:
        """Load session - LAZY INITIALIZATION ON FIRST CALL WITH TIMEOUT"""
        with self.lock:
            logger.debug(f"🔍 Loading session {session_id[:8]}...")
            
            success = await self._ensure_connection_with_timeout(15) # Use a moderate timeout for loading
            if not success:
                logger.warning(f"⚠️ Connection timeout while loading session {session_id[:8]}, checking memory")
            
            if self.db_type == "memory":
                session = self.local_sessions.get(session_id)
                if session and isinstance(session.user_type, str):
                    try:
                        session.user_type = UserType(session.user_type)
                    except ValueError:
                        session.user_type = UserType.GUEST
                logger.debug(f"📝 Loaded session {session_id[:8]} from memory")
                return copy.deepcopy(session) if session else None
            
            try:
                # FIXED: Updated SELECT statement to include all new fields from fifi.py's UserSession
                cursor = await self._execute_with_socket_retry_async("""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, 
                           created_at, last_activity, messages, active, wp_token, 
                           timeout_saved_to_crm, fingerprint_id, fingerprint_method, 
                           visitor_type, daily_question_count, total_question_count, 
                           last_question_time, question_limit_reached, ban_status, 
                           ban_start_time, ban_end_time, ban_reason, evasion_count, 
                           current_penalty_hours, escalation_level, email_addresses_used, 
                           email_switches_count, browser_privacy_level, registration_prompted, 
                           registration_link_clicked, recognition_response, display_message_offset,
                           reverification_pending, pending_user_type, pending_email, pending_full_name,
                           pending_zoho_contact_id, pending_wp_token
                    FROM sessions WHERE session_id = ? AND active = 1
                """, (session_id,))
                
                row = await asyncio.to_thread(cursor.fetchone)
                
                if not row:
                    logger.info(f"❌ No active session found for {session_id[:8]}")
                    return None
                
                # FIXED: Check for the new, higher number of columns
                expected_min_cols = 38 
                if len(row) < expected_min_cols:
                    logger.error(f"❌ Row has insufficient columns: {len(row)} (expected at least {expected_min_cols}) for session {session_id[:8]}. Data corruption or old schema detected, attempting partial load.")
                    # Allow to proceed, but log error. Missing fields will default to None.
                    pass
                
                try:
                    # Safely access all fields, providing defaults for missing columns
                    loaded_display_message_offset = row[31] if len(row) > 31 else 0
                    loaded_reverification_pending = bool(row[32]) if len(row) > 32 else False
                    loaded_pending_user_type = UserType(row[33]) if len(row) > 33 and row[33] else None
                    loaded_pending_email = row[34] if len(row) > 34 else None
                    loaded_pending_full_name = row[35] if len(row) > 35 else None
                    loaded_pending_zoho_contact_id = row[36] if len(row) > 36 else None
                    loaded_pending_wp_token = row[37] if len(row) > 37 else None

                    user_session = UserSession(
                        session_id=row[0],
                        user_type=UserType(row[1]) if row[1] else UserType.GUEST,
                        email=row[2],
                        full_name=row[3],
                        zoho_contact_id=row[4],
                        created_at=datetime.fromisoformat(row[5]) if row[5] else datetime.now(),
                        last_activity=datetime.fromisoformat(row[6]) if row[6] else datetime.now(),
                        messages=safe_json_loads(row[7], []),
                        active=bool(row[8]),
                        wp_token=row[9],
                        timeout_saved_to_crm=bool(row[10]),
                        fingerprint_id=row[11],
                        fingerprint_method=row[12],
                        visitor_type=row[13] or 'new_visitor',
                        daily_question_count=row[14] or 0,
                        total_question_count=row[15] or 0,
                        last_question_time=datetime.fromisoformat(row[16]) if row[16] else None,
                        question_limit_reached=bool(row[17]),
                        ban_status=BanStatus(row[18]) if row[18] else BanStatus.NONE,
                        ban_start_time=datetime.fromisoformat(row[19]) if row[19] else None,
                        ban_end_time=datetime.fromisoformat(row[20]) if row[20] else None, # FIXED: Ensure this is datetime object for UserSession
                        ban_reason=row[21],
                        evasion_count=row[22] or 0,
                        current_penalty_hours=row[23] or 0,
                        escalation_level=row[24] or 0,
                        email_addresses_used=safe_json_loads(row[25], []),
                        email_switches_count=row[26] or 0,
                        browser_privacy_level=row[27],
                        registration_prompted=bool(row[28]),
                        registration_link_clicked=bool(row[29]),
                        recognition_response=row[30],
                        display_message_offset=loaded_display_message_offset,
                        reverification_pending=loaded_reverification_pending,
                        pending_user_type=loaded_pending_user_type,
                        pending_email=loaded_pending_email,
                        pending_full_name=loaded_pending_full_name,
                        pending_zoho_contact_id=loaded_pending_zoho_contact_id,
                        pending_wp_token=loaded_pending_wp_token
                    )
                    
                    logger.info(f"✅ Successfully loaded session {session_id[:8]}")
                    return user_session
                    
                except Exception as e:
                    logger.error(f"❌ Failed to create UserSession object from row for session {session_id[:8]}: {e}", exc_info=True)
                    logger.error(f"Problematic row data (truncated): {str(row)[:200]}")
                    return None
                    
            except Exception as e:
                logger.error(f"❌ Failed to load session {session_id[:8]}: {e}", exc_info=True)
                return None

    async def save_session(self, session: UserSession):
        """Save session - LAZY INITIALIZATION ON FIRST CALL WITH TIMEOUT"""
        with self.lock:
            success = await self._ensure_connection_with_timeout(15) # Use a moderate timeout for saving
            if not success:
                logger.warning(f"⚠️ Connection timeout while saving session {session.session_id[:8]}, using memory")
            
            if self.db_type == "memory":
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.debug(f"💾 Saved session {session.session_id[:8]} to memory")
                return
            
            try:
                # Ensure JSON serializable
                try:
                    json_messages = json.dumps(session.messages)
                    json_emails_used = json.dumps(session.email_addresses_used)
                except (TypeError, ValueError) as e:
                    logger.error(f"❌ Session data not JSON serializable for {session.session_id[:8]}: {e}")
                    json_messages = "[]"
                    json_emails_used = "[]"
                
                # FIXED: Updated INSERT/REPLACE query to include all new fields from fifi.py's UserSession
                # Ensure pending_user_type.value is used if not None
                pending_user_type_value = session.pending_user_type.value if session.pending_user_type else None

                await self._execute_with_socket_retry_async('''
                    INSERT OR REPLACE INTO sessions (
                        session_id, user_type, email, full_name, zoho_contact_id, 
                        created_at, last_activity, messages, active, wp_token, 
                        timeout_saved_to_crm, fingerprint_id, fingerprint_method, 
                        visitor_type, daily_question_count, total_question_count, 
                        last_question_time, question_limit_reached, ban_status, 
                        ban_start_time, ban_end_time, ban_reason, evasion_count, 
                        current_penalty_hours, escalation_level, email_addresses_used, 
                        email_switches_count, browser_privacy_level, registration_prompted, 
                        registration_link_clicked, recognition_response, display_message_offset,
                        reverification_pending, pending_user_type, pending_email, pending_full_name,
                        pending_zoho_contact_id, pending_wp_token
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    session.session_id, session.user_type.value, session.email, session.full_name,
                    session.zoho_contact_id, session.created_at.isoformat(),
                    session.last_activity.isoformat(), json_messages, int(session.active),
                    session.wp_token, int(session.timeout_saved_to_crm), session.fingerprint_id,
                    session.fingerprint_method, session.visitor_type, session.daily_question_count,
                    session.total_question_count,
                    session.last_question_time.isoformat() if session.last_question_time else None,
                    int(session.question_limit_reached), session.ban_status.value,
                    session.ban_start_time.isoformat() if session.ban_start_time else None,
                    session.ban_end_time.isoformat() if session.ban_end_time else None, # FIXED: Convert datetime to isoformat
                    session.ban_reason, session.evasion_count, session.current_penalty_hours,
                    session.escalation_level, json_emails_used,
                    session.email_switches_count, session.browser_privacy_level,
                    int(session.registration_prompted), int(session.registration_link_clicked),
                    session.recognition_response, session.display_message_offset,
                    int(session.reverification_pending), 
                    pending_user_type_value, # FIXED: Use the value
                    session.pending_email, session.pending_full_name,
                    session.pending_zoho_contact_id, session.pending_wp_token
                ))
                
                await asyncio.to_thread(self.conn.commit)
                logger.debug(f"✅ Successfully saved session {session.session_id[:8]} to database")
                
            except Exception as e:
                logger.error(f"❌ Failed to save session {session.session_id[:8]} to database: {e}", exc_info=True)
                
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback")

    async def cleanup_expired_sessions(self, expiry_minutes: int = 5, limit: int = 20) -> Dict[str, Any]:
        """
        COMPLETE IMPLEMENTATION: Clean up expired sessions with FULL LOGIC and CRM processing.
        Processes a limited number of sessions per call to avoid timeouts.
        """
        with self.lock:
            logger.info(f"🧹 Starting COMPLETE cleanup of sessions expired more than {expiry_minutes} minutes ago, LIMIT {limit} per run...")
            
            success = await self._ensure_connection_with_timeout(15) # Use a moderate timeout for this cleanup entrypoint
            if not success:
                logger.warning("⚠️ Connection timeout during cleanup, checking memory mode")
            
            if self.db_type == "memory":
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                expired_sessions_processed = []
                crm_eligible_sessions_to_process = [] 
                
                # Sort to ensure consistent processing order if limit is applied to memory
                sorted_sessions = sorted(list(self.local_sessions.items()), key=lambda item: item[1].last_activity or datetime.min)

                sessions_to_check = 0
                for session_id, session in sorted_sessions:
                    if sessions_to_check >= limit: # Apply limit for memory mode too
                        break
                    
                    if session.active and session.last_activity < cutoff_time:
                        sessions_to_check += 1 # Count towards limit only if it's potentially eligible
                        if (not session.timeout_saved_to_crm and 
                            is_crm_eligible(session, is_emergency_save=False)):
                            crm_eligible_sessions_to_process.append(copy.deepcopy(session))
                        
                        # Mark as inactive in memory, regardless of CRM save
                        session.active = False
                        expired_sessions_processed.append(session_id)
                        logger.debug(f"🔄 Marked in-memory session {session_id[:8]} as inactive")
                
                more_sessions_remaining = len(sorted_sessions) > sessions_to_check
                logger.info(f"✅ Cleaned up {len(expired_sessions_processed)} expired sessions from memory (limit {limit})")
                logger.info(f"📝 Found {len(crm_eligible_sessions_to_process)} sessions eligible for CRM save in memory. More remaining: {more_sessions_remaining}")
                
                return {
                    "success": True,
                    "cleaned_up_count": len(expired_sessions_processed),
                    "crm_eligible_count": len(crm_eligible_sessions_to_process),
                    "storage_type": "memory",
                    "expired_session_ids": [sid[:8] + "..." for sid in expired_sessions_processed],
                    "crm_eligible_sessions": [s.session_id[:8] + "..." for s in crm_eligible_sessions_to_process],
                    "more_sessions_remaining": more_sessions_remaining # Indicate if more cleanup runs are needed
                }
            
            try:
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                cutoff_iso = cutoff_time.isoformat()
                logger.info(f"🕒 Cleanup cutoff time: {cutoff_iso}")
                
                # FIXED: Update SELECT statement to include all new fields for compatibility and apply LIMIT
                cursor = await self._execute_with_socket_retry_async(f"""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, 
                           created_at, last_activity, messages, active, wp_token, 
                           timeout_saved_to_crm, fingerprint_id, fingerprint_method, 
                           visitor_type, daily_question_count, total_question_count, 
                           last_question_time, question_limit_reached, ban_status, 
                           ban_start_time, ban_end_time, ban_reason, evasion_count, 
                           current_penalty_hours, escalation_level, email_addresses_used, 
                           email_switches_count, browser_privacy_level, registration_prompted, 
                           registration_link_clicked, recognition_response, display_message_offset,
                           reverification_pending, pending_user_type, pending_email, pending_full_name,
                           pending_zoho_contact_id, pending_wp_token
                    FROM sessions 
                    WHERE active = 1 
                    AND last_activity < ? 
                    AND timeout_saved_to_crm = 0
                    AND (user_type = 'registered_user' OR user_type = 'email_verified_guest')
                    AND email IS NOT NULL 
                    AND daily_question_count >= 1
                    ORDER BY last_activity ASC -- Process oldest first
                    LIMIT {limit + 1} -- Fetch one more than limit to detect if more are remaining
                """, (cutoff_iso,))
                
                expired_sessions_rows = await asyncio.to_thread(cursor.fetchall)
                
                # Check if there are more sessions than the limit
                more_sessions_remaining = len(expired_sessions_rows) > limit
                sessions_to_process_this_run = expired_sessions_rows[:limit] # Actual sessions for this run
                
                logger.info(f"🔍 Found {len(expired_sessions_rows)} sessions (potential total). Processing {len(sessions_to_process_this_run)} sessions this run (limit {limit}). More remaining: {more_sessions_remaining}")
                
                if not sessions_to_process_this_run:
                    logger.info("✅ No active, expired, unsaved sessions found for background cleanup processing this run")
                    return {
                        "success": True,
                        "cleaned_up_count": 0,
                        "crm_eligible_count": 0,
                        "storage_type": self.db_type,
                        "message": "No eligible sessions found that need processing in this run",
                        "more_sessions_remaining": more_sessions_remaining
                    }
                
                sessions_for_crm_and_update = []
                
                for row in sessions_to_process_this_run:
                    await asyncio.sleep(0) # Yield control to event loop for fairness
                    try:
                        # Safely load the session (using the same logic as load_session for robustness)
                        expected_min_cols = 38 
                        if len(row) < expected_min_cols:
                            logger.warning(f"❌ Skipping session with insufficient columns ({len(row)}) from cleanup query: {row[0][:8]}")
                            continue

                        # Safely access all fields
                        loaded_display_message_offset = row[31] if len(row) > 31 else 0
                        loaded_reverification_pending = bool(row[32]) if len(row) > 32 else False
                        loaded_pending_user_type = UserType(row[33]) if len(row) > 33 and row[33] else None
                        loaded_pending_email = row[34] if len(row) > 34 else None
                        loaded_pending_full_name = row[35] if len(row) > 35 else None
                        loaded_pending_zoho_contact_id = row[36] if len(row) > 36 else None
                        loaded_pending_wp_token = row[37] if len(row) > 37 else None

                        session_obj = UserSession( # Renamed to session_obj to avoid conflict with outer 'session' variable
                            session_id=row[0],
                            user_type=UserType(row[1]) if row[1] else UserType.GUEST,
                            email=row[2],
                            full_name=row[3],
                            zoho_contact_id=row[4],
                            created_at=datetime.fromisoformat(row[5]) if row[5] else datetime.now(),
                            last_activity=datetime.fromisoformat(row[6]) if row[6] else datetime.now(),
                            messages=safe_json_loads(row[7], []),
                            active=bool(row[8]),
                            wp_token=row[9],
                            timeout_saved_to_crm=bool(row[10]),
                            fingerprint_id=row[11],
                            fingerprint_method=row[12],
                            visitor_type=row[13] or 'new_visitor',
                            daily_question_count=row[14] or 0,
                            total_question_count=row[15] or 0,
                            last_question_time=datetime.fromisoformat(row[16]) if row[16] else None,
                            question_limit_reached=bool(row[17]),
                            ban_status=BanStatus(row[18]) if row[18] else BanStatus.NONE,
                            ban_start_time=datetime.fromisoformat(row[19]) if row[19] else None,
                            ban_end_time=datetime.fromisoformat(row[20]) if row[20] else None, # FIXED: Ensure this is datetime object for UserSession
                            ban_reason=row[21],
                            evasion_count=row[22] or 0,
                            current_penalty_hours=row[23] or 0,
                            escalation_level=row[24] or 0,
                            email_addresses_used=safe_json_loads(row[25], []),
                            email_switches_count=row[26] or 0,
                            browser_privacy_level=row[27],
                            registration_prompted=bool(row[28]),
                            registration_link_clicked=bool(row[29]),
                            recognition_response=row[30],
                            display_message_offset=loaded_display_message_offset,
                            reverification_pending=loaded_reverification_pending,
                            pending_user_type=loaded_pending_user_type,
                            pending_email=loaded_pending_email,
                            pending_full_name=loaded_pending_full_name,
                            pending_zoho_contact_id=loaded_pending_zoho_contact_id,
                            pending_wp_token=loaded_pending_wp_token
                        )
                        sessions_for_crm_and_update.append(session_obj)
                        logger.debug(f"📝 Session {session_obj.session_id[:8]} added for CRM save processing in cleanup")
                        
                    except Exception as session_error:
                        logger.error(f"❌ Error processing session row for cleanup: {session_error}", exc_info=True)
                        continue
                
                crm_saved_count = 0
                crm_failed_count = 0
                
                for session_to_process in sessions_for_crm_and_update:
                    await asyncio.sleep(0) # Yield control to event loop for fairness
                    try:
                        logger.info(f"🔄 Background cleanup processing CRM for session {session_to_process.session_id[:8]}...")
                        
                        # Attempt to save to CRM (this is the "retry" attempt)
                        # Ensure zoho_manager is available (initialized in startup)
                        if zoho_manager is None:
                            raise RuntimeError("ZohoCRMManager is not initialized.")

                        save_result = await zoho_manager.save_chat_transcript_sync(session_to_process, "Automated Session Timeout Cleanup")
                        
                        if save_result.get("success"):
                            session_to_process.timeout_saved_to_crm = True
                            session_to_process.active = False # Mark inactive after successful save
                            logger.info(f"✅ Background CRM cleanup save SUCCESS for {session_to_process.session_id[:8]} - Marked INACTIVE")
                            crm_saved_count += 1
                        else:
                            # Even if CRM save fails now, mark as inactive to prevent endless retries
                            session_to_process.active = False
                            logger.error(f"❌ Background CRM cleanup save FAILED for {session_to_process.session_id[:8]} - Marking INACTIVE anyway")
                            crm_failed_count += 1
                        
                        session_to_process.last_activity = datetime.now() # Update last activity before final save
                        
                        # If Zoho returned a contact_id and it's not already set in session, update it
                        if save_result.get("contact_id") and not session_to_process.zoho_contact_id:
                            session_to_process.zoho_contact_id = save_result["contact_id"]
                            logger.info(f"🔗 Background CRM - Saved contact ID {save_result['contact_id']} to session {session_to_process.session_id[:8]}.")

                        await self.save_session(session_to_process) # Save final state (active=0)
                        
                        logger.info(f"📊 Cleanup Session State Final: {session_to_process.session_id[:8]} - Active: {session_to_process.active}, Saved: {session_to_process.timeout_saved_to_crm}, Contact: {session_to_process.zoho_contact_id}")
                        
                    except Exception as e:
                        crm_failed_count += 1
                        logger.critical(f"❌ Critical error in background CRM processing for session {session_to_process.session_id[:8]}: {e}", exc_info=True)
                        # Ensure session is marked inactive even on critical processing error
                        try:
                            session_to_process.active = False
                            session_to_process.last_activity = datetime.now()
                            await self.save_session(session_to_process)
                            logger.info(f"🔒 Background CRM - Session {session_to_process.session_id[:8]} marked as INACTIVE after critical error.")
                        except Exception as fallback_error:
                            logger.critical(f"❌ Failed to mark session inactive even after critical error: {fallback_error}")
                
                logger.info(f"✅ Background CRM processing completed: {len(sessions_for_crm_and_update)} sessions processed, {crm_saved_count} saved to CRM, {crm_failed_count} failed")
                
                logger.info("✅ Background cleanup completed successfully with INDIVIDUAL CRM PROCESSING")
                
                return {
                    "success": True,
                    "cleaned_up_count": len(sessions_for_crm_and_update),
                    "crm_eligible_count": len(sessions_for_crm_and_update), # Corrected as all were eligible and processed
                    "rows_affected_inactive": crm_saved_count + crm_failed_count, # Number of sessions whose status we tried to finalize
                    "storage_type": self.db_type,
                    "crm_saved_count": crm_saved_count,
                    "crm_failed_count": crm_failed_count,
                    "cutoff_time": cutoff_iso,
                    "more_sessions_remaining": more_sessions_remaining # Indicate if more cleanup runs are needed
                }
                        
            except Exception as e:
                logger.error(f"❌ Failed to cleanup expired sessions: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": str(e),
                    "storage_type": self.db_type,
                    "message": "Cleanup failed due to database error",
                    "more_sessions_remaining": True # Assume more if cleanup failed partway
                }

# PDF Exporter (now async/thread-pooled)
class PDFExporter:
    def __init__(self):
        self.styles = getSampleStyleSheet()
        # Removed the redundant additions for 'Heading1' and 'Normal' as they are already
        # provided by getSampleStyleSheet() and caused a KeyError.
        # self.styles.add(ParagraphStyle(name='Heading1', fontSize=18, leading=22, spaceAfter=12))
        # self.styles.add(ParagraphStyle(name='Normal', fontSize=10, leading=14, spaceAfter=6))
        self.styles.add(ParagraphStyle(name='UserMessage', backColor=lightgrey, fontSize=10, leading=14, spaceAfter=6))

    async def generate_chat_pdf(self, session: UserSession) -> Optional[io.BytesIO]:
        """Generates a PDF of the chat transcript, running the blocking build in a thread."""
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter)
            story = [Paragraph("FiFi AI Emergency Save Transcript", self.styles['Heading1'])]
            
            story.append(Paragraph(f"Session ID: {session.session_id}", self.styles['Normal']))
            story.append(Paragraph(f"User: {session.full_name or 'Anonymous'} ({session.email or 'No email'})", self.styles['Normal']))
            story.append(Paragraph(f"Created: {session.created_at.strftime('%Y-%m-%d %H:%M:%S')}", self.styles['Normal']))
            story.append(Spacer(1, 12))
            
            for i, msg in enumerate(session.messages):
                role = str(msg.get('role', 'unknown')).capitalize()
                content = html.escape(str(msg.get('content', '')))
                content = re.sub(r'<[^>]+>', '', content)
                
                # REMOVED TRUNCATION: This line was causing the PDF content to be truncated.
                # if len(content) > 200: # Truncate for summary in PDF as well if too long
                #     content = content[:200] + "..."
                    
                style = self.styles['UserMessage'] if role == 'User' else self.styles['Normal']
                story.append(Spacer(1, 8))
                story.append(Paragraph(f"<b>{role}:</b> {content}", style))
            
            await asyncio.to_thread(doc.build, story)
            
            buffer.seek(0)
            return buffer
        except Exception as e:
            logger.error(f"❌ PDF generation failed: {e}", exc_info=True)
            return None

# Zoho CRM Manager (now asynchronous with httpx)
class ZohoCRMManager:
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token = None
        self._token_expiry = None
        self._http_client = httpx.AsyncClient(timeout=30) # Default timeout for the client

    async def close_http_client(self):
        if self._http_client:
            await self._http_client.aclose()
            logger.info("✅ httpx.AsyncClient for ZohoCRMManager closed.")

    async def _get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        if not ZOHO_ENABLED:
            logger.debug("Zoho is not enabled. Skipping access token request.")
            return None

        if not force_refresh and self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            logger.debug("Using cached Zoho access token.")
            return self._access_token
        
        logger.info("🔑 Requesting new Zoho access token...")
        try:
            response = await self._http_client.post(
                "https://accounts.zoho.com/oauth/v2/token",
                data={
                    'refresh_token': ZOHO_REFRESH_TOKEN,
                    'client_id': ZOHO_CLIENT_ID,
                    'client_secret': ZOHO_CLIENT_SECRET,
                    'grant_type': 'refresh_token'
                },
                timeout=15 # Specific timeout for token request
            )
            response.raise_for_status()
            data = response.json()
            
            self._access_token = data.get('access_token')
            self._token_expiry = datetime.now() + timedelta(minutes=50)
            logger.info("✅ Successfully obtained Zoho access token.")
            return self._access_token
        except httpx.TimeoutException:
            logger.error("⏰ Zoho token request timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get Zoho access token: {e}", exc_info=True)
            return None

    async def _find_contact_by_email(self, email: str) -> Optional[str]:
        access_token = await self._get_access_token()
        if not access_token:
            return None
        
        logger.debug(f"🔍 Searching Zoho for contact with email: {email}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
            params = {'criteria': f'(Email:equals:{email})'}
            response = await self._http_client.get(f"{self.base_url}/Contacts/search", headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                # FIXED: Access the first element of the 'data' list, then 'id'
                contact_id = data['data'][0]['id'] 
                logger.info(f"✅ Found existing Zoho contact: {contact_id}")
                return contact_id
            logger.debug(f"❌ No Zoho contact found for email: {email}")
            return None
        except Exception as e:
            logger.error(f"❌ Error finding contact by email {email}: {e}", exc_info=True)
        return None

    async def _create_contact(self, email: str, full_name: Optional[str]) -> Optional[str]:
        access_token = await self._get_access_token()
        if not access_token:
            return None

        logger.info(f"👤 Creating new Zoho contact for email: {email}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}', 'Content-Type': 'application/json'}
            contact_data = {
                "data": [{
                    "Last_Name": full_name or "Food Professional",
                    "Email": email,
                    "Lead_Source": "FiFi AI Emergency API"
                }]
            }
            response = await self._http_client.post(f"{self.base_url}/Contacts", headers=headers, json=contact_data, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data'] and data['data'][0]['code'] == 'SUCCESS':
                # FIXED: Access the first element of the 'data' list, then 'details', then 'id'
                contact_id = data['data'][0]['details']['id']
                logger.info(f"✅ Created new Zoho contact: {contact_id}")
                return contact_id
            
            logger.error(f"❌ Zoho contact creation failed with response: {data}")
            return None
        except Exception as e:
            logger.error(f"❌ Error creating contact for {email}: {e}", exc_info=True)
        return None

    async def _add_note(self, contact_id: str, note_title: str, note_content: str) -> bool:
        access_token = await self._get_access_token()
        if not access_token:
            return False

        logger.info(f"📝 Adding note '{note_title}' to Zoho contact {contact_id}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}', 'Content-Type': 'application/json'}
            
            # This truncation remains due to Zoho API's 32,000 character limit for Note_Content
            if len(note_content) > 32000:
                logger.warning(f"⚠️ Note content for {contact_id} exceeds 32000 chars. Truncating.")
                note_content = note_content[:32000 - 100] + "\n\n[Content truncated due to size limits]" 
            
            note_data = {
                "data": [{
                    "Note_Title": note_title,
                    "Note_Content": note_content,
                    "Parent_Id": contact_id,  # Correctly passed as string ID
                    "se_module": "Contacts"
                }]
            }
            response = await self._http_client.post(f"{self.base_url}/Notes", headers=headers, json=note_data, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data'] and data['data'][0]['code'] == 'SUCCESS':
                logger.info(f"✅ Successfully added Zoho note: {note_title}")
                return True
            logger.error(f"❌ Zoho note creation failed with response: {data}")
            return False
        except Exception as e:
            logger.error(f"❌ Error adding note '{note_title}' to Zoho contact {contact_id}: {e}", exc_info=True)
            return False

    async def _upload_attachment(self, contact_id: str, pdf_buffer: io.BytesIO, filename: str) -> bool:
        access_token = await self._get_access_token()
        if not access_token:
            return False

        logger.info(f"📎 Adding PDF attachment '{filename}' to Zoho contact {contact_id}")
        
        upload_url = f"{self.base_url}/Contacts/{contact_id}/Attachments"
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
                
                pdf_buffer.seek(0)
                pdf_content = await asyncio.to_thread(pdf_buffer.read)
                
                response = await self._http_client.post(
                    upload_url, 
                    headers=headers, 
                    files={'file': (filename, pdf_content, 'application/pdf')},
                    timeout=60
                )
                
                if response.status_code == 401:
                    logger.warning("Zoho token expired during upload, attempting refresh...")
                    access_token = await self._get_access_token(force_refresh=True)
                    if not access_token: 
                        return False
                    headers['Authorization'] = f'Zoho-oauthtoken {access_token}'
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if 'data' in data and data['data'] and data['data'][0]['code'] == 'SUCCESS':
                    logger.info(f"✅ Successfully added PDF attachment: {filename}")
                    return True
                else:
                    logger.error(f"❌ PDF attachment creation failed with response: {data}")
                    
            except httpx.TimeoutException:
                logger.error(f"⏰ Zoho upload timeout (attempt {attempt + 1}/{max_retries})")
            except Exception as e:
                logger.error(f"❌ Error adding PDF attachment (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                
        return False

    async def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> Dict[str, Any]:
        """Now async internally, but named sync for compatibility (could rename if desired)."""
        if not ZOHO_ENABLED:
            logger.info("ℹ️ Zoho is not enabled. Skipping Zoho CRM save.")
            return {"success": False, "reason": "zoho_disabled"}
            
        if not session.email:
            logger.info(f"ℹ️ Session {session.session_id[:8]} has no email. Skipping Zoho CRM save.")
            return {"success": False, "reason": "no_email"}
            
        if not session.messages:
            logger.info(f"ℹ️ Session {session.session_id[:8]} has no messages. Skipping Zoho CRM save.")
            return {"success": False, "reason": "no_messages"}
        
        try:
            logger.info(f"🔄 Starting Zoho CRM save for session {session.session_id[:8]} (Reason: {trigger_reason})")
            
            contact_id = await self._find_contact_by_email(session.email)
            if not contact_id:
                contact_id = await self._create_contact(session.email, session.full_name)
            if not contact_id:
                logger.error(f"❌ Failed to find or create Zoho contact for {session.email}. Aborting CRM save.")
                return {"success": False, "reason": "contact_creation_failed"}

            if not session.zoho_contact_id:
                session.zoho_contact_id = contact_id
                logger.info(f"🔗 Updated session {session.session_id[:8]} with Zoho contact ID: {contact_id}")

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            note_title = f"FiFi AI Emergency Save - {timestamp} ({trigger_reason})"
            
            note_content = f"**Emergency Save Information:**\n"
            note_content += f"- Session ID: {session.session_id}\n"
            note_content += f"- User: {session.full_name or 'Unknown'} ({session.email})\n"
            note_content += f"- User Type: {session.user_type.value}\n"
            note_content += f"- Save Trigger: {trigger_reason}\n"
            note_content += f"- Timestamp: {timestamp}\n"
            note_content += f"- Total Messages: {len(session.messages)}\n"
            note_content += f"- Questions Asked (Daily Count): {session.daily_question_count}\n\n"
            note_content += "**Conversation Summary (see PDF attachment for full details):**\n"
            
            for i, msg in enumerate(session.messages):
                role = msg.get("role", "Unknown").capitalize()
                content = re.sub(r'<[^>]+>', '', msg.get("content", ""))
                
                # REMOVED TRUNCATION FOR NOTES CONTENT
                # if len(content) > 200:
                #     content = content[:200] + "..."
                    
                note_content += f"\n{i+1}. **{role}:** {content}\n"
                
            note_success = await self._add_note(contact_id, note_title, note_content)
            
            pdf_success = False
            pdf_buffer = await self.pdf_exporter.generate_chat_pdf(session)
            if pdf_buffer:
                pdf_filename = f"FiFi_Chat_Transcript_{session.session_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                pdf_success = await self._upload_attachment(contact_id, pdf_buffer, pdf_filename)
                pdf_buffer.close()
            else:
                logger.warning(f"⚠️ Failed to generate PDF for session {session.session_id[:8]}")
                
            if note_success:
                logger.info(f"✅ Zoho CRM save successful for session {session.session_id[:8]} (Note: {note_success}, PDF: {pdf_success})")
                return {
                    "success": True, 
                    "contact_id": contact_id,
                    "note_created": note_success,
                    "pdf_attached": pdf_success
                }
            else:
                logger.error(f"❌ Zoho note creation failed for session {session.session_id[:8]}.")
                return {"success": False, "reason": "note_creation_failed"}
                
        except Exception as e:
            logger.error(f"❌ Emergency CRM save process failed for session {session.session_id[:8]}: {e}", exc_info=True)
            return {"success": False, "reason": "exception", "error": str(e)}

# Declare managers globally as None, to be initialized in startup
db_manager: Optional[ResilientDatabaseManager] = None
pdf_exporter: Optional[PDFExporter] = None
zoho_manager: Optional[ZohoCRMManager] = None

# NEW: FastAPI shutdown event to close the httpx client
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("👋 Shutting down FastAPI application. Closing httpx client...")
    if zoho_manager:
        await zoho_manager.close_http_client()
    if db_manager and db_manager.conn:
        try:
            await asyncio.to_thread(db_manager.conn.close)
            logger.info("✅ Database connection closed.")
        except Exception as e:
            logger.error(f"Error closing database connection during shutdown: {e}")
    logger.info("✅ FastAPI shutdown complete.")

# Helper functions (unchanged)
def is_crm_eligible(session: UserSession, is_emergency_save: bool = False) -> bool:
    try:
        if not session.email or not session.messages:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Missing email ({bool(session.email)}) or messages ({bool(session.messages)})")
            return False
        
        if session.user_type not in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: User type {session.user_type.value} not eligible.")
            return False
        
        if session.daily_question_count < 1:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: No questions asked ({session.daily_question_count}).")
            return False
        
        if not is_emergency_save:
            # Note: last_activity can be None from DB if session is brand new and not active yet
            start_time = session.created_at
            if session.last_question_time and session.last_question_time < start_time:
                start_time = session.last_question_time
            elif session.last_activity and session.last_activity > start_time: # Use actual last_activity if more recent
                start_time = session.last_activity
            
            elapsed_time = datetime.now() - start_time
            elapsed_minutes = elapsed_time.total_seconds() / 60
            
            if elapsed_minutes < 5.0:
                logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Less than 5 minutes active ({elapsed_minutes:.1f} min).")
                return False
            
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: All checks passed. UserType={session.user_type.value}, Questions={session.daily_question_count}, Elapsed={elapsed_minutes:.1f}min.")
        else:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Emergency save - bypassing time requirement. UserType={session.user_type.value}, Questions={session.daily_question_count}.")
            
        return True
    except Exception as e:
        logger.error(f"❌ Error checking CRM eligibility for session {session.session_id[:8]}: {e}", exc_info=True)
        return False

# Background tasks (COMPLETELY IMPLEMENTED, now async)
async def _perform_emergency_crm_save(session_id: str, reason: str):
    """
    Performs an emergency CRM save in background.
    Loads session, checks eligibility, then attempts CRM save, and marks session active=False.
    """
    # Ensure managers are initialized before attempting to use them in a background task
    if db_manager is None or zoho_manager is None:
        logger.critical(f"❌ Managers not initialized for background task {session_id[:8]}. Aborting save.")
        return

    try:
        logger.info(f"🔄 Background CRM save task starting for session {session_id[:8]} (Reason: {reason})")
        
        is_session_ending = is_session_ending_reason(reason)
        logger.info(f"📋 Session ending check: {is_session_ending} for reason '{reason}'")

        session = await db_manager.load_session(session_id)
        if not session:
            logger.error(f"❌ Background save: Session {session_id[:8]} not found or not active. Cannot save to CRM.")
            return
        
        if not is_crm_eligible(session, is_emergency_save=True):
            logger.info(f"ℹ️ Background save: Session {session.session_id[:8]} not eligible for CRM save (reason: {reason}). Skipping.")
            return

        if session.timeout_saved_to_crm:
            logger.info(f"ℹ️ Background save: Session {session.session_id[:8]} already marked as saved to CRM. Skipping duplicate save.")
            return

        save_result = await zoho_manager.save_chat_transcript_sync(session, reason)
        
        if save_result.get("success"):
            session.timeout_saved_to_crm = True # Mark as saved by timeout/emergency
            if is_session_ending:
                session.active = False
                logger.info(f"✅ CRM save SUCCESS for {session.session_id[:8]} (Reason: {reason}) - Session marked INACTIVE")
            else:
                logger.info(f"✅ CRM save SUCCESS for {session.session_id[:8]} (Reason: {reason}) - Session remains ACTIVE (not session-ending reason)")
        else:
            if is_session_ending:
                logger.warning(f"⚠️ CRM save FAILED for {session.session_id[:8]} (Reason: {reason}) - Marking session as INACTIVE regardless (final attempt on cleanup side)")
                session.active = False # Force inactive even if save failed on final attempt
            else:
                logger.warning(f"⚠️ CRM save FAILED for {session.session_id[:8]} (Reason: {reason}) - Session remains ACTIVE (not session-ending reason), cleanup will retry later.")
            # session.active remains True - DO NOT set to False here if it's not a session-ending reason.
            # The full cleanup task is the ultimate arbiter for truly inactive sessions.
        
        # Update last_activity and save session state
        session.last_activity = datetime.now()
        
        # If Zoho returned a contact_id and it's not already set in session, update it
        if save_result.get("contact_id") and not session.zoho_contact_id:
            session.zoho_contact_id = save_result["contact_id"]
            logger.info(f"🔗 Background CRM - Saved contact ID {save_result['contact_id']} to session {session.session_id[:8]}.")

        await db_manager.save_session(session)
        logger.info(f"✅ Background CRM save task finished for session {session.session_id[:8]} (PDF: {save_result.get('pdf_attached', False)}, Active: {session.active})")
            
    except Exception as e:
        logger.critical(f"❌ Critical error in background CRM save task for session {session_id[:8]} (Reason: {reason}): {e}", exc_info=True)
        # On unexpected exception during save:
        # Load fresh session to avoid overwriting with potentially stale object if original task crashed early
        try:
            # Re-load the session to ensure we're updating the latest state in DB
            reloaded_session = await db_manager.load_session(session_id)
            if reloaded_session:
                reloaded_session.last_activity = datetime.now() # Update activity
                if is_session_ending_reason(reason):
                    reloaded_session.active = False # Mark inactive if it was a session-ending reason
                    logger.warning(f"⚠️ Exception during save - Session {session_id[:8]} marked INACTIVE to prevent endless processing.")
                else:
                    logger.warning(f"⚠️ Exception during save - Session {session_id[:8]} remains ACTIVE; cleanup will re-attempt later.")
                await db_manager.save_session(reloaded_session)
            else:
                logger.error(f"❌ Could not reload session {session_id[:8]} to update after background save exception.")
        except Exception as fallback_error:
            logger.critical(f"❌ Failed to save session state after emergency save exception: {fallback_error}", exc_info=True)


async def _perform_full_cleanup_in_background():
    """COMPLETELY IMPLEMENTED: Full cleanup logic with INDIVIDUAL CRM PROCESSING"""
    # Ensure managers are initialized before attempting to use them in a background task
    if db_manager is None or zoho_manager is None:
        logger.critical("❌ Managers not initialized for full cleanup task. Aborting.")
        return

    try:
        logger.info("🔄 Background FULL CLEANUP task starting (Async version)...")
        
        quick_status = await db_manager.quick_status_check()
        logger.info(f"📊 Background cleanup - Quick database status: {quick_status.get('status', 'unknown')}")
        
        # Pass expiry_minutes=5 as confirmed
        cleanup_result = await db_manager.cleanup_expired_sessions(expiry_minutes=5)
        
        if not cleanup_result.get("success"):
            logger.error(f"❌ Background cleanup - Database cleanup failed: {cleanup_result}")
            return
        
        logger.info(f"✅ Background cleanup - Successfully processed {cleanup_result.get('cleaned_up_count', 0)} expired sessions")
        
    except Exception as e:
        logger.critical(f"❌ Critical error in background cleanup: {e}", exc_info=True)

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "FiFi Emergency API - Async Operations Enabled",
        "status": "running",
        "version": "3.6.0-final-fix-v4", # Version bumped
        "critical_fix": "Blocking I/O operations moved to async or thread pool and endpoint logic refactored",
        "compatibility": "100% compatible with fifi.py database schema and UserSession structure",
        "fixes_applied": [
            "CRITICAL: All database operations (sqlitecloud/sqlite3) now use asyncio.to_thread",
            "CRITICAL: All external HTTP requests (Zoho) now use httpx.AsyncClient (long-lived instance)",
            "CRITICAL: PDF generation (reportlab) now uses asyncio.to_thread",
            "FIXED: All time.sleep replaced with await asyncio.sleep",
            "FIXED: `httpx.AsyncClient` used correctly (single instance per manager)",
            "FIXED: Newline characters (`\n`) corrected to standard `\n` in string literals for Zoho notes",
            "FIXED: `safe_json_loads` initial check from `===` to `is None` or `==`",
            "FIXED: `UserSession.ban_end_time` type hint consistency for loading",
            "FIXED: Zoho Note `MANDATORY_NOT_FOUND` by changing `Parent_Id` format to string ID.",
            "CRITICAL: `/emergency-save` endpoint refactored to be truly non-blocking, moving ALL DB/CRM/eligibility checks to background task.",
            "OPTIMIZED: Background tasks are now truly non-blocking for the event loop", 
            "PRESERVED: All working CRM functionality with PDF attachments",
            "PRESERVED: Socket error resilience and auto-recovery",
            "PRESERVED: fifi.py compatibility (display_message_offset field)",
            "OPTIMIZED: Reduced retry counts for faster background processing",
            "CRITICAL FIX: Managers (db_manager, zoho_manager, pdf_exporter) now initialized in `on_event('startup')` instead of global scope, resolving cold start 504 timeouts.",
            "FURTHER OPTIMIZED: Startup `db_manager.test_connection()` is now `asyncio.wait_for` with a hard timeout, preventing startup from hanging.",
            "ENHANCED: More robust session loading/saving in `ResilientDatabaseManager` with explicit timeouts for each step.",
            "CRITICAL FIX: `UserSession` dataclass and corresponding DB `SELECT`/`INSERT`/`REPLACE` queries fully aligned with `fifi.py`'s latest schema (including `reverification_pending` fields).",
            "CRITICAL FIX: `fetchone()` comparison bug (`test_result_fetched == 1` changed to `test_result_fetched[0] == 1`).",
            "FIXED: Zoho CRM `_find_contact_by_email` and `_create_contact` response parsing for `data['data']` which is a list.",
            "SYNTAX ERROR FIX: Fixed orphaned else block in cleanup_expired_sessions method",
            "FIXED: PDFExporter: Corrected `SimpleDocDocument` to `SimpleDocTemplate`",
            "FIXED: `KeyError: \"Style 'Heading1' already defined in stylesheet\"` by removing redundant style additions in `PDFExporter`'s `__init__`.",
            "FIXED: `no such table: sessions` error by ensuring schema initialization is attempted when a new persistent database connection is established.",
            "FIXED: `connect() got an unexpected keyword argument 'timeout'` by removing the `timeout` parameter from `sqlitecloud.connect` calls to align with `fifi.py`.",
            "ENHANCED: More verbose logging for SQLite Cloud connection failures in `_ensure_connection_with_timeout`.",
            "CRITICAL FIX: Schema initialization now correctly handles `sqlitecloud.exceptions.SQLiteCloudOperationalError` for 'duplicate column name' errors, preventing fallback to memory.",
            "CRITICAL FIX: Removed message content truncation in PDF generation for full chat transcripts.",
            "CRITICAL FIX: Removed individual message content truncation within Zoho Notes content generation for full detail (Zoho API's 32k character limit for entire note remains)." # NEW FIX
        ],
        "cleanup_logic_complete": {
            "5_minute_timeout_check": "IMPLEMENTED - Sessions inactive for 5+ minutes are processed",
            "active_session_filter": "IMPLEMENTED - Only processes sessions where active = 1 (for first pass)",
            "crm_save_filter": "IMPLEMENTED - Only processes sessions where timeout_saved_to_crm = 0",
            "crm_eligibility_rules": "IMPLEMENTED - Registered/verified users with email, messages, and 1+ questions",
            "session_marking": "IMPLEMENTED - Marks processed sessions as active = 0 (after successful cleanup save)",
            "background_processing": "OPTIMIZED - All heavy work done after endpoint response asynchronously",
            "memory_fallback": "IMPLEMENTED - Graceful fallback when database unavailable",
            "failed_beacon_retry": "IMPLEMENTED - Sessions stay active if beacon save fails for cleanup retry"
        },
        "timeout_optimizations": {
            "connection_timeouts": "30 seconds maximum for initialization",
            "background_task_timeout": "15 seconds for background operations", 
            "retry_reduction": "Maximum 2-3 attempts vs previous 5",
            "quick_status_checks": "Available without full database initialization",
            "memory_mode_fallback": "Instant fallback when connections fail",
            "main_endpoint_response_time": "Near-instant for /emergency-save"
        },
        "timestamp": datetime.now()
    }

@app.get("/health")
async def health_check():
    # Check if managers are initialized. If not, means app is not fully up yet.
    if db_manager is None or zoho_manager is None:
        return {
            "status": "initializing",
            "message": "Application managers are still initializing. Please wait.",
            "timestamp": datetime.now()
        }

    try:
        quick_status = await db_manager.quick_status_check()
        
        if quick_status.get("status") != "quick_check_failed":
            db_status = quick_status
        else:
            db_status = await db_manager.test_connection()
            
        return {
            "status": "healthy",
            "timestamp": datetime.now(),
            "database": db_status,
            "zoho": "enabled" if ZOHO_ENABLED else "disabled",
            "sqlitecloud_sdk": "available" if SQLITECLOUD_AVAILABLE else "not_available",
            "timeout_fix": {
                "lazy_initialization": "active",
                "blocking_startup_operations": "eliminated",
                "options_preflight": "optimized",
                "background_processing": "async_non_blocking",
                "quick_status_available": True
            }
        }
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}", exc_info=True)
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(),
            "error": str(e)
        }

@app.get("/diagnostics")
async def comprehensive_diagnostics():
    # Check if managers are initialized. If not, means app is not fully up yet.
    if db_manager is None or zoho_manager is None:
        return {
            "status": "initializing",
            "message": "Application managers are still initializing. Please wait.",
            "timestamp": datetime.now()
        }

    try:
        diagnostics = {
            "timestamp": datetime.now(),
            "version": "3.6.0-final-fix-v4", # Version bumped
            "timeout_fix_status": {
                "lazy_database_initialization": "active_with_timeouts_async",
                "blocking_startup_eliminated": True,
                "options_response_optimized": True,
                "background_task_timeouts": "implemented",
                "quick_status_checks": True,
                "expected_cold_start_time": "< 5 seconds (after this change)",
                "emergency_save_endpoint_blocking_status": "Non-blocking (all logic in background)"
            },
            "cleanup_endpoint_status": {
                "endpoint_available": True,
                "background_processing": "non_blocking_async",
                "5_minute_timeout_logic": "COMPLETELY_IMPLEMENTED",
                "crm_eligibility_rules": "COMPLETELY_IMPLEMENTED",
                "session_marking_logic": "COMPLETELY_IMPLEMENTED",
                "memory_mode_support": True,
                "timeout_protection": True
            },
            "fifi_compatibility": {
                "database_schema_compatible": True,
                "session_structure_compatible": True,
                "display_message_offset_support": True,
                "emergency_save_protocol_match": True,
                "backward_compatibility": "Handles both 31 and 32 column databases"
            },
            "performance_optimizations": {
                "connection_timeouts": "30s max for initialization",
                "background_timeouts": "15s for background ops",
                "retry_counts_reduced": "2-3 attempts max",
                "memory_fallback": "immediate when connections fail",
                "quick_status_available": True
            },
            "environment": {
                "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING",
                "ZOHO_ENABLED": ZOHO_ENABLED,
                "SQLITECLOUD_AVAILABLE": SQLITECLOUD_AVAILABLE
            }
        }
        
        quick_status = await db_manager.quick_status_check()
        diagnostics["quick_database_status"] = quick_status
        
        return diagnostics
        
    except Exception as e:
        logger.error(f"❌ Comprehensive diagnostics failed: {e}", exc_info=True)
        return {
            "timestamp": datetime.now(),
            "error": str(e),
            "status": "diagnostics_failed"
        }

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions(background_tasks: BackgroundTasks):
    """COMPLETELY FIXED: Ultra-fast cleanup endpoint with full background implementation"""
    try:
        logger.info("🧹 ULTRA-FAST CLEANUP: Starting immediate response with complete background processing")
        
        # Check if managers are initialized before queuing the task
        if db_manager is None or zoho_manager is None:
            logger.warning("⚠️ Cleanup request received, but managers not fully initialized. Skipping task.")
            # Still return 200 OK to Cloud Scheduler, but indicate internal state
            return {
                "success": False,
                "message": "Cleanup managers not yet initialized. Will retry on next schedule.",
                "queued_for_background_processing": False,
                "status": "initialization_pending",
                "timestamp": datetime.now(),
            }

        background_tasks.add_task(_perform_full_cleanup_in_background)
        
        return {
            "success": True,
            "message": "Complete cleanup queued for background processing",
            "queued_for_background_processing": True,
            "implementation_status": "COMPLETE",
            "features": [
                "5-minute timeout detection",
                "CRM eligibility checking", 
                "Active session marking (active = 0)",
                "Background CRM processing",
                "Memory mode fallback support",
                "Timeout protection on all operations",
                "Retries for sessions with failed beacon saves"
            ],
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"❌ Cleanup endpoint error: {e}")
        return {
            "success": False,
            "message": "Cleanup failed to queue due to internal error",
            "error": str(e),
            "timestamp": datetime.now()
        }

@app.post("/emergency-save")
async def emergency_save(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    """
    CRITICAL FIX: Emergency save endpoint now truly non-blocking.
    All potentially long-running database loading and CRM logic is moved to a background task.
    The endpoint returns immediately.
    """
    try:
        logger.info(f"🚨 EMERGENCY SAVE ENDPOINT: Request received for session {request.session_id[:8]}, reason: {request.reason}")
        
        # Check if managers are initialized before queuing the task
        if db_manager is None or zoho_manager is None:
            logger.warning(f"⚠️ Emergency save request received for {request.session_id[:8]}, but managers not fully initialized. Returning OK, but save will be skipped.")
            # Return OK to the client as sendBeacon expects immediate response, but log the issue.
            return {
                "success": False, # Mark as False because the save won't actually happen now
                "message": "Emergency save managers not yet initialized. Try again later.",
                "session_id": request.session_id,
                "reason": request.reason,
                "queued_for_background_processing": False,
                "status": "initialization_pending",
                "timestamp": datetime.now()
            }

        # All heavy lifting (DB load, eligibility check, CRM save) is moved to background task.
        # This endpoint just validates input and queues the task.
        background_tasks.add_task(
            _perform_emergency_crm_save,
            request.session_id,
            f"Async Emergency Save: {request.reason}"
        )
        
        is_session_ending = is_session_ending_reason(request.reason)
        
        logger.info(f"✅ Emergency save queued successfully for {request.session_id[:8]} - Returning immediate response.")
        return {
            "success": True,
            "message": f"Emergency save queued successfully ({'session will be processed by backend and marked closed if save succeeds' if is_session_ending else 'session processing in background; remains active if not session-ending'})",
            "session_id": request.session_id,
            "reason": request.reason,
            "queued_for_background_processing": True,
            "session_ending_reason": is_session_ending, # Clarify if the reason implies session end
            "timestamp": datetime.now(),
            "ultimate_fix": {
                "timeout_protection": "active_on_endpoint",
                "background_processing": "optimized_async_all_logic",
                "504_timeout_resolved": True, # Should resolve 504 issues now
                "immediate_response": True
            }
        }
            
    except Exception as e:
        logger.critical(f"❌ Critical error in emergency_save endpoint for session {request.session_id[:8]}: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Internal server error while queuing emergency save: {str(e)}",
            "session_id": request.session_id,
            "reason": "internal_error_on_queue",
            "timestamp": datetime.now()
        }

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting FiFi Emergency API - ASYNC OPERATIONS ENABLED...")
    logger.info("🔑 Features: Async DB/API calls + Complete Cleanup Logic + Timeout Protection + Working CRM + PDF Attachments + fifi.py Schema")
    uvicorn.run(app, host="0.0.0.0", port=8000)
