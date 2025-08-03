from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import json
import sqlite3
import threading
import copy
import requests
import time
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="FiFi Emergency API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Configuration from environment variables
SQLITE_CLOUD_CONNECTION = os.getenv("SQLITE_CLOUD_CONNECTION")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ENABLED = all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN])

logger.info(f"🔧 CONFIG CHECK:")
logger.info(f"SQLite Cloud: {'SET' if SQLITE_CLOUD_CONNECTION else 'MISSING'}")
logger.info(f"Zoho Enabled: {ZOHO_ENABLED}")

# Graceful fallback for optional imports
SQLITECLOUD_AVAILABLE = False
try:
    import sqlitecloud
    SQLITECLOUD_AVAILABLE = True
    logger.info("✅ sqlitecloud SDK detected and available.")
except ImportError:
    logger.warning("❌ SQLiteCloud SDK not available. Emergency beacon will use local SQLite fallback.")

# Models
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
    ban_end_time: Optional[datetime] = None
    ban_reason: Optional[str] = None
    evasion_count: int = 0
    current_penalty_hours: int = 0
    escalation_level: int = 0
    email_addresses_used: List[str] = field(default_factory=list)
    email_switches_count: int = 0
    browser_privacy_level: Optional[str] = None
    registration_prompted: bool = False
    registration_link_clicked: bool = False

# Utility functions
def safe_json_loads(data: Optional[str], default_value: Any = None) -> Any:
    if data is None or data == "":
        return default_value
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Failed to decode JSON data: {str(data)[:100]}...")
        return default_value

# Enhanced Database Manager with robust SQLite Cloud handling
class DatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock()
        self.conn = None
        self.connection_string = connection_string
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=5)
        self._connection_attempts = 0
        self._max_connection_attempts = 3
        
        logger.info("🔄 INITIALIZING ENHANCED DATABASE MANAGER")
        
        self.db_type = "memory"  # Default to memory initially
        self.local_sessions = {}  # Ensure this is always initialized

        # Detailed connection attempt logging
        if connection_string and SQLITECLOUD_AVAILABLE:
            logger.info(f"Attempting SQLite Cloud connection with string: {connection_string[:20]}...")
            self._attempt_cloud_connection()
        elif connection_string:
            logger.error("❌ SQLite Cloud connection string provided but sqlitecloud library is not available.")
        else:
            logger.info("ℹ️ No SQLite Cloud connection string provided, skipping cloud connection attempt.")
        
        # Fallback to local SQLite if cloud connection failed
        if not self.conn:
            logger.info("☁️ Cloud connection failed or not attempted, trying local SQLite...")
            self._attempt_local_connection()

        # Initialize database schema after determining connection type
        if self.conn:
            try:
                self._init_complete_database()
                logger.info("✅ Database schema initialization completed successfully.")
            except Exception as e:
                logger.critical(f"❌ Database schema initialization failed after connection: {e}", exc_info=True)
                logger.critical("🚨 Falling back to in-memory storage due to schema initialization failure.")
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                self.conn = None
                self.db_type = "memory"
        else:
            logger.critical("🚨 ALL DATABASE CONNECTIONS FAILED. OPERATING IN NON-PERSISTENT IN-MEMORY STORAGE MODE.")

    def _attempt_cloud_connection(self):
        """Attempt to connect to SQLite Cloud with detailed error reporting and API key support."""
        # Analyze authentication method
        auth_method = self._detect_auth_method()
        
        for attempt in range(self._max_connection_attempts):
            try:
                logger.info(f"🔄 SQLite Cloud connection attempt {attempt + 1}/{self._max_connection_attempts} using {auth_method}")
                
                # Create connection - sqlitecloud library handles both auth methods automatically
                logger.info(f"🔗 Connecting to: {self.connection_string[:50]}...")
                self.conn = sqlitecloud.connect(self.connection_string)
                
                # Test connection immediately with a simple query
                logger.info("🧪 Testing SQLite Cloud connection...")
                test_result = self.conn.execute("SELECT 1 as test_connection").fetchone()
                
                if test_result and test_result[0] == 1:
                    logger.info(f"✅ SQLite Cloud connection established successfully using {auth_method}!")
                    logger.info(f"📊 Test query result: {test_result}")
                    self.db_type = "cloud"
                    self._connection_attempts = 0
                    
                    # Additional tests for API key authentication
                    if auth_method == "API_KEY":
                        try:
                            # Test database info
                            db_list = self.conn.execute("PRAGMA database_list").fetchall()
                            logger.info(f"📋 Database list retrieved: {len(db_list)} databases")
                        except Exception as db_error:
                            logger.warning(f"⚠️ Could not retrieve database info: {db_error}")
                    
                    return
                else:
                    raise Exception(f"Connection test failed - unexpected result: {test_result}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ SQLite Cloud connection attempt {attempt + 1} failed: {error_msg}")
                
                # Enhanced error guidance for API key authentication
                if "api" in error_msg.lower() or "key" in error_msg.lower():
                    logger.error("🔐 API Key error detected. Check your API key validity and permissions.")
                elif "authentication" in error_msg.lower() or "login" in error_msg.lower():
                    logger.error("🔐 Authentication error detected. Verify your API key or credentials.")
                elif "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    logger.error("⏰ Connection timeout detected. Check network connectivity.")
                elif "writing data" in error_msg.lower():
                    logger.error("💾 Data writing error detected. For API key auth, this could be:")
                    logger.error("   • API key lacks write permissions")
                    logger.error("   • Database is in read-only mode") 
                    logger.error("   • Connection stability issues")
                elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                    logger.error("🚫 Permission error detected. Check API key permissions in SQLite Cloud dashboard.")
                
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
                
                if attempt < self._max_connection_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ All SQLite Cloud connection attempts failed using {auth_method}.")
                    self._connection_attempts = self._max_connection_attempts

    def _detect_auth_method(self) -> str:
        """Detect authentication method from connection string"""
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.connection_string)
            query_params = parse_qs(parsed.query)
            
            if 'apikey' in query_params:
                return "API_KEY"
            elif parsed.username and parsed.password:
                return "USERNAME_PASSWORD"
            else:
                return "UNKNOWN"
        except Exception:
            return "PARSE_ERROR"

    def _attempt_local_connection(self):
        """Attempt to connect to local SQLite as fallback."""
        try:
            logger.info("🔄 Attempting local SQLite connection...")
            self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
            
            # Test connection
            test_result = self.conn.execute("SELECT 1 as test_local").fetchone()
            if test_result and test_result[0] == 1:
                logger.info(f"✅ Local SQLite connection established! Test result: {test_result}")
                self.db_type = "file"
            else:
                raise Exception(f"Local connection test failed: {test_result}")
                
        except Exception as e:
            logger.error(f"❌ Local SQLite connection failed: {e}", exc_info=True)
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
            self.conn = None
            self.db_type = "memory"

    def _init_complete_database(self):
        """Initialize database schema with all columns upfront."""
        with self.lock:
            try:
                # For SQLite Cloud, never set row_factory
                if hasattr(self.conn, 'row_factory'): 
                    self.conn.row_factory = None

                logger.info("🏗️ Creating database schema...")
                
                # Create sessions table with all required columns
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
                        wp_token TEXT,
                        timeout_saved_to_crm INTEGER DEFAULT 0,
                        recognition_response TEXT
                    )
                ''')
                
                logger.info("📇 Creating database indexes...")
                # Create indexes for performance
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_session_lookup ON sessions(session_id, active)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fingerprint_id ON sessions(fingerprint_id)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON sessions(email)")
                
                # Commit all changes
                self.conn.commit()
                logger.info("✅ Database schema and indexes created successfully.")
                
            except Exception as e:
                logger.error(f"❌ Database schema creation failed: {e}", exc_info=True)
                raise

    def _check_connection_health(self) -> bool:
        """Enhanced connection health check with detailed logging."""
        if self.db_type == "memory":
            return True
            
        if not self.conn:
            logger.debug("❌ No database connection object available.")
            return False
            
        # Check if we're within the health check interval
        now = datetime.now()
        if (self._last_health_check and 
            now - self._last_health_check < self._health_check_interval):
            return True
            
        try:
            logger.debug(f"🔍 Performing health check on {self.db_type} database...")
            result = self.conn.execute("SELECT 1 as health_check").fetchone()
            
            if result and result[0] == 1:
                self._last_health_check = now
                logger.debug("✅ Database health check passed.")
                return True
            else:
                logger.error(f"❌ Database health check failed - unexpected result: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Database health check failed with error: {e}", exc_info=True)
            
            # Specific error handling for SQLite Cloud
            if "writing data" in str(e).lower():
                logger.error("💾 'Writing data' error detected - likely SQLite Cloud connection issue")
            elif "timeout" in str(e).lower():
                logger.error("⏰ Timeout detected during health check")
            
            # Mark connection as bad
            self.conn = None
            return False

    def _ensure_connection(self): 
        """Enhanced connection management with retry logic."""
        if self._check_connection_health():
            return
            
        logger.warning("⚠️ Database connection unhealthy, attempting to restore...")
        
        # Close old connection if it exists
        old_conn = self.conn
        self.conn = None
        
        if old_conn:
            try:
                old_conn.close()
                logger.debug("🔐 Closed old database connection.")
            except Exception as e:
                logger.debug(f"⚠️ Error closing old connection: {e}")
        
        # Attempt reconnection based on original type
        if self.db_type == "cloud" and SQLITECLOUD_AVAILABLE and self.connection_string:
            logger.info("🔄 Attempting to restore SQLite Cloud connection...")
            self._attempt_cloud_connection()
            
            if self.conn:
                try:
                    self._init_complete_database()
                    logger.info("✅ SQLite Cloud connection restored successfully!")
                    return
                except Exception as e:
                    logger.error(f"❌ Schema initialization failed after reconnection: {e}")
                    self.conn = None
                    
        elif self.db_type == "file":
            logger.info("🔄 Attempting to restore local SQLite connection...")
            self._attempt_local_connection()
            
            if self.conn:
                try:
                    self._init_complete_database()
                    logger.info("✅ Local SQLite connection restored successfully!")
                    return
                except Exception as e:
                    logger.error(f"❌ Schema initialization failed after local reconnection: {e}")
                    self.conn = None
            
        # If all reconnection attempts failed, fall back to memory
        logger.critical("🚨 All database reconnection attempts failed. Falling back to in-memory storage.")
        self.db_type = "memory"

    def test_connection(self) -> Dict[str, Any]:
        """Comprehensive connection test with detailed diagnostics."""
        try:
            with self.lock:
                # Force connection check
                self._ensure_connection()
                
                if self.db_type == "memory":
                    return {
                        "status": "healthy",
                        "type": "memory",
                        "session_count": len(self.local_sessions),
                        "message": "Running in non-persistent memory mode",
                        "connection_attempts": self._connection_attempts
                    }
                
                if not self.conn:
                    return {
                        "status": "failed",
                        "type": self.db_type,
                        "message": "No database connection available after ensure_connection",
                        "connection_attempts": self._connection_attempts,
                        "last_health_check": self._last_health_check.isoformat() if self._last_health_check else None
                    }
                
                # Perform comprehensive tests
                try:
                    # Test 1: Basic connectivity
                    result = self.conn.execute("SELECT 1 as connectivity_test").fetchone()
                    if not result or result[0] != 1:
                        raise Exception(f"Connectivity test failed: {result}")
                    
                    # Test 2: Table existence and structure
                    table_info = self.conn.execute("PRAGMA table_info(sessions)").fetchall()
                    if not table_info:
                        raise Exception("Sessions table does not exist or is not accessible")
                    
                    # Test 3: Count sessions
                    count_result = self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
                    session_count = count_result[0] if count_result else 0
                    
                    # Test 4: Active sessions count
                    active_count_result = self.conn.execute("SELECT COUNT(*) FROM sessions WHERE active = 1").fetchone()
                    active_session_count = active_count_result[0] if active_count_result else 0
                    
                    return {
                        "status": "healthy",
                        "type": self.db_type,
                        "total_sessions": session_count,
                        "active_sessions": active_session_count,
                        "table_columns": len(table_info),
                        "message": f"Connected to {self.db_type} database - all tests passed",
                        "connection_attempts": self._connection_attempts,
                        "last_health_check": self._last_health_check.isoformat() if self._last_health_check else None
                    }
                    
                except Exception as test_error:
                    logger.error(f"❌ Database functionality test failed: {test_error}", exc_info=True)
                    return {
                        "status": "connection_ok_functionality_failed",
                        "type": self.db_type,
                        "message": f"Connection established but functionality test failed: {str(test_error)}",
                        "connection_attempts": self._connection_attempts,
                        "error_type": type(test_error).__name__
                    }
                    
        except Exception as e:
            logger.error(f"❌ Connection test completely failed: {e}", exc_info=True)
            return {
                "status": "critical_failure",
                "type": self.db_type,
                "message": f"Connection test failed with critical error: {str(e)}",
                "connection_attempts": self._connection_attempts,
                "error_type": type(e).__name__
            }

    def load_session(self, session_id: str) -> Optional[UserSession]:
        """Enhanced session loading with comprehensive error handling and debugging."""
        with self.lock:
            logger.info(f"🔍 Loading session {session_id[:8]}...")
            
            try:
                self._ensure_connection()
            except Exception as e:
                logger.error(f"❌ Failed to ensure connection while loading session {session_id[:8]}: {e}", exc_info=True)
                return None

            # Handle in-memory storage
            if self.db_type == "memory":
                session = self.local_sessions.get(session_id)
                if session:
                    # Ensure user_type is correctly typed
                    if isinstance(session.user_type, str):
                        try:
                            session.user_type = UserType(session.user_type)
                        except ValueError:
                            session.user_type = UserType.GUEST
                    logger.info(f"✅ Loaded session {session_id[:8]} from in-memory storage")
                else:
                    logger.info(f"❌ Session {session_id[:8]} not found in in-memory storage")
                return copy.deepcopy(session) if session else None
            
            # Handle database storage
            if not self.conn:
                logger.error(f"❌ Cannot load session {session_id[:8]}: No active database connection")
                return None

            try:
                # Ensure row_factory is not set for SQLite Cloud compatibility
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                logger.debug(f"🗃️ Executing SELECT query for session {session_id[:8]}...")
                
                # Execute the query
                cursor = self.conn.execute("""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, 
                           created_at, last_activity, messages, active, wp_token, 
                           timeout_saved_to_crm, fingerprint_id, fingerprint_method, 
                           visitor_type, daily_question_count, total_question_count, 
                           last_question_time, question_limit_reached, ban_status, 
                           ban_start_time, ban_end_time, ban_reason, evasion_count, 
                           current_penalty_hours, escalation_level, email_addresses_used, 
                           email_switches_count, browser_privacy_level, registration_prompted, 
                           registration_link_clicked, recognition_response 
                    FROM sessions WHERE session_id = ? AND active = 1
                """, (session_id,))
                
                row = cursor.fetchone()
                logger.debug(f"📋 Query executed for {session_id[:8]}, row found: {row is not None}")
                
                if not row:
                    logger.info(f"❌ No active session found for ID {session_id[:8]} in database")
                    
                    # Additional debugging - check if session exists but is inactive
                    inactive_cursor = self.conn.execute("SELECT COUNT(*) FROM sessions WHERE session_id = ?", (session_id,))
                    inactive_count = inactive_cursor.fetchone()
                    if inactive_count and inactive_count[0] > 0:
                        logger.info(f"ℹ️ Session {session_id[:8]} exists but is marked as inactive")
                    else:
                        logger.info(f"ℹ️ Session {session_id[:8]} does not exist in database at all")
                    
                    return None
                
                # Validate row structure
                expected_cols = 31
                if len(row) < expected_cols:
                    logger.error(f"❌ Database row has insufficient columns: {len(row)} (expected {expected_cols}) for session {session_id[:8]}")
                    return None
                
                # Parse the session data
                messages_data = safe_json_loads(row[7], [])
                email_addresses_data = safe_json_loads(row[25], [])
                
                logger.info(f"📊 Session data for {session_id[:8]}: user_type={row[1]}, has_email={bool(row[2])}, messages_count={len(messages_data)}, daily_questions={row[14]}")
                
                try:
                    # Create UserSession object
                    user_session = UserSession(
                        session_id=row[0],
                        user_type=UserType(row[1]) if row[1] else UserType.GUEST,
                        email=row[2],
                        full_name=row[3],
                        zoho_contact_id=row[4],
                        created_at=datetime.fromisoformat(row[5]) if row[5] else datetime.now(),
                        last_activity=datetime.fromisoformat(row[6]) if row[6] else datetime.now(),
                        messages=messages_data,
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
                        ban_end_time=datetime.fromisoformat(row[20]) if row[20] else None,
                        ban_reason=row[21],
                        evasion_count=row[22] or 0,
                        current_penalty_hours=row[23] or 0,
                        escalation_level=row[24] or 0,
                        email_addresses_used=email_addresses_data,
                        email_switches_count=row[26] or 0,
                        browser_privacy_level=row[27],
                        registration_prompted=bool(row[28]),
                        registration_link_clicked=bool(row[29]),
                        recognition_response=row[30]
                    )
                    
                    logger.info(f"✅ Successfully loaded and parsed session {session_id[:8]}: user_type={user_session.user_type.value}, messages={len(user_session.messages)}, email_verified={bool(user_session.email)}")
                    return user_session
                    
                except Exception as e:
                    logger.error(f"❌ Failed to create UserSession object from database row for session {session_id[:8]}: {e}", exc_info=True)
                    logger.error(f"🔍 Row data sample (first 10 fields): {str(row[:10])}")
                    return None
                    
            except Exception as e:
                logger.error(f"❌ Database query failed for session {session_id[:8]}: {e}", exc_info=True)
                
                # Provide specific error guidance
                error_str = str(e).lower()
                if "writing data" in error_str:
                    logger.error("💾 'Writing data' error detected - this indicates SQLite Cloud connection or permissions issues")
                    # Force reconnection on next call
                    self.conn = None
                elif "timeout" in error_str:
                    logger.error("⏰ Database query timeout detected")
                elif "locked" in error_str:
                    logger.error("🔒 Database locked error detected")
                
                return None

    def save_session(self, session: UserSession):
        """Enhanced session saving with comprehensive error handling."""
        with self.lock:
            try:
                self._ensure_connection()
            except Exception as e:
                logger.error(f"❌ Failed to ensure connection while saving session {session.session_id[:8]}: {e}", exc_info=True)
                return

            if self.db_type == "memory":
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.debug(f"💾 Saved session {session.session_id[:8]} to in-memory storage")
                return
            
            if not self.conn:
                logger.error(f"❌ Cannot save session {session.session_id[:8]}: No active database connection")
                # Fallback to memory storage
                if session.session_id not in self.local_sessions:
                    self.local_sessions[session.session_id] = copy.deepcopy(session)
                    logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback")
                return

            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                logger.debug(f"💾 Saving session {session.session_id[:8]} to {self.db_type} database...")
                
                # Prepare JSON data
                try:
                    json_messages = json.dumps(session.messages)
                    json_emails_used = json.dumps(session.email_addresses_used)
                except (TypeError, ValueError) as e:
                    logger.error(f"❌ Session data not JSON serializable for {session.session_id[:8]}: {e}")
                    json_messages = "[]"
                    json_emails_used = "[]"
                
                # Execute the save operation
                self.conn.execute('''
                    REPLACE INTO sessions (
                        session_id, user_type, email, full_name, zoho_contact_id, 
                        created_at, last_activity, messages, active, wp_token, 
                        timeout_saved_to_crm, fingerprint_id, fingerprint_method, 
                        visitor_type, daily_question_count, total_question_count, 
                        last_question_time, question_limit_reached, ban_status, 
                        ban_start_time, ban_end_time, ban_reason, evasion_count, 
                        current_penalty_hours, escalation_level, email_addresses_used, 
                        email_switches_count, browser_privacy_level, registration_prompted, 
                        registration_link_clicked, recognition_response
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    session.ban_end_time.isoformat() if session.ban_end_time else None,
                    session.ban_reason, session.evasion_count, session.current_penalty_hours,
                    session.escalation_level, json_emails_used,
                    session.email_switches_count, session.browser_privacy_level,
                    int(session.registration_prompted), int(session.registration_link_clicked),
                    session.recognition_response
                ))
                
                self.conn.commit()
                logger.debug(f"✅ Successfully saved session {session.session_id[:8]} to database")
                
            except Exception as e:
                logger.error(f"❌ Failed to save session {session.session_id[:8]} to database: {e}", exc_info=True)
                
                # Fallback to in-memory storage
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback due to database error")

    def get_all_active_sessions(self) -> List[UserSession]:
        """Get all active sessions with enhanced error handling."""
        with self.lock:
            try:
                self._ensure_connection()
            except Exception as e:
                logger.error(f"❌ Failed to ensure connection during get_all_active_sessions: {e}")
                return []

            if self.db_type == "memory":
                active_sessions = [copy.deepcopy(s) for s in self.local_sessions.values() if s.active]
                logger.debug(f"📊 Found {len(active_sessions)} active sessions in memory")
                return active_sessions
            
            if not self.conn:
                logger.error("❌ Cannot get active sessions: No database connection available")
                return []

            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None

                logger.debug(f"🔍 Querying all active sessions from {self.db_type} database...")
                cursor = self.conn.execute("""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, 
                           created_at, last_activity, messages, active, wp_token, 
                           timeout_saved_to_crm, fingerprint_id, fingerprint_method, 
                           visitor_type, daily_question_count, total_question_count, 
                           last_question_time, question_limit_reached, ban_status, 
                           ban_start_time, ban_end_time, ban_reason, evasion_count, 
                           current_penalty_hours, escalation_level, email_addresses_used, 
                           email_switches_count, browser_privacy_level, registration_prompted, 
                           registration_link_clicked, recognition_response 
                    FROM sessions WHERE active = 1 ORDER BY last_activity ASC
                """)
                
                sessions = []
                expected_cols = 31
                
                for row in cursor.fetchall():
                    if len(row) < expected_cols:
                        logger.warning(f"⚠️ Row has insufficient columns: {len(row)} (expected {expected_cols}). Skipping.")
                        continue
                        
                    try:
                        s = UserSession(
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
                            ban_end_time=datetime.fromisoformat(row[20]) if row[20] else None,
                            ban_reason=row[21],
                            evasion_count=row[22] or 0,
                            current_penalty_hours=row[23] or 0,
                            escalation_level=row[24] or 0,
                            email_addresses_used=safe_json_loads(row[25], []),
                            email_switches_count=row[26] or 0,
                            browser_privacy_level=row[27],
                            registration_prompted=bool(row[28]),
                            registration_link_clicked=bool(row[29]),
                            recognition_response=row[30]
                        )
                        sessions.append(s)
                    except Exception as e:
                        logger.error(f"❌ Error converting row to session in get_all_active_sessions: {e}", exc_info=True)
                        continue
                        
                logger.info(f"📊 Retrieved {len(sessions)} active sessions from database")
                return sessions
                
            except Exception as e:
                logger.error(f"❌ Failed to get active sessions: {e}", exc_info=True)
                return []

# PDF Exporter (unchanged - robust enough)
class PDFExporter:
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='UserMessage', backColor=lightgrey))

    def generate_chat_pdf(self, session: UserSession) -> Optional[io.BytesIO]:
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
                
                style = self.styles['UserMessage'] if role == 'User' else self.styles['Normal']
                story.append(Spacer(1, 8))
                story.append(Paragraph(f"<b>{role}:</b> {content}", style))
                
            doc.build(story)
            buffer.seek(0)
            return buffer
        except Exception as e:
            logger.error(f"❌ PDF generation failed: {e}", exc_info=True)
            return None

# Zoho CRM Manager (unchanged - already robust)
class ZohoCRMManager:
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token = None
        self._token_expiry = None

    def _get_access_token(self) -> Optional[str]:
        if not ZOHO_ENABLED:
            logger.debug("Zoho is not enabled. Skipping access token request.")
            return None

        if self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            logger.debug("Using cached Zoho access token.")
            return self._access_token
        
        logger.info("🔑 Requesting new Zoho access token...")
        try:
            response = requests.post(
                "https://accounts.zoho.com/oauth/v2/token",
                data={
                    'refresh_token': ZOHO_REFRESH_TOKEN,
                    'client_id': ZOHO_CLIENT_ID,
                    'client_secret': ZOHO_CLIENT_SECRET,
                    'grant_type': 'refresh_token'
                },
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            
            self._access_token = data.get('access_token')
            self._token_expiry = datetime.now() + timedelta(minutes=50)
            logger.info("✅ Successfully obtained Zoho access token.")
            return self._access_token
        except requests.exceptions.Timeout:
            logger.error("⏰ Zoho token request timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"❌ Failed to get Zoho access token: {e}", exc_info=True)
            return None

    def _find_contact_by_email(self, email: str) -> Optional[str]:
        access_token = self._get_access_token()
        if not access_token:
            return None
        
        logger.debug(f"🔍 Searching Zoho for contact with email: {email}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
            params = {'criteria': f'(Email:equals:{email})'}
            response = requests.get(f"{self.base_url}/Contacts/search", headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                contact_id = data['data'][0]['id']
                logger.info(f"✅ Found existing Zoho contact: {contact_id}")
                return contact_id
            logger.debug(f"❌ No Zoho contact found for email: {email}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Zoho contact search for {email} timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"❌ Error finding contact by email {email}: {e}", exc_info=True)
        return None

    def _create_contact(self, email: str, full_name: Optional[str]) -> Optional[str]:
        access_token = self._get_access_token()
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
            response = requests.post(f"{self.base_url}/Contacts", headers=headers, json=contact_data, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data'] and data['data'][0]['code'] == 'SUCCESS':
                contact_id = data['data'][0]['details']['id']
                logger.info(f"✅ Created new Zoho contact: {contact_id}")
                return contact_id
            
            logger.error(f"❌ Zoho contact creation failed with response: {data}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Zoho contact creation for {email} timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"❌ Error creating contact for {email}: {e}", exc_info=True)
        return None

    def _add_note(self, contact_id: str, note_title: str, note_content: str) -> bool:
        access_token = self._get_access_token()
        if not access_token:
            return False

        logger.info(f"📝 Adding note '{note_title}' to Zoho contact {contact_id}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}', 'Content-Type': 'application/json'}
            
            if len(note_content) > 32000:
                logger.warning(f"⚠️ Note content for {contact_id} exceeds 32000 chars. Truncating.")
                note_content = note_content[:32000 - 100] + "\n\n[Content truncated due to size limits]"
            
            note_data = {
                "data": [{
                    "Note_Title": note_title,
                    "Note_Content": note_content,
                    "Parent_Id": {"id": contact_id},
                    "se_module": "Contacts"
                }]
            }
            
            response = requests.post(f"{self.base_url}/Notes", headers=headers, json=note_data, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data'] and data['data'][0]['code'] == 'SUCCESS':
                logger.info(f"✅ Successfully added Zoho note: {note_title}")
                return True
            logger.error(f"❌ Zoho note creation failed with response: {data}")
            return False
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Zoho note '{note_title}' creation for {contact_id} timed out.", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"❌ Error adding note '{note_title}' to Zoho contact {contact_id}: {e}", exc_info=True)
            return False

    def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> bool:
        if not ZOHO_ENABLED:
            logger.info("ℹ️ Zoho is not enabled. Skipping Zoho CRM save.")
            return False
        if not session.email:
            logger.info(f"ℹ️ Session {session.session_id[:8]} has no email. Skipping Zoho CRM save.")
            return False
        if not session.messages:
            logger.info(f"ℹ️ Session {session.session_id[:8]} has no messages. Skipping Zoho CRM save.")
            return False
        
        try:
            logger.info(f"🔄 Starting Zoho CRM save for session {session.session_id[:8]} (Reason: {trigger_reason})")
            
            contact_id = self._find_contact_by_email(session.email)
            if not contact_id:
                contact_id = self._create_contact(session.email, session.full_name)
            if not contact_id:
                logger.error(f"❌ Failed to find or create Zoho contact for {session.email}. Aborting CRM save.")
                return False

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
            note_content += "**Conversation Transcript (truncated if too long):**\n"
            
            for i, msg in enumerate(session.messages):
                role = msg.get("role", "Unknown").capitalize()
                content = re.sub(r'<[^>]+>', '', msg.get("content", ""))
                
                if len(content) > 500:
                    content = content[:500] + "..."
                    
                note_content += f"\n{i+1}. **{role}:** {content}\n"
                
            note_success = self._add_note(contact_id, note_title, note_content)
            if note_success:
                logger.info(f"✅ Zoho CRM save successful for session {session.session_id[:8]}")
                return True
            else:
                logger.error(f"❌ Zoho note creation failed for session {session.session_id[:8]}.")
                return False
                
        except Exception as e:
            logger.error(f"❌ Emergency CRM save process failed for session {session.session_id[:8]}: {e}", exc_info=True)
            return False

# Initialize managers
logger.info("🚀 Initializing managers...")
db_manager = DatabaseManager(SQLITE_CLOUD_CONNECTION)
pdf_exporter = PDFExporter()
zoho_manager = ZohoCRMManager(pdf_exporter)
logger.info("✅ All managers initialized.")

# Helper functions
def is_crm_eligible(session: UserSession) -> bool:
    """Enhanced eligibility check for CRM saves."""
    try:
        if not session.email or not session.messages:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Missing email ({bool(session.email)}) or messages ({bool(session.messages)})")
            return False
        
        # User type eligibility: registered_user OR email_verified_guest
        if session.user_type not in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: User type {session.user_type.value} not eligible.")
            return False
        
        # Question count requirement: at least 1 question asked
        if session.daily_question_count < 1:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: No questions asked ({session.daily_question_count}).")
            return False
        
        # 15-minute eligibility check
        start_time = session.created_at
        if session.last_question_time and session.last_question_time < start_time:
            start_time = session.last_question_time
        
        elapsed_time = datetime.now() - start_time
        elapsed_minutes = elapsed_time.total_seconds() / 60
        
        if elapsed_minutes < 15.0:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Less than 15 minutes active ({elapsed_minutes:.1f} min).")
            return False
            
        logger.debug(f"CRM Eligibility for {session.session_id[:8]}: All checks passed. UserType={session.user_type.value}, Questions={session.daily_question_count}, Elapsed={elapsed_minutes:.1f}min.")
        return True
    except Exception as e:
        logger.error(f"❌ Error checking CRM eligibility for session {session.session_id[:8]}: {e}", exc_info=True)
        return False

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "FiFi Emergency API - Enhanced Version",
        "status": "running",
        "version": "2.0.0",
        "timestamp": datetime.now()
    }

@app.get("/health")
async def health_check():
    try:
        db_status = db_manager.test_connection()
        return {
            "status": "healthy",
            "timestamp": datetime.now(),
            "database": db_status,
            "zoho": "enabled" if ZOHO_ENABLED else "disabled",
            "sqlitecloud_sdk": "available" if SQLITECLOUD_AVAILABLE else "not_available"
        }
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}", exc_info=True)
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(),
            "error": str(e),
            "database": {"status": "failed", "message": "Health check failed"},
            "zoho": "enabled" if ZOHO_ENABLED else "disabled"
        }

@app.get("/quick-diagnose-apikey")
async def quick_diagnose_apikey():
    """Quick diagnostic endpoint specifically for API key authentication issues"""
    
    diagnosis = {
        "timestamp": datetime.now(),
        "connection_string_format": "API_KEY_AUTHENTICATION",
        "environment": {},
        "apikey_tests": {},
        "error_details": []
    }
    
    # Check environment variables
    diagnosis["environment"] = {
        "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING",
        "ZOHO_ENABLED": ZOHO_ENABLED,
        "SQLITECLOUD_AVAILABLE": SQLITECLOUD_AVAILABLE
    }
    
    # Parse and validate API key connection string
    if SQLITE_CLOUD_CONNECTION and SQLITECLOUD_AVAILABLE:
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(SQLITE_CLOUD_CONNECTION)
            query_params = parse_qs(parsed.query)
            
            diagnosis["apikey_tests"]["connection_string_analysis"] = {
                "scheme": parsed.scheme,
                "hostname": parsed.hostname,
                "port": parsed.port,
                "database_path": parsed.path,
                "has_apikey": 'apikey' in query_params,
                "apikey_length": len(query_params['apikey'][0]) if 'apikey' in query_params else 0
            }
            
            # Validate format
            format_issues = []
            if parsed.scheme != 'sqlitecloud':
                format_issues.append(f"Wrong scheme: {parsed.scheme}")
            if not (parsed.hostname.endswith('.sqlite.cloud') or parsed.hostname.endswith('.g4.sqlite.cloud')):
                format_issues.append(f"Invalid hostname: {parsed.hostname}")
            if parsed.port != 8860:
                format_issues.append(f"Wrong port: {parsed.port}")
            if not parsed.path or parsed.path == '/':
                format_issues.append("Missing database name")
            if 'apikey' not in query_params:
                format_issues.append("Missing API key")
            elif len(query_params['apikey'][0]) < 20:
                format_issues.append("API key appears too short")
            
            diagnosis["apikey_tests"]["format_validation"] = {
                "valid": len(format_issues) == 0,
                "issues": format_issues
            }
            
            # Test actual connection with detailed API key error handling
            if len(format_issues) == 0:
                logger.info("🔄 Quick diagnose: Testing SQLite Cloud API key connection...")
                
                try:
                    # Attempt connection
                    conn = sqlitecloud.connect(SQLITE_CLOUD_CONNECTION)
                    diagnosis["apikey_tests"]["connection_established"] = True
                    
                    try:
                        # Test basic query
                        result = conn.execute("SELECT 1 as test").fetchone()
                        if result and result[0] == 1:
                            diagnosis["apikey_tests"]["basic_query"] = "SUCCESS"
                        else:
                            diagnosis["apikey_tests"]["basic_query"] = f"UNEXPECTED_RESULT: {result}"
                            
                    except Exception as query_error:
                        diagnosis["apikey_tests"]["basic_query"] = f"FAILED: {str(query_error)}"
                        diagnosis["error_details"].append({
                            "stage": "basic_query", 
                            "error": str(query_error),
                            "error_type": type(query_error).__name__
                        })
                    
                    try:
                        # Test write permissions (critical for API keys)
                        test_table = f"apikey_test_{int(time.time())}"
                        conn.execute(f"CREATE TABLE IF NOT EXISTS {test_table} (id INTEGER)")
                        conn.execute(f"INSERT INTO {test_table} (id) VALUES (1)")
                        write_result = conn.execute(f"SELECT * FROM {test_table} WHERE id = 1").fetchone()
                        conn.execute(f"DROP TABLE {test_table}")
                        
                        if write_result:
                            diagnosis["apikey_tests"]["write_permissions"] = "SUCCESS"
                        else:
                            diagnosis["apikey_tests"]["write_permissions"] = "NO_DATA_RETURNED"
                            
                    except Exception as write_error:
                        diagnosis["apikey_tests"]["write_permissions"] = f"FAILED: {str(write_error)}"
                        
                        # Specific guidance for API key write permission errors
                        if "writing data" in str(write_error).lower():
                            diagnosis["apikey_tests"]["write_error_guidance"] = {
                                "issue": "API_KEY_WRITE_PERMISSIONS",
                                "solution": "Your API key may not have WRITE permissions",
                                "steps": [
                                    "1. Log into your SQLite Cloud Dashboard",
                                    "2. Go to API Keys section",
                                    "3. Check that your API key has 'Write' or 'Full Access' permissions",
                                    "4. If not, regenerate the API key with proper permissions"
                                ]
                            }
                        
                        diagnosis["error_details"].append({
                            "stage": "write_permissions_test",
                            "error": str(write_error),
                            "error_type": type(write_error).__name__
                        })
                    
                    try:
                        # Check sessions table
                        sessions_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone()
                        if sessions_check:
                            count_result = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
                            diagnosis["apikey_tests"]["sessions_table"] = {
                                "exists": True,
                                "total_count": count_result[0] if count_result else 0
                            }
                        else:
                            diagnosis["apikey_tests"]["sessions_table"] = {
                                "exists": False,
                                "note": "Will be created when first session is saved"
                            }
                    except Exception as sessions_error:
                        diagnosis["apikey_tests"]["sessions_table"] = f"CHECK_FAILED: {str(sessions_error)}"
                    
                    conn.close()
                    diagnosis["apikey_tests"]["connection_closed"] = "SUCCESS"
                    
                except Exception as conn_error:
                    diagnosis["apikey_tests"]["connection_established"] = False
                    diagnosis["apikey_tests"]["connection_error"] = str(conn_error)
                    
                    # Enhanced error categorization for API key issues
                    error_str = str(conn_error).lower()
                    if "api" in error_str or "key" in error_str:
                        diagnosis["apikey_tests"]["error_category"] = "API_KEY_INVALID"
                        diagnosis["apikey_tests"]["solution"] = "Check if API key is correct and not expired"
                    elif "permission" in error_str or "access" in error_str:
                        diagnosis["apikey_tests"]["error_category"] = "API_KEY_PERMISSIONS"
                        diagnosis["apikey_tests"]["solution"] = "API key may lack necessary permissions"
                    elif "writing data" in error_str:
                        diagnosis["apikey_tests"]["error_category"] = "WRITE_PERMISSIONS"
                        diagnosis["apikey_tests"]["solution"] = "API key lacks write permissions - check SQLite Cloud dashboard"
                    elif "timeout" in error_str or "connection" in error_str:
                        diagnosis["apikey_tests"]["error_category"] = "NETWORK_CONNECTIVITY"
                        diagnosis["apikey_tests"]["solution"] = "Network connectivity issue to SQLite Cloud"
                    else:
                        diagnosis["apikey_tests"]["error_category"] = "UNKNOWN"
                        diagnosis["apikey_tests"]["solution"] = "Check SQLite Cloud service status and logs"
                    
                    diagnosis["error_details"].append({
                        "stage": "connection_establishment",
                        "error": str(conn_error),
                        "error_type": type(conn_error).__name__
                    })
            else:
                diagnosis["apikey_tests"]["connection_test"] = "SKIPPED_DUE_TO_FORMAT_ERRORS"
        
        except Exception as parse_error:
            diagnosis["apikey_tests"]["connection_string_parsing"] = f"FAILED: {str(parse_error)}"
            diagnosis["error_details"].append({
                "stage": "connection_string_parsing",
                "error": str(parse_error),
                "error_type": type(parse_error).__name__
            })
    
    else:
        if not SQLITE_CLOUD_CONNECTION:
            diagnosis["error_details"].append({
                "stage": "environment_check",
                "error": "SQLITE_CLOUD_CONNECTION environment variable not set",
                "error_type": "CONFIGURATION"
            })
        if not SQLITECLOUD_AVAILABLE:
            diagnosis["error_details"].append({
                "stage": "dependencies", 
                "error": "sqlitecloud library not available",
                "error_type": "DEPENDENCY"
            })
    
    # Test current database manager state
    try:
        db_status = db_manager.test_connection()
        diagnosis["current_db_manager"] = db_status
    except Exception as db_error:
        diagnosis["current_db_manager"] = {
            "status": "error",
            "error": str(db_error)
        }
    
    return diagnosis
    """Enhanced debug endpoint with comprehensive database diagnostics."""
    try:
        # Test connection comprehensively
        db_status = db_manager.test_connection()
        
        # Test recent sessions
        recent_sessions = []
        try:
            if db_manager.conn and db_manager.db_type != "memory":
                cursor = db_manager.conn.execute("""
                    SELECT session_id, email, user_type, daily_question_count, 
                           created_at, active, timeout_saved_to_crm, last_activity,
                           fingerprint_id, visitor_type
                    FROM sessions 
                    WHERE active = 1 
                    ORDER BY last_activity DESC 
                    LIMIT 5
                """)
                for row in cursor.fetchall():
                    recent_sessions.append({
                        "session_id": row[0][:8] + "...",
                        "email": row[1],
                        "user_type": row[2],
                        "daily_questions": row[3],
                        "created_at": row[4],
                        "active": bool(row[5]),
                        "saved_to_crm": bool(row[6]),
                        "last_activity": row[7],
                        "fingerprint_id": row[8][:8] + "..." if row[8] else None,
                        "visitor_type": row[9]
                    })
            elif db_manager.db_type == "memory":
                for session_id, session in list(db_manager.local_sessions.items())[:5]:
                    recent_sessions.append({
                        "session_id": session_id[:8] + "...",
                        "email": session.email,
                        "user_type": session.user_type.value if hasattr(session.user_type, 'value') else str(session.user_type),
                        "daily_questions": session.daily_question_count,
                        "created_at": session.created_at.isoformat() if hasattr(session.created_at, 'isoformat') else str(session.created_at),
                        "active": session.active,
                        "saved_to_crm": session.timeout_saved_to_crm,
                        "last_activity": session.last_activity.isoformat() if hasattr(session.last_activity, 'isoformat') else str(session.last_activity),
                        "source": "memory"
                    })
        except Exception as query_error:
            recent_sessions = {
                "error": f"Query failed: {str(query_error)}",
                "db_status": db_status
            }
            logger.error(f"❌ Debug DB query for recent sessions failed: {query_error}", exc_info=True)
        
        return {
            "database_status": db_status,
            "recent_sessions": recent_sessions,
            "env_check": {
                "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING",
                "ZOHO_CLIENT_ID": "SET" if ZOHO_CLIENT_ID else "MISSING",
                "ZOHO_CLIENT_SECRET": "SET" if ZOHO_CLIENT_SECRET else "MISSING",
                "ZOHO_REFRESH_TOKEN": "SET" if ZOHO_REFRESH_TOKEN else "MISSING"
            },
            "zoho_status": "enabled" if ZOHO_ENABLED else "disabled",
            "connection_info": {
                "db_type": db_manager.db_type,
                "connection_attempts": getattr(db_manager, '_connection_attempts', 0),
                "sqlitecloud_available": SQLITECLOUD_AVAILABLE,
                "has_connection_string": bool(SQLITE_CLOUD_CONNECTION)
            }
        }
    except Exception as e:
        logger.critical(f"❌ Critical error in debug_database endpoint: {e}", exc_info=True)
        return {
            "error": str(e),
            "database_status": "CRITICAL_FAILURE",
            "env_check": {
                "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING"
            },
            "connection_info": {
                "db_type": getattr(db_manager, 'db_type', 'unknown'),
                "sqlitecloud_available": SQLITECLOUD_AVAILABLE
            }
        }

@app.post("/emergency-save")
async def emergency_save(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    try:
        logger.info(f"🚨 EMERGENCY SAVE: Request for session {request.session_id[:8]}, reason: {request.reason}")
        
        # Enhanced database status check before attempting to load session
        db_status = db_manager.test_connection()
        logger.info(f"📊 Database status before session load: {db_status}")
        
        if db_status["status"] not in ["healthy", "connection_ok_functionality_failed"]:
            logger.error(f"❌ Database is not healthy before attempting to load session: {db_status}")
            return {
                "success": False,
                "message": f"Database connection issue: {db_status.get('message', 'Unknown database error')}",
                "session_id": request.session_id,
                "reason": "database_unhealthy",
                "db_status": db_status,
                "timestamp": datetime.now()
            }
        
        # Attempt to load the session
        logger.info(f"🔍 Attempting to load session {request.session_id[:8]} from database...")
        session = db_manager.load_session(request.session_id)
        
        if not session:
            logger.error(f"❌ Session {request.session_id[:8]} not found or not active after load attempt.")
            
            # Provide enhanced error information
            return {
                "success": False,
                "message": "Session not found or not active. This could indicate the session expired, was never created in Streamlit, or there's a database connectivity issue between services.",
                "session_id": request.session_id,
                "reason": "session_not_found",
                "db_status": db_status,
                "timestamp": datetime.now(),
                "suggestions": [
                    "Verify the session was created in the Streamlit app",
                    "Check if the session expired due to inactivity (15+ minutes)",
                    "Confirm database connectivity between Streamlit and FastAPI services",
                    "Check if the session was manually closed or marked inactive"
                ],
                "debug_info": {
                    "db_type": db_manager.db_type,
                    "connection_attempts": getattr(db_manager, '_connection_attempts', 0)
                }
            }
        
        logger.info(f"✅ Session {session.session_id[:8]} loaded successfully:")
        logger.info(f"   - Email: {'SET' if session.email else 'NOT_SET'}")
        logger.info(f"   - User Type: {session.user_type.value}")
        logger.info(f"   - Messages: {len(session.messages)}")
        logger.info(f"   - Daily Questions: {session.daily_question_count}")
        logger.info(f"   - Already Saved to CRM: {session.timeout_saved_to_crm}")
        
        # Check CRM eligibility
        if not is_crm_eligible(session):
            logger.info(f"ℹ️ Session {request.session_id[:8]} not eligible for CRM save based on current criteria.")
            return {
                "success": False,
                "message": "Session not eligible for CRM save based on usage criteria (user type, activity level, or duration).",
                "session_id": request.session_id,
                "reason": "not_eligible",
                "timestamp": datetime.now(),
                "eligibility_details": {
                    "has_email": bool(session.email),
                    "has_messages": len(session.messages) > 0,
                    "message_count": len(session.messages),
                    "user_type": session.user_type.value,
                    "daily_questions": session.daily_question_count,
                    "is_registered_or_verified": session.user_type in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST],
                    "session_age_minutes": (datetime.now() - session.created_at).total_seconds() / 60
                }
            }
        
        # Check if already saved
        if session.timeout_saved_to_crm:
            logger.info(f"ℹ️ Session {request.session_id[:8]} already marked as saved to CRM. Skipping duplicate save.")
            return {
                "success": True,
                "message": "Session already saved to CRM, no action needed.",
                "session_id": request.session_id,
                "reason": "already_saved",
                "timestamp": datetime.now()
            }

        # Queue CRM save in background to avoid timeout
        logger.info(f"📝 Queuing emergency CRM save for session {request.session_id[:8]}...")
        background_tasks.add_task(
            _perform_emergency_crm_save,
            session,
            f"Emergency Save: {request.reason}"
        )
        
        logger.info(f"✅ Emergency save queued successfully for {request.session_id[:8]}")
        return {
            "success": True,
            "message": "Emergency save queued successfully",
            "session_id": request.session_id,
            "reason": request.reason,
            "queued_for_background_processing": True,
            "timestamp": datetime.now(),
            "session_info": {
                "user_type": session.user_type.value,
                "message_count": len(session.messages),
                "daily_questions": session.daily_question_count
            }
        }
            
    except Exception as e:
        logger.critical(f"❌ Critical error in emergency_save endpoint for session {request.session_id[:8]}: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Internal server error during emergency save: {str(e)}",
            "session_id": request.session_id,
            "reason": "internal_error",
            "error_type": type(e).__name__,
            "timestamp": datetime.now()
        }

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions():
    """Enhanced endpoint to manually trigger cleanup of expired sessions."""
    try:
        logger.info("🧹 SESSION CLEANUP: Starting enhanced cleanup process")
        
        results = {
            "processed_sessions": 0, 
            "crm_saved": 0, 
            "marked_inactive": 0, 
            "errors_processing": 0,
            "skipped_active": 0
        }
        
        active_sessions = db_manager.get_all_active_sessions()
        logger.info(f"📊 Found {len(active_sessions)} potentially active sessions for cleanup evaluation.")
        
        for session in active_sessions:
            try:
                results["processed_sessions"] += 1
                time_since_activity = datetime.now() - session.last_activity
                inactive_minutes = time_since_activity.total_seconds() / 60
                
                # 15-minute inactivity threshold
                if inactive_minutes >= 15:
                    logger.info(f"🔄 Session {session.session_id[:8]} inactive for {inactive_minutes:.1f} minutes. Processing for cleanup.")
                    
                    # Check CRM eligibility and save if needed
                    if is_crm_eligible(session) and not session.timeout_saved_to_crm:
                        logger.info(f"💾 Session {session.session_id[:8]} is CRM eligible and not yet saved. Attempting auto-save.")
                        save_success = zoho_manager.save_chat_transcript_sync(
                            session, "Cleanup - Expired Session"
                        )
                        if save_success:
                            session.timeout_saved_to_crm = True
                            results["crm_saved"] += 1
                            logger.info(f"✅ CRM auto-save successful for {session.session_id[:8]} during cleanup.")
                        else:
                            logger.warning(f"⚠️ CRM auto-save FAILED for {session.session_id[:8]} during cleanup.")
                    else:
                        logger.debug(f"ℹ️ Session {session.session_id[:8]} not CRM eligible or already saved during cleanup.")
                    
                    # Mark session as inactive
                    session.active = False
                    session.last_activity = datetime.now()
                    db_manager.save_session(session)
                    results["marked_inactive"] += 1
                    logger.info(f"🔒 Session {session.session_id[:8]} marked inactive during cleanup.")
                else:
                    results["skipped_active"] += 1
                    logger.debug(f"⏰ Session {session.session_id[:8]} still active ({inactive_minutes:.1f} min inactivity). Skipping cleanup.")
                    
            except Exception as e:
                logger.error(f"❌ Error processing session {session.session_id[:8]} during cleanup: {e}", exc_info=True)
                results["errors_processing"] += 1
        
        cleanup_summary = (
            f"🧹 SESSION CLEANUP COMPLETE: "
            f"Processed={results['processed_sessions']}, "
            f"CRM_Saved={results['crm_saved']}, "
            f"Marked_Inactive={results['marked_inactive']}, "
            f"Errors={results['errors_processing']}, "
            f"Skipped_Active={results['skipped_active']}"
        )
        logger.info(cleanup_summary)
        
        return {
            **results, 
            "timestamp": datetime.now(),
            "summary": cleanup_summary
        }
        
    except Exception as e:
        logger.critical(f"❌ Critical error during cleanup_expired_sessions endpoint execution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during cleanup: {str(e)}")

async def _perform_emergency_crm_save(session, reason: str):
    """Enhanced background task to perform CRM save and update session status."""
    try:
        logger.info(f"🔄 Background CRM save task starting for session {session.session_id[:8]} (Reason: {reason})")
        
        save_success = zoho_manager.save_chat_transcript_sync(session, reason)
        
        if save_success:
            # Update session to reflect the save
            session.timeout_saved_to_crm = True
            session.last_activity = datetime.now()
            db_manager.save_session(session)
            logger.info(f"✅ Background CRM save completed successfully for session {session.session_id[:8]}")
        else:
            logger.error(f"❌ Background CRM save failed for session {session.session_id[:8]}")
            
    except Exception as e:
        logger.critical(f"❌ Critical error in background CRM save task for session {session.session_id[:8]}: {e}", exc_info=True)

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting FiFi Emergency API server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
