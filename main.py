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
import base64

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI app with startup event
app = FastAPI(title="FiFi Emergency API - Complete Integrated Version", version="3.2.2-cold-start-fixed")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.options("/emergency-save")
async def emergency_save_options():
    """Fast OPTIONS handler to prevent 504 timeouts"""
    return {"status": "ok"}

# FIXED: Lightweight startup events - no heavy operations
@app.on_event("startup")
async def startup_event():
    """FIXED: Handle cold start without heavy operations"""
    logger.info("🚀 FastAPI startup event - Cold start optimized")
    logger.info("🔄 Database connections will initialize on first request")

@app.on_event("startup") 
async def startup_health_check():
    """FIXED: Lightweight health check on startup"""
    try:
        logger.info("✅ Startup health check completed - ready for requests")
    except Exception as e:
        logger.error(f"⚠️ Startup health check warning: {e}")

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

# FIXED: Database Manager with Cold Start Fix Only
class ResilientDatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock()
        self.conn = None
        self.connection_string = connection_string
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=2)
        self._connection_attempts = 0
        self._max_connection_attempts = 5
        self._last_socket_error = None
        self._consecutive_socket_errors = 0
        self._auth_method = None
        
        logger.info("🔄 INITIALIZING RESILIENT DATABASE MANAGER WITH COLD START FIX")
        logger.info("🔑 Features: API Key Auth + Socket Error Resilience + FIXED Cold Start")
        
        self.db_type = "memory"
        self.local_sessions = {}

        # Analyze connection string format
        if connection_string:
            self._analyze_connection_string()
        
        # Attempt connections with enhanced support
        if connection_string and SQLITECLOUD_AVAILABLE:
            logger.info(f"🔑 Attempting SQLite Cloud connection using {self._auth_method} authentication...")
            self._attempt_resilient_cloud_connection()
        elif connection_string:
            logger.error("❌ SQLite Cloud connection string provided but sqlitecloud library is not available.")
        else:
            logger.info("ℹ️ No SQLite Cloud connection string provided, skipping cloud connection attempt.")
        
        # Fallback to local SQLite if cloud connection failed
        if not self.conn:
            logger.info("☁️ Cloud connection failed or not attempted, trying local SQLite...")
            self._attempt_local_connection()

        # Initialize database schema
        if self.conn:
            try:
                self._init_complete_database()
                logger.info("✅ Database schema initialization completed successfully.")
            except Exception as e:
                logger.critical(f"❌ Database schema initialization failed: {e}", exc_info=True)
                self._fallback_to_memory()
        else:
            logger.critical("🚨 ALL DATABASE CONNECTIONS FAILED. OPERATING IN NON-PERSISTENT IN-MEMORY STORAGE MODE.")

    def _analyze_connection_string(self):
        """Analyze connection string to determine authentication method"""
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.connection_string)
            query_params = parse_qs(parsed.query)
            
            if 'apikey' in query_params:
                self._auth_method = "API_KEY"
                apikey = query_params['apikey'][0]
                logger.info(f"🔑 Detected API Key authentication (key length: {len(apikey)} chars)")
            elif parsed.username and parsed.password:
                self._auth_method = "USERNAME_PASSWORD"
                logger.info(f"🔑 Detected Username/Password authentication (user: {parsed.username})")
            else:
                self._auth_method = "UNKNOWN"
                logger.warning("⚠️ Could not determine authentication method from connection string")
                
        except Exception as e:
            logger.error(f"❌ Failed to analyze connection string: {e}")
            self._auth_method = "PARSE_ERROR"

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
            "connection timed out"
        ]
        return any(indicator in error_str for indicator in socket_indicators)

    def _attempt_resilient_cloud_connection(self):
        """Enhanced connection attempt with socket error resilience and API key support"""
        for attempt in range(self._max_connection_attempts):
            try:
                logger.info(f"🔄 Resilient SQLite Cloud connection attempt {attempt + 1}/{self._max_connection_attempts} using {self._auth_method}")
                
                # Close any existing connection first
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
                
                # Create fresh connection - sqlitecloud library handles API key automatically
                logger.info(f"🔗 Connecting to: {self.connection_string[:50]}...")
                self.conn = sqlitecloud.connect(self.connection_string)
                
                # Immediate connection test with socket error detection
                logger.info("🧪 Testing connection with socket error detection...")
                test_result = self.conn.execute("SELECT 1 as connection_test").fetchone()
                
                if test_result and test_result[0] == 1:
                    logger.info(f"✅ SQLite Cloud connection established successfully using {self._auth_method}!")
                    logger.info(f"📊 Test query result: {test_result}")
                    self.db_type = "cloud"
                    self._connection_attempts = 0
                    self._consecutive_socket_errors = 0
                    
                    # Additional tests for API key authentication
                    if self._auth_method == "API_KEY":
                        try:
                            # Test database info
                            db_list = self.conn.execute("PRAGMA database_list").fetchall()
                            logger.info(f"📋 API Key access confirmed: {len(db_list)} databases accessible")
                        except Exception as db_error:
                            logger.warning(f"⚠️ Could not retrieve database info: {db_error}")
                    
                    return
                else:
                    raise Exception(f"Connection test failed - unexpected result: {test_result}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ SQLite Cloud connection attempt {attempt + 1} failed: {error_msg}")
                
                # Handle socket errors specifically
                if self._is_socket_error(e):
                    self._consecutive_socket_errors += 1
                    self._last_socket_error = datetime.now()
                    logger.error(f"🔌 SOCKET ERROR DETECTED (#{self._consecutive_socket_errors}): {error_msg}")
                    logger.error("🔧 This indicates network connectivity issues with SQLite Cloud")
                    
                    # Progressive backoff for socket errors
                    if attempt < self._max_connection_attempts - 1:
                        wait_time = min(10, 2 ** attempt)  # Cap at 10 seconds
                        logger.info(f"⏳ Socket error backoff: waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                else:
                    # Enhanced error guidance for API key authentication
                    if "api" in error_msg.lower() or "key" in error_msg.lower():
                        logger.error("🔐 API Key error detected. Check your API key validity and permissions.")
                    elif "authentication" in error_msg.lower() or "login" in error_msg.lower():
                        logger.error("🔐 Authentication error detected. Verify your API key or credentials.")
                    elif "writing data" in error_msg.lower():
                        logger.error("💾 Data writing error detected. For API key auth, this could be:")
                        logger.error("   • API key lacks write permissions")
                        logger.error("   • Database is in read-only mode") 
                        logger.error("   • Connection stability issues")
                    elif "permission" in error_msg.lower() or "access" in error_msg.lower():
                        logger.error("🚫 Permission error detected. Check API key permissions in SQLite Cloud dashboard.")
                    
                    if attempt < self._max_connection_attempts - 1:
                        wait_time = 2 ** attempt
                        time.sleep(wait_time)

                # Clean up failed connection
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
        
        logger.error(f"❌ All {self._max_connection_attempts} SQLite Cloud connection attempts failed using {self._auth_method}")
        if self._consecutive_socket_errors > 0:
            logger.error(f"🔌 Total socket errors encountered: {self._consecutive_socket_errors}")
            logger.error("💡 Recommendation: Check network connectivity between Cloud Run and SQLite Cloud")
        
        self._connection_attempts = self._max_connection_attempts

    def _attempt_local_connection(self):
        """Fallback to local SQLite"""
        try:
            logger.info("🔄 Attempting local SQLite connection as fallback...")
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
            self._fallback_to_memory()

    def _fallback_to_memory(self):
        """Fallback to in-memory storage with proper cleanup"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        self.conn = None
        self.db_type = "memory"
        logger.warning("⚠️ Operating in in-memory mode due to persistent connection issues")

    def _init_complete_database(self):
        """Initialize database schema with all columns upfront"""
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
        """Enhanced health check with socket error detection"""
        if self.db_type == "memory":
            return True
            
        if not self.conn:
            logger.debug("❌ No database connection object available")
            return False
            
        # Check if we're within the health check interval (unless we've had recent socket errors)
        now = datetime.now()
        if (self._last_health_check and 
            now - self._last_health_check < self._health_check_interval and
            self._consecutive_socket_errors == 0):  # Force check if we've had socket errors
            return True
            
        try:
            logger.debug(f"🔍 Performing health check with socket error detection...")
            result = self.conn.execute("SELECT 1 as health_check").fetchone()
            
            if result and result[0] == 1:
                self._last_health_check = now
                self._consecutive_socket_errors = 0  # Reset on successful health check
                logger.debug("✅ Database health check passed")
                return True
            else:
                logger.error(f"❌ Database health check failed - unexpected result: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}", exc_info=True)
            
            # Socket error specific handling
            if self._is_socket_error(e):
                self._consecutive_socket_errors += 1
                self._last_socket_error = now
                logger.error(f"🔌 SOCKET ERROR during health check (#{self._consecutive_socket_errors})")
                logger.error("🔧 Connection will be refreshed on next operation")
            
            # Mark connection as bad
            self.conn = None
            return False

    def _ensure_connection(self):
        """FIXED: Enhanced connection management WITHOUT forced cold start reconnection"""
        
        # REMOVED: The problematic cold start forced reconnection that was causing hangs
        # OLD CODE THAT CAUSED HANGS:
        # if not hasattr(self, '_container_start_time'):
        #     self._container_start_time = datetime.now()
        #     logger.info("🆕 Cold start detected - forcing fresh database connection")
        #     if self.conn:
        #         try:
        #             self.conn.close()
        #         except:
        #             pass
        #         self.conn = None
        
        if self._check_connection_health():
            return
            
        logger.warning("⚠️ Database connection unhealthy or has socket errors, attempting to restore...")
        
        # If we've had recent socket errors, always try to reconnect
        if (self._consecutive_socket_errors > 0 and 
            self._last_socket_error and 
            datetime.now() - self._last_socket_error < timedelta(minutes=5)):
            logger.info(f"🔌 Recent socket errors detected ({self._consecutive_socket_errors}), forcing reconnection")
        
        # Close old connection
        old_conn = self.conn
        self.conn = None
        
        if old_conn:
            try:
                old_conn.close()
                logger.debug("🔐 Closed old database connection")
            except Exception as e:
                logger.debug(f"⚠️ Error closing old connection (expected for socket errors): {e}")
        
        # KEPT: Only clear connection state for actual reconnection needs
        self._last_health_check = None
        
        # Attempt reconnection with socket error handling
        if self.db_type == "cloud" and SQLITECLOUD_AVAILABLE and self.connection_string:
            logger.info("🔄 Attempting to restore SQLite Cloud connection with socket resilience...")
            self._attempt_resilient_cloud_connection()
            
            if self.conn:
                try:
                    self._init_complete_database()
                    logger.info("✅ SQLite Cloud connection restored successfully!")
                    return
                except Exception as e:
                    logger.error(f"❌ Schema initialization failed after reconnection: {e}")
                    self._fallback_to_memory()
                    return
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
                    self._fallback_to_memory()
                    return
        
        # If all reconnection attempts failed, fall back
        logger.critical("🚨 All connection restoration attempts failed, falling back to in-memory storage")
        self._fallback_to_memory()

    def _execute_with_socket_retry(self, query: str, params: tuple = None, max_retries: int = 3):
        """Execute query with automatic socket error retry"""
        for attempt in range(max_retries):
            try:
                # Ensure connection is healthy
                self._ensure_connection()
                
                if self.db_type == "memory":
                    raise Exception("Cannot execute SQL in memory mode")
                
                if not self.conn:
                    raise Exception("No database connection available")
                
                # Execute query
                if params:
                    result = self.conn.execute(query, params)
                else:
                    result = self.conn.execute(query)
                
                # Reset socket error counter on successful execution
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
                        # Force connection refresh for socket errors
                        self.conn = None
                        wait_time = min(5, 2 ** attempt)
                        logger.info(f"⏳ Retrying query in {wait_time} seconds due to socket error...")
                        time.sleep(wait_time)
                        continue
                else:
                    logger.error(f"❌ Non-socket error during query execution: {e}")
                
                # Re-raise the error if we've exhausted retries or it's not a socket error
                if attempt == max_retries - 1 or not is_socket_error:
                    raise

    def test_connection(self) -> Dict[str, Any]:
        """Comprehensive connection test with API key and socket error diagnostics"""
        try:
            with self.lock:
                self._ensure_connection()
                
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
                    # Test basic connectivity
                    basic_result = self.conn.execute("SELECT 1 as connectivity_test").fetchone()
                    if not basic_result or basic_result[0] != 1:
                        raise Exception(f"Connectivity test failed: {basic_result}")
                    
                    # Test database operations
                    if self._auth_method == "API_KEY":
                        try:
                            db_list = self.conn.execute("PRAGMA database_list").fetchall()
                            result["api_key_access"] = f"{len(db_list)} databases accessible"
                        except Exception as db_list_error:
                            result["api_key_access_error"] = str(db_list_error)
                    
                    # Test sessions table
                    try:
                        sessions_check = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone()
                        if sessions_check:
                            count_result = self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
                            active_count_result = self.conn.execute("SELECT COUNT(*) FROM sessions WHERE active = 1").fetchone()
                            result["sessions_table"] = {
                                "exists": True,
                                "total_count": count_result[0] if count_result else 0,
                                "active_count": active_count_result[0] if active_count_result else 0
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

    def load_session(self, session_id: str) -> Optional[UserSession]:
        """Load session with complete socket error resilience"""
        with self.lock:
            logger.debug(f"🔍 Loading session {session_id[:8]} with complete resilience...")
            
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
                # Use socket-resilient query execution
                cursor = self._execute_with_socket_retry("""
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
                
                if not row:
                    logger.info(f"❌ No active session found for {session_id[:8]}")
                    return None
                
                # Parse session
                expected_cols = 31
                if len(row) < expected_cols:
                    logger.error(f"❌ Row has insufficient columns: {len(row)} (expected {expected_cols})")
                    return None
                
                try:
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
                    
                    logger.info(f"✅ Successfully loaded session {session_id[:8]} with complete resilience")
                    return user_session
                    
                except Exception as e:
                    logger.error(f"❌ Failed to create UserSession object: {e}", exc_info=True)
                    return None
                    
            except Exception as e:
                logger.error(f"❌ Failed to load session {session_id[:8]} due to persistent errors: {e}", exc_info=True)
                
                # If we have consecutive socket errors, recommend monitoring
                if self._consecutive_socket_errors >= 3:
                    logger.error(f"🔌 {self._consecutive_socket_errors} consecutive socket errors detected")
                    logger.error("💡 Consider checking network connectivity or switching to local SQLite")
                
                return None

    def save_session(self, session: UserSession):
        """Save session with socket error resilience and proper UPDATE logic"""
        with self.lock:
            if self.db_type == "memory":
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.debug(f"💾 Saved session {session.session_id[:8]} to memory")
                return
            
            try:
                # Prepare JSON data
                try:
                    json_messages = json.dumps(session.messages)
                    json_emails_used = json.dumps(session.email_addresses_used)
                except (TypeError, ValueError) as e:
                    logger.error(f"❌ Session data not JSON serializable for {session.session_id[:8]}: {e}")
                    json_messages = "[]"
                    json_emails_used = "[]"
                
                # Use INSERT OR REPLACE for proper upsert behavior
                self._execute_with_socket_retry('''
                    INSERT OR REPLACE INTO sessions (
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
                logger.debug(f"✅ Successfully saved session {session.session_id[:8]} to database (Zoho Contact ID: {'SET' if session.zoho_contact_id else 'NOT_SET'})")
                
            except Exception as e:
                logger.error(f"❌ Failed to save session {session.session_id[:8]} to database: {e}", exc_info=True)
                
                # Fallback to in-memory storage
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback due to database error")

    def cleanup_expired_sessions(self, expiry_minutes: int = 15) -> Dict[str, Any]:
        """Clean up expired sessions and mark them as inactive"""
        with self.lock:
            logger.info(f"🧹 Starting cleanup of sessions expired more than {expiry_minutes} minutes ago...")
            
            if self.db_type == "memory":
                # Clean up in-memory sessions
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                expired_sessions = []
                
                for session_id, session in list(self.local_sessions.items()):
                    if session.active and session.last_activity < cutoff_time:
                        session.active = False  # Mark as inactive
                        expired_sessions.append(session_id)
                        logger.debug(f"🔄 Marked in-memory session {session_id[:8]} as inactive")
                
                logger.info(f"✅ Cleaned up {len(expired_sessions)} expired sessions from memory")
                return {
                    "success": True,
                    "cleaned_up_count": len(expired_sessions),
                    "storage_type": "memory",
                    "expired_session_ids": [sid[:8] + "..." for sid in expired_sessions]
                }
            
            try:
                # Ensure we have a healthy connection before cleanup
                self._ensure_connection()
                
                if not self.conn:
                    logger.error("❌ No database connection available for cleanup")
                    return {
                        "success": False,
                        "error": "No database connection available",
                        "storage_type": self.db_type,
                        "message": "Cleanup failed due to no database connection"
                    }
                
                # Calculate cutoff time
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                cutoff_iso = cutoff_time.isoformat()
                logger.info(f"🕒 Cleanup cutoff time: {cutoff_iso}")
                
                # Find expired sessions first
                cursor = self._execute_with_socket_retry("""
                    SELECT session_id, last_activity FROM sessions 
                    WHERE active = 1 AND last_activity < ?
                """, (cutoff_iso,))
                
                expired_sessions = cursor.fetchall()
                logger.info(f"🔍 Found {len(expired_sessions)} expired sessions to clean up")
                
                if not expired_sessions:
                    logger.info("✅ No expired sessions found")
                    return {
                        "success": True,
                        "cleaned_up_count": 0,
                        "storage_type": self.db_type,
                        "message": "No expired sessions found"
                    }
                
                # Log sessions being cleaned up
                for session_id, last_activity in expired_sessions:
                    logger.info(f"🧹 Cleaning up session {session_id[:8]} (last activity: {last_activity})")
                
                # Use explicit UPDATE with rowcount verification
                cursor = self._execute_with_socket_retry("""
                    UPDATE sessions SET active = 0 
                    WHERE active = 1 AND last_activity < ?
                """, (cutoff_iso,))
                
                # Get the number of rows actually updated
                rows_affected = cursor.rowcount if hasattr(cursor, 'rowcount') else len(expired_sessions)
                logger.info(f"📊 Database reports {rows_affected} rows updated")
                
                # Force commit to ensure changes are persisted
                self.conn.commit()
                logger.info("💾 Changes committed to database")
                
                # Verify the update worked by checking again
                verification_cursor = self._execute_with_socket_retry("""
                    SELECT COUNT(*) FROM sessions 
                    WHERE active = 1 AND last_activity < ?
                """, (cutoff_iso,))
                
                remaining_expired = verification_cursor.fetchone()[0]
                logger.info(f"🔍 Verification: {remaining_expired} expired sessions still active after cleanup")
                
                expired_session_ids = [session[0] for session in expired_sessions]
                logger.info(f"✅ Successfully processed {len(expired_sessions)} expired sessions")
                
                return {
                    "success": True,
                    "cleaned_up_count": len(expired_sessions),
                    "rows_affected": rows_affected,
                    "remaining_expired_after_cleanup": remaining_expired,
                    "storage_type": self.db_type,
                    "expired_session_ids": [sid[:8] + "..." for sid in expired_session_ids],
                    "cutoff_time": cutoff_iso
                }
                
            except Exception as e:
                logger.error(f"❌ Failed to cleanup expired sessions: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": str(e),
                    "storage_type": self.db_type,
                    "message": "Cleanup failed due to database error"
                }

# KEPT: Working PDF Exporter (unchanged)
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

# KEPT: Working Zoho CRM Manager (unchanged)
class ZohoCRMManager:
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token = None
        self._token_expiry = None

    def _get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        if not ZOHO_ENABLED:
            logger.debug("Zoho is not enabled. Skipping access token request.")
            return None

        # Add force_refresh support for PDF attachment retry logic
        if not force_refresh and self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
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
        except Exception as e:
            logger.error(f"❌ Error adding note '{note_title}' to Zoho contact {contact_id}: {e}", exc_info=True)
            return False

    def _upload_attachment(self, contact_id: str, pdf_buffer: io.BytesIO, filename: str) -> bool:
        """KEPT: Working PDF attachment upload method"""
        access_token = self._get_access_token()
        if not access_token:
            return False

        logger.info(f"📎 Adding PDF attachment '{filename}' to Zoho contact {contact_id}")
        
        # Use the correct URL format from working fifi.py
        upload_url = f"{self.base_url}/Contacts/{contact_id}/Attachments"
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
                
                # Use the exact working approach from fifi.py
                pdf_buffer.seek(0)
                response = requests.post(
                    upload_url, 
                    headers=headers, 
                    files={'file': (filename, pdf_buffer.read(), 'application/pdf')},
                    timeout=60  # Increased timeout like fifi.py
                )
                
                # Handle token refresh like fifi.py
                if response.status_code == 401:
                    logger.warning("Zoho token expired during upload, attempting refresh...")
                    access_token = self._get_access_token(force_refresh=True)
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
                    
            except requests.exceptions.Timeout:
                logger.error(f"⏰ Zoho upload timeout (attempt {attempt + 1}/{max_retries})")
            except Exception as e:
                logger.error(f"❌ Error adding PDF attachment (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                
        return False

    def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> Dict[str, Any]:
        """KEPT: Working CRM save method with PDF attachment"""
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
            logger.info(f"🔄 Starting enhanced Zoho CRM save for session {session.session_id[:8]} (Reason: {trigger_reason})")
            
            # Find or create contact
            contact_id = self._find_contact_by_email(session.email)
            if not contact_id:
                contact_id = self._create_contact(session.email, session.full_name)
            if not contact_id:
                logger.error(f"❌ Failed to find or create Zoho contact for {session.email}. Aborting CRM save.")
                return {"success": False, "reason": "contact_creation_failed"}

            # Update session with contact ID and save to database
            if not session.zoho_contact_id:
                session.zoho_contact_id = contact_id
                logger.info(f"🔗 Updated session {session.session_id[:8]} with Zoho contact ID: {contact_id}")

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            note_title = f"FiFi AI Emergency Save - {timestamp} ({trigger_reason})"
            
            # Create note content
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
                
                if len(content) > 200:  # Shorter preview since we have PDF
                    content = content[:200] + "..."
                    
                note_content += f"\n{i+1}. **{role}:** {content}\n"
                
            # Add the note
            note_success = self._add_note(contact_id, note_title, note_content)
            
            # Generate and attach PDF using WORKING method
            pdf_success = False
            pdf_buffer = self.pdf_exporter.generate_chat_pdf(session)
            if pdf_buffer:
                pdf_filename = f"FiFi_Chat_Transcript_{session.session_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                pdf_success = self._upload_attachment(contact_id, pdf_buffer, pdf_filename)
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

# Initialize managers
logger.info("🚀 Initializing managers with cold start fix...")
db_manager = ResilientDatabaseManager(SQLITE_CLOUD_CONNECTION)
pdf_exporter = PDFExporter()
zoho_manager = ZohoCRMManager(pdf_exporter)
logger.info("✅ All managers initialized with MINIMAL COLD START FIX.")

# KEPT: Working helper functions
def is_crm_eligible(session: UserSession, is_emergency_save: bool = False) -> bool:
    """Enhanced eligibility check for CRM saves"""
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
        
        # Emergency saves bypass the 15-minute rule
        if not is_emergency_save:
            # 15-minute eligibility check (only for timeout saves, NOT emergency saves)
            start_time = session.created_at
            if session.last_question_time and session.last_question_time < start_time:
                start_time = session.last_question_time
            
            elapsed_time = datetime.now() - start_time
            elapsed_minutes = elapsed_time.total_seconds() / 60
            
            if elapsed_minutes < 15.0:
                logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Less than 15 minutes active ({elapsed_minutes:.1f} min).")
                return False
            
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: All checks passed. UserType={session.user_type.value}, Questions={session.daily_question_count}, Elapsed={elapsed_minutes:.1f}min.")
        else:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Emergency save - bypassing time requirement. UserType={session.user_type.value}, Questions={session.daily_question_count}.")
            
        return True
    except Exception as e:
        logger.error(f"❌ Error checking CRM eligibility for session {session.session_id[:8]}: {e}", exc_info=True)
        return False

# KEPT: Working background task
async def _perform_emergency_crm_save(session, reason: str):
    """KEPT: Working background task with session-ending logic"""
    try:
        logger.info(f"🔄 Background CRM save task starting for session {session.session_id[:8]} (Reason: {reason})")
        
        # Check if this is a session-ending reason
        is_session_ending = is_session_ending_reason(reason)
        logger.info(f"📋 Session ending check: {is_session_ending} for reason '{reason}'")
        
        save_result = zoho_manager.save_chat_transcript_sync(session, reason)
        
        if save_result.get("success"):
            # Only set timeout_saved_to_crm for actual timeouts
            if any(keyword in reason.lower() for keyword in ['timeout', 'inactivity', 'expired']):
                session.timeout_saved_to_crm = True
                logger.info(f"📝 Session {session.session_id[:8]} marked as timeout-saved to CRM")
            else:
                logger.info(f"📝 Session {session.session_id[:8]} emergency-saved to CRM (not timeout)")
            session.last_activity = datetime.now()
            
            # For session-ending reasons, immediately set active = 0
            if is_session_ending:
                session.active = False
                logger.info(f"🔒 Session {session.session_id[:8]} marked as INACTIVE due to session-ending reason: {reason}")
            
            # If we got a contact ID, make sure it's saved
            if save_result.get("contact_id") and not session.zoho_contact_id:
                session.zoho_contact_id = save_result["contact_id"]
                logger.info(f"🔗 Saved contact ID {save_result['contact_id']} to session {session.session_id[:8]}")
            
            db_manager.save_session(session)
            logger.info(f"✅ Background CRM save completed successfully for session {session.session_id[:8]} (PDF: {save_result.get('pdf_attached', False)}, Active: {session.active})")
        else:
            logger.error(f"❌ Background CRM save failed for session {session.session_id[:8]}: {save_result.get('reason', 'unknown')}")
            
            # Even if CRM save fails, still end session for session-ending reasons
            if is_session_ending:
                session.active = False
                session.last_activity = datetime.now()
                db_manager.save_session(session)
                logger.info(f"🔒 Session {session.session_id[:8]} marked as INACTIVE despite CRM save failure (session-ending reason: {reason})")
            
    except Exception as e:
        logger.critical(f"❌ Critical error in background CRM save task for session {session.session_id[:8]}: {e}", exc_info=True)
        
        # Even on critical error, still end session for session-ending reasons
        try:
            if is_session_ending_reason(reason):
                session.active = False
                session.last_activity = datetime.now()
                db_manager.save_session(session)
                logger.info(f"🔒 Session {session.session_id[:8]} marked as INACTIVE after critical error (session-ending reason: {reason})")
        except Exception as fallback_error:
            logger.critical(f"❌ Failed to end session even after critical error: {fallback_error}")

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "FiFi Emergency API - MINIMAL COLD START FIX (Preserves Working CRM)",
        "status": "running",
        "version": "3.2.2-cold-start-fixed",
        "fixes_applied": [
            "CRITICAL FIX: Removed forced cold start reconnection that was causing hangs",
            "PRESERVED: All working CRM functionality with PDF attachments",
            "PRESERVED: Working socket error resilience",
            "PRESERVED: Working background task processing"
        ],
        "working_features_preserved": [
            "SQLite Cloud API Key Authentication",
            "Socket Error Resilience", 
            "Automatic Connection Recovery",
            "Session Cleanup Endpoint",
            "Enhanced Contact ID Tracking",
            "Comprehensive Error Handling",
            "Fallback Storage Support",
            "Working PDF Attachments",
            "Background Task Processing"
        ],
        "cold_start_fix": "Removed problematic forced reconnection on container startup",
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
            "sqlitecloud_sdk": "available" if SQLITECLOUD_AVAILABLE else "not_available",
            "features": {
                "api_key_auth": True,
                "socket_resilience": True,
                "auto_reconnect": True,
                "fallback_storage": True,
                "pdf_attachments": True,
                "session_cleanup": True,
                "immediate_session_ending": True,
                "cold_start_fixed": True
            }
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

@app.get("/diagnostics")
async def comprehensive_diagnostics():
    """Complete diagnostic endpoint with all checks"""
    try:
        diagnostics = {
            "timestamp": datetime.now(),
            "version": "3.2.2-cold-start-fixed",
            "environment": {
                "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING",
                "ZOHO_ENABLED": ZOHO_ENABLED,
                "SQLITECLOUD_AVAILABLE": SQLITECLOUD_AVAILABLE
            },
            "database_status": {},
            "connection_resilience": {},
            "error_summary": []
        }
        
        # Database connection test
        db_status = db_manager.test_connection()
        diagnostics["database_status"] = db_status
        
        # Connection resilience status
        diagnostics["connection_resilience"] = {
            "socket_errors_detected": getattr(db_manager, '_consecutive_socket_errors', 0),
            "last_socket_error": getattr(db_manager, '_last_socket_error', None),
            "auth_method": getattr(db_manager, '_auth_method', 'unknown'),
            "connection_attempts": getattr(db_manager, '_connection_attempts', 0),
            "current_db_type": getattr(db_manager, 'db_type', 'unknown'),
            "cold_start_fix_applied": True
        }
        
        # If there are socket errors or connection issues, add to error summary
        if getattr(db_manager, '_consecutive_socket_errors', 0) > 0:
            diagnostics["error_summary"].append({
                "type": "SOCKET_CONNECTIVITY",
                "severity": "HIGH" if db_manager._consecutive_socket_errors > 2 else "MEDIUM",
                "message": f"{db_manager._consecutive_socket_errors} consecutive socket errors detected",
                "recommendation": "Check network connectivity between Cloud Run and SQLite Cloud"
            })
        
        if db_status.get("status") != "healthy":
            diagnostics["error_summary"].append({
                "type": "DATABASE_CONNECTION",
                "severity": "HIGH",
                "message": db_status.get("message", "Database connection issues"),
                "recommendation": "Check database configuration and connectivity"
            })
        
        if not diagnostics["error_summary"]:
            diagnostics["error_summary"].append({
                "type": "ALL_SYSTEMS_OPERATIONAL",
                "severity": "INFO",
                "message": "All systems functioning normally with cold start fix applied",
                "recommendation": "No action required"
            })
        
        return diagnostics
        
    except Exception as e:
        logger.error(f"❌ Comprehensive diagnostics failed: {e}", exc_info=True)
        return {
            "timestamp": datetime.now(),
            "error": str(e),
            "error_type": type(e).__name__,
            "status": "diagnostics_failed"
        }

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions():
    """Clean up expired sessions - called by Google Cloud Scheduler"""
    try:
        logger.info("🧹 Cleanup endpoint called by scheduler")
        
        # Clean up sessions that have been inactive for more than 15 minutes
        cleanup_result = db_manager.cleanup_expired_sessions(expiry_minutes=15)
        
        if cleanup_result["success"]:
            logger.info(f"✅ Cleanup completed: {cleanup_result['cleaned_up_count']} sessions marked as inactive")
        else:
            logger.error(f"❌ Cleanup failed: {cleanup_result.get('message', 'Unknown error')}")
        
        return {
            "success": cleanup_result["success"],
            "timestamp": datetime.now(),
            "result": cleanup_result
        }
        
    except Exception as e:
        logger.error(f"❌ Cleanup endpoint failed: {e}", exc_info=True)
        return {
            "success": False,
            "timestamp": datetime.now(),
            "error": str(e),
            "error_type": type(e).__name__
        }

# KEPT: Working Emergency Save Endpoint with Background Tasks
@app.post("/emergency-save")
async def emergency_save(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    """MINIMAL FIX: Preserved working CRM functionality, only fixed cold start hanging"""
    
    try:
        logger.info(f"🚨 COLD START FIXED EMERGENCY SAVE: Request for session {request.session_id[:8]}, reason: {request.reason}")
        
        # Enhanced database status check
        db_status = db_manager.test_connection()
        logger.info(f"📊 Database status: {db_status.get('status', 'unknown')}")
        
        if db_status["status"] not in ["healthy", "connection_ok_functionality_failed"]:
            logger.error(f"❌ Database is not healthy: {db_status}")
            return {
                "success": False,
                "message": f"Database connection issue: {db_status.get('message', 'Unknown database error')}",
                "session_id": request.session_id,
                "reason": "database_unhealthy",
                "db_status": db_status,
                "timestamp": datetime.now()
            }
        
        # Attempt to load the session with complete resilience
        logger.info(f"🔍 Loading session {request.session_id[:8]} with complete resilience...")
        session = db_manager.load_session(request.session_id)
        
        if not session:
            logger.error(f"❌ Session {request.session_id[:8]} not found or not active")
            
            return {
                "success": False,
                "message": "Session not found or not active. This could indicate the session expired, was never created, or there are database connectivity issues.",
                "session_id": request.session_id,
                "reason": "session_not_found",
                "db_status": db_status,
                "timestamp": datetime.now(),
                "suggestions": [
                    "Verify the session was created in the Streamlit app",
                    "Check if the session expired due to inactivity (15+ minutes)",
                    "Confirm database connectivity is stable",
                    "Check if the session was manually closed"
                ],
                "debug_info": {
                    "db_type": getattr(db_manager, 'db_type', 'unknown'),
                    "socket_errors": getattr(db_manager, '_consecutive_socket_errors', 0),
                    "auth_method": getattr(db_manager, '_auth_method', 'unknown')
                }
            }
        
        logger.info(f"✅ Session {session.session_id[:8]} loaded successfully:")
        logger.info(f"   - Email: {'SET' if session.email else 'NOT_SET'}")
        logger.info(f"   - User Type: {session.user_type.value}")
        logger.info(f"   - Messages: {len(session.messages)}")
        logger.info(f"   - Daily Questions: {session.daily_question_count}")
        logger.info(f"   - Already Saved to CRM: {session.timeout_saved_to_crm}")
        logger.info(f"   - Zoho Contact ID: {'SET' if session.zoho_contact_id else 'NOT_SET'}")
        
        # Check CRM eligibility with emergency save flag
        if not is_crm_eligible(session, is_emergency_save=True):
            logger.info(f"ℹ️ Session {request.session_id[:8]} not eligible for CRM save")
            return {
                "success": False,
                "message": "Session not eligible for CRM save based on usage criteria",
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
                    "session_age_minutes": (datetime.now() - session.created_at).total_seconds() / 60,
                    "emergency_save": True
                }
            }
        
        # Check if already saved
        if session.timeout_saved_to_crm:
            logger.info(f"ℹ️ Session {request.session_id[:8]} already saved to CRM")
            return {
                "success": True,
                "message": "Session already saved to CRM, no action needed.",
                "session_id": request.session_id,
                "reason": "already_saved",
                "timestamp": datetime.now()
            }

        # KEPT: Queue CRM save in background with session-ending detection
        logger.info(f"📝 Queuing WORKING emergency CRM save with PDF attachment for session {request.session_id[:8]}...")
        
        # Check if this is a session-ending reason for proper handling
        is_session_ending = is_session_ending_reason(request.reason)
        logger.info(f"📋 Emergency save type: {'SESSION-ENDING' if is_session_ending else 'NON-SESSION-ENDING'} for reason '{request.reason}'")
        
        background_tasks.add_task(
            _perform_emergency_crm_save,
            session,
            f"Cold Start Fixed Emergency Save: {request.reason}"
        )
        
        logger.info(f"✅ WORKING emergency save queued successfully for {request.session_id[:8]}")
        return {
            "success": True,
            "message": f"Emergency save with PDF attachment queued successfully ({'session will be closed' if is_session_ending else 'session remains active'})",
            "session_id": request.session_id,
            "reason": request.reason,
            "queued_for_background_processing": True,
            "session_ending": is_session_ending,
            "timestamp": datetime.now(),
            "session_info": {
                "user_type": session.user_type.value,
                "message_count": len(session.messages),
                "daily_questions": session.daily_question_count,
                "has_zoho_contact_id": bool(session.zoho_contact_id)
            },
            "resilience_info": {
                "socket_errors_recovered": getattr(db_manager, '_consecutive_socket_errors', 0),
                "db_type": getattr(db_manager, 'db_type', 'unknown'),
                "auth_method": getattr(db_manager, '_auth_method', 'unknown'),
                "cold_start_fix_applied": True
            },
            "features": {
                "pdf_attachment": True,
                "contact_id_tracking": True,
                "session_cleanup": True,
                "immediate_session_ending": is_session_ending,
                "working_crm_preserved": True
            }
        }
            
    except Exception as e:
        logger.critical(f"❌ Critical error in emergency_save for session {request.session_id[:8]}: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Internal server error during emergency save: {str(e)}",
            "session_id": request.session_id,
            "reason": "internal_error",
            "error_type": type(e).__name__,
            "timestamp": datetime.now()
        }

# Legacy endpoint support (for backwards compatibility)
@app.post("/emergency-save-resilient")
async def emergency_save_resilient(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    """Legacy endpoint - redirects to main emergency save"""
    logger.info("🔄 Legacy endpoint called, redirecting to main emergency save")
    return await emergency_save(request, background_tasks)

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting FiFi Emergency API - MINIMAL COLD START FIX (Preserves Working CRM)...")
    logger.info("🔑 Features: PRESERVED Working CRM + PDF Attachments + Session Ending + FIXED Cold Start")
    uvicorn.run(app, host="0.0.0.0", port=8000)
