from fastapi import FastAPI, HTTPException
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
import signal
from contextlib import contextmanager
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

# Cloud Run optimization settings
MAX_REQUEST_TIMEOUT = int(os.getenv("MAX_REQUEST_TIMEOUT", "60"))
DB_OPERATION_TIMEOUT = int(os.getenv("DB_OPERATION_TIMEOUT", "10"))
CRM_OPERATION_TIMEOUT = int(os.getenv("CRM_OPERATION_TIMEOUT", "45"))

# FastAPI app with optimized startup
app = FastAPI(title="FiFi Emergency API - Cloud Run Optimized", version="4.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# OPTIMIZED: Lightweight startup events
@app.on_event("startup")
async def startup_event():
    """FIXED: Lightweight startup - no heavy database operations"""
    logger.info("🚀 FastAPI startup - Cloud Run optimized")
    logger.info(f"🔧 Timeouts: Request={MAX_REQUEST_TIMEOUT}s, DB={DB_OPERATION_TIMEOUT}s, CRM={CRM_OPERATION_TIMEOUT}s")
    logger.info("✅ Startup completed - database connections will be established on first request")

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

# Timeout context manager
@contextmanager
def timeout_context(seconds):
    """Context manager for operation timeouts"""
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")
    
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

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

# OPTIMIZED: Cloud Run Resilient Database Manager
class CloudRunOptimizedDatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock()
        self.conn = None
        self.connection_string = connection_string
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=2)
        self._connection_attempts = 0
        self._max_connection_attempts = 3  # Reduced for faster failover
        self._auth_method = None
        
        logger.info("🔄 INITIALIZING CLOUD RUN OPTIMIZED DATABASE MANAGER")
        logger.info("🚀 Features: Fast Cold Start + Timeout Protection + Socket Error Resilience")
        
        self.db_type = "memory"
        self.local_sessions = {}

        # Analyze connection string format
        if connection_string:
            self._analyze_connection_string()
        
        # DON'T attempt connections during initialization - do it on demand
        logger.info("✅ Database manager initialized - connections will be established on first request")

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

    def _attempt_fast_cloud_connection(self):
        """OPTIMIZED: Fast connection attempt with reduced retries"""
        for attempt in range(self._max_connection_attempts):
            try:
                logger.info(f"🔄 Fast SQLite Cloud connection attempt {attempt + 1}/{self._max_connection_attempts}")
                
                # Close any existing connection first
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
                
                # Create fresh connection with timeout
                logger.info(f"🔗 Connecting to: {self.connection_string[:50]}...")
                self.conn = sqlitecloud.connect(self.connection_string)
                
                # Quick connection test
                test_result = self.conn.execute("SELECT 1 as connection_test").fetchone()
                
                if test_result and test_result[0] == 1:
                    logger.info(f"✅ SQLite Cloud connection established using {self._auth_method}!")
                    self.db_type = "cloud"
                    self._connection_attempts = 0
                    return
                else:
                    raise Exception(f"Connection test failed - unexpected result: {test_result}")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ SQLite Cloud connection attempt {attempt + 1} failed: {error_msg}")
                
                # Clean up failed connection
                if self.conn:
                    try:
                        self.conn.close()
                    except:
                        pass
                    self.conn = None
                
                # Reduced wait time for faster failover
                if attempt < self._max_connection_attempts - 1:
                    wait_time = min(2, 2 ** attempt)  # Max 2 seconds wait
                    time.sleep(wait_time)
        
        logger.error(f"❌ All {self._max_connection_attempts} SQLite Cloud connection attempts failed")
        self._connection_attempts = self._max_connection_attempts

    def _attempt_local_connection(self):
        """Fallback to local SQLite"""
        try:
            logger.info("🔄 Attempting local SQLite connection as fallback...")
            self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
            
            # Test connection
            test_result = self.conn.execute("SELECT 1 as test_local").fetchone()
            if test_result and test_result[0] == 1:
                logger.info(f"✅ Local SQLite connection established!")
                self.db_type = "file"
            else:
                raise Exception(f"Local connection test failed: {test_result}")
                
        except Exception as e:
            logger.error(f"❌ Local SQLite connection failed: {e}")
            self._fallback_to_memory()

    def _fallback_to_memory(self):
        """Fallback to in-memory storage"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        self.conn = None
        self.db_type = "memory"
        logger.warning("⚠️ Operating in in-memory mode")

    def _init_fast_database(self):
        """OPTIMIZED: Fast database initialization with minimal schema"""
        with self.lock:
            try:
                logger.info("🏗️ Creating streamlined database schema...")
                
                # Set timeout for faster operations
                try:
                    self.conn.execute("PRAGMA busy_timeout = 10000")  # 10 second timeout
                    self.conn.execute("PRAGMA journal_mode = WAL")    # Faster writes
                except:
                    pass

                # Create sessions table with essential columns only
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
                        timeout_saved_to_crm INTEGER DEFAULT 0,
                        fingerprint_id TEXT,
                        daily_question_count INTEGER DEFAULT 0,
                        total_question_count INTEGER DEFAULT 0,
                        last_question_time TEXT,
                        question_limit_reached INTEGER DEFAULT 0,
                        ban_status TEXT DEFAULT 'none',
                        visitor_type TEXT DEFAULT 'new_visitor'
                    )
                ''')
                
                # Create only essential index
                try:
                    self.conn.execute("CREATE INDEX IF NOT EXISTS idx_session_lookup ON sessions(session_id, active)")
                except Exception as idx_error:
                    logger.warning(f"⚠️ Index creation skipped: {idx_error}")
                
                # Quick commit
                self.conn.commit()
                logger.info("✅ Streamlined database schema created successfully")
                
            except Exception as e:
                logger.error(f"❌ Database schema creation failed: {e}")
                raise

    def _check_connection_health(self) -> bool:
        """Fast health check"""
        if self.db_type == "memory":
            return True
            
        if not self.conn:
            return False
            
        # Skip frequent health checks for performance
        now = datetime.now()
        if (self._last_health_check and 
            now - self._last_health_check < self._health_check_interval):
            return True
            
        try:
            result = self.conn.execute("SELECT 1 as health_check").fetchone()
            if result and result[0] == 1:
                self._last_health_check = now
                return True
            else:
                logger.error(f"❌ Database health check failed - unexpected result: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            self.conn = None
            return False

    def _ensure_connection(self):
        """OPTIMIZED: Fast connection management without forced cold start reconnection"""
        
        if self._check_connection_health():
            return
            
        logger.warning("⚠️ Database connection unhealthy, attempting to restore...")
        
        # Clean up old connection
        old_conn = self.conn
        self.conn = None
        
        if old_conn:
            try:
                old_conn.close()
            except Exception as e:
                logger.debug(f"⚠️ Error closing old connection: {e}")
        
        # Attempt reconnection
        if self.connection_string and SQLITECLOUD_AVAILABLE:
            logger.info("🔄 Attempting to restore SQLite Cloud connection...")
            self._attempt_fast_cloud_connection()
            
            if self.conn:
                try:
                    self._init_fast_database()
                    logger.info("✅ SQLite Cloud connection restored!")
                    return
                except Exception as e:
                    logger.error(f"❌ Schema initialization failed: {e}")
                    self._fallback_to_memory()
                    return
        elif self.db_type == "file":
            logger.info("🔄 Attempting to restore local SQLite connection...")
            self._attempt_local_connection()
            
            if self.conn:
                try:
                    self._init_fast_database()
                    logger.info("✅ Local SQLite connection restored!")
                    return
                except Exception as e:
                    logger.error(f"❌ Schema initialization failed: {e}")
                    self._fallback_to_memory()
                    return
        
        # If all else fails
        logger.critical("🚨 All connection attempts failed, falling back to in-memory storage")
        self._fallback_to_memory()

    def _execute_with_timeout_retry(self, query: str, params: tuple = None, max_retries: int = 2):
        """Execute query with timeout and retry"""
        for attempt in range(max_retries):
            try:
                self._ensure_connection()
                
                if self.db_type == "memory":
                    raise Exception("Cannot execute SQL in memory mode")
                
                if not self.conn:
                    raise Exception("No database connection available")
                
                # Execute with timeout protection
                if params:
                    result = self.conn.execute(query, params)
                else:
                    result = self.conn.execute(query)
                
                return result
                
            except Exception as e:
                if attempt < max_retries - 1 and self._is_socket_error(e):
                    logger.error(f"🔌 Socket error during query (attempt {attempt + 1}): {e}")
                    self.conn = None
                    time.sleep(1)  # Short wait
                    continue
                raise

    def test_connection(self) -> Dict[str, Any]:
        """Fast connection test"""
        try:
            with self.lock:
                self._ensure_connection()
                
                result = {
                    "timestamp": datetime.now(),
                    "auth_method": self._auth_method,
                    "connection_attempts": self._connection_attempts
                }
                
                if self.db_type == "memory":
                    return {
                        **result,
                        "status": "healthy",
                        "type": "memory",
                        "session_count": len(self.local_sessions),
                        "message": "Running in memory mode"
                    }
                
                if not self.conn:
                    return {
                        **result,
                        "status": "failed",
                        "type": self.db_type,
                        "message": "No database connection available"
                    }
                
                try:
                    # Quick connectivity test
                    basic_result = self.conn.execute("SELECT 1 as connectivity_test").fetchone()
                    if not basic_result or basic_result[0] != 1:
                        raise Exception(f"Connectivity test failed: {basic_result}")
                    
                    # Quick sessions table check
                    try:
                        count_result = self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
                        active_count_result = self.conn.execute("SELECT COUNT(*) FROM sessions WHERE active = 1").fetchone()
                        result["sessions"] = {
                            "total": count_result[0] if count_result else 0,
                            "active": active_count_result[0] if active_count_result else 0
                        }
                    except Exception:
                        result["sessions"] = "table_check_failed"
                    
                    return {
                        **result,
                        "status": "healthy",
                        "type": self.db_type,
                        "message": f"Connected to {self.db_type} database using {self._auth_method}"
                    }
                    
                except Exception as test_error:
                    return {
                        **result,
                        "status": "connection_ok_functionality_failed",
                        "type": self.db_type,
                        "message": f"Connection OK but functionality test failed: {str(test_error)}"
                    }
                    
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return {
                "status": "critical_failure",
                "type": getattr(self, 'db_type', 'unknown'),
                "message": f"Connection test failed: {str(e)}"
            }

    def load_session(self, session_id: str) -> Optional[UserSession]:
        """Load session with timeout protection"""
        with self.lock:
            logger.debug(f"🔍 Loading session {session_id[:8]}...")
            
            if self.db_type == "memory":
                session = self.local_sessions.get(session_id)
                if session and isinstance(session.user_type, str):
                    try:
                        session.user_type = UserType(session.user_type)
                    except ValueError:
                        session.user_type = UserType.GUEST
                return copy.deepcopy(session) if session else None
            
            try:
                cursor = self._execute_with_timeout_retry("""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, 
                           created_at, last_activity, messages, active, timeout_saved_to_crm,
                           fingerprint_id, daily_question_count, total_question_count, 
                           last_question_time, question_limit_reached, ban_status, visitor_type
                    FROM sessions WHERE session_id = ? AND active = 1
                """, (session_id,))
                
                row = cursor.fetchone()
                
                if not row:
                    logger.info(f"❌ No active session found for {session_id[:8]}")
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
                        timeout_saved_to_crm=bool(row[9]),
                        fingerprint_id=row[10],
                        daily_question_count=row[11] or 0,
                        total_question_count=row[12] or 0,
                        last_question_time=datetime.fromisoformat(row[13]) if row[13] else None,
                        question_limit_reached=bool(row[14]),
                        ban_status=BanStatus(row[15]) if row[15] else BanStatus.NONE,
                        visitor_type=row[16] or 'new_visitor',
                        # Set defaults for missing fields
                        wp_token=None,
                        fingerprint_method=None,
                        recognition_response=None,
                        ban_start_time=None,
                        ban_end_time=None,
                        ban_reason=None,
                        evasion_count=0,
                        current_penalty_hours=0,
                        escalation_level=0,
                        email_addresses_used=[],
                        email_switches_count=0,
                        browser_privacy_level=None,
                        registration_prompted=False,
                        registration_link_clicked=False
                    )
                    
                    logger.info(f"✅ Successfully loaded session {session_id[:8]}")
                    return user_session
                    
                except Exception as e:
                    logger.error(f"❌ Failed to create UserSession object: {e}")
                    return None
                    
            except Exception as e:
                logger.error(f"❌ Failed to load session {session_id[:8]}: {e}")
                return None

    def save_session(self, session: UserSession):
        """Save session with timeout protection"""
        with self.lock:
            if self.db_type == "memory":
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.debug(f"💾 Saved session {session.session_id[:8]} to memory")
                return
            
            try:
                # Prepare JSON data
                try:
                    json_messages = json.dumps(session.messages)
                except (TypeError, ValueError) as e:
                    logger.error(f"❌ Session data not JSON serializable: {e}")
                    json_messages = "[]"
                
                # Use simple INSERT OR REPLACE for essential fields
                self._execute_with_timeout_retry('''
                    INSERT OR REPLACE INTO sessions (
                        session_id, user_type, email, full_name, zoho_contact_id, 
                        created_at, last_activity, messages, active, timeout_saved_to_crm,
                        fingerprint_id, daily_question_count, total_question_count, 
                        last_question_time, question_limit_reached, ban_status, visitor_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    session.session_id, session.user_type.value, session.email, session.full_name,
                    session.zoho_contact_id, session.created_at.isoformat(),
                    session.last_activity.isoformat(), json_messages, int(session.active),
                    int(session.timeout_saved_to_crm), session.fingerprint_id,
                    session.daily_question_count, session.total_question_count,
                    session.last_question_time.isoformat() if session.last_question_time else None,
                    int(session.question_limit_reached), session.ban_status.value,
                    session.visitor_type
                ))
                
                self.conn.commit()
                logger.debug(f"✅ Successfully saved session {session.session_id[:8]} to database")
                
            except Exception as e:
                logger.error(f"❌ Failed to save session {session.session_id[:8]}: {e}")
                # Fallback to memory
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback")

    def cleanup_expired_sessions(self, expiry_minutes: int = 15) -> Dict[str, Any]:
        """Clean up expired sessions"""
        with self.lock:
            logger.info(f"🧹 Starting cleanup of sessions expired more than {expiry_minutes} minutes ago...")
            
            if self.db_type == "memory":
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                expired_sessions = []
                
                for session_id, session in list(self.local_sessions.items()):
                    if session.active and session.last_activity < cutoff_time:
                        session.active = False
                        expired_sessions.append(session_id)
                
                return {
                    "success": True,
                    "cleaned_up_count": len(expired_sessions),
                    "storage_type": "memory"
                }
            
            try:
                self._ensure_connection()
                
                if not self.conn:
                    return {
                        "success": False,
                        "error": "No database connection",
                        "storage_type": self.db_type
                    }
                
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                cutoff_iso = cutoff_time.isoformat()
                
                # Find and update expired sessions
                cursor = self._execute_with_timeout_retry("""
                    SELECT session_id FROM sessions 
                    WHERE active = 1 AND last_activity < ?
                """, (cutoff_iso,))
                
                expired_sessions = cursor.fetchall()
                
                if expired_sessions:
                    self._execute_with_timeout_retry("""
                        UPDATE sessions SET active = 0 
                        WHERE active = 1 AND last_activity < ?
                    """, (cutoff_iso,))
                    
                    self.conn.commit()
                
                return {
                    "success": True,
                    "cleaned_up_count": len(expired_sessions),
                    "storage_type": self.db_type
                }
                
            except Exception as e:
                logger.error(f"❌ Failed to cleanup expired sessions: {e}")
                return {
                    "success": False,
                    "error": str(e),
                    "storage_type": self.db_type
                }

# PDF Exporter
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
                
                if len(content) > 200:
                    content = content[:200] + "..."
                    
                style = self.styles['UserMessage'] if role == 'User' else self.styles['Normal']
                story.append(Spacer(1, 8))
                story.append(Paragraph(f"<b>{role}:</b> {content}", style))
                
            doc.build(story)
            buffer.seek(0)
            return buffer
        except Exception as e:
            logger.error(f"❌ PDF generation failed: {e}")
            return None

# OPTIMIZED: Zoho CRM Manager with timeout protection
class OptimizedZohoCRMManager:
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token = None
        self._token_expiry = None

    def _get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        if not ZOHO_ENABLED:
            return None

        if not force_refresh and self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._access_token
        
        logger.info("🔑 Requesting Zoho access token...")
        try:
            response = requests.post(
                "https://accounts.zoho.com/oauth/v2/token",
                data={
                    'refresh_token': ZOHO_REFRESH_TOKEN,
                    'client_id': ZOHO_CLIENT_ID,
                    'client_secret': ZOHO_CLIENT_SECRET,
                    'grant_type': 'refresh_token'
                },
                timeout=10  # Reduced timeout
            )
            response.raise_for_status()
            data = response.json()
            
            self._access_token = data.get('access_token')
            self._token_expiry = datetime.now() + timedelta(minutes=50)
            logger.info("✅ Zoho access token obtained")
            return self._access_token
        except Exception as e:
            logger.error(f"❌ Failed to get Zoho access token: {e}")
            return None

    def _find_contact_by_email(self, email: str) -> Optional[str]:
        access_token = self._get_access_token()
        if not access_token:
            return None
        
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
            params = {'criteria': f'(Email:equals:{email})'}
            response = requests.get(f"{self.base_url}/Contacts/search", headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                contact_id = data['data'][0]['id']
                logger.info(f"✅ Found Zoho contact: {contact_id}")
                return contact_id
            return None
        except Exception as e:
            logger.error(f"❌ Error finding contact: {e}")
        return None

    def _create_contact(self, email: str, full_name: Optional[str]) -> Optional[str]:
        access_token = self._get_access_token()
        if not access_token:
            return None

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
                logger.info(f"✅ Created Zoho contact: {contact_id}")
                return contact_id
            return None
        except Exception as e:
            logger.error(f"❌ Error creating contact: {e}")
        return None

    def _add_note(self, contact_id: str, note_title: str, note_content: str) -> bool:
        access_token = self._get_access_token()
        if not access_token:
            return False

        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}', 'Content-Type': 'application/json'}
            
            if len(note_content) > 32000:
                note_content = note_content[:32000 - 100] + "\n\n[Content truncated]"
            
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
                logger.info("✅ Zoho note added successfully")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Error adding note: {e}")
            return False

    def _upload_attachment(self, contact_id: str, pdf_buffer: io.BytesIO, filename: str) -> bool:
        """Upload PDF attachment with timeout protection"""
        access_token = self._get_access_token()
        if not access_token:
            return False

        upload_url = f"{self.base_url}/Contacts/{contact_id}/Attachments"
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
                
                pdf_buffer.seek(0)
                response = requests.post(
                    upload_url, 
                    headers=headers, 
                    files={'file': (filename, pdf_buffer.read(), 'application/pdf')},
                    timeout=30  # Reduced timeout
                )
                
                if response.status_code == 401:
                    access_token = self._get_access_token(force_refresh=True)
                    if not access_token: 
                        return False
                    headers['Authorization'] = f'Zoho-oauthtoken {access_token}'
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if 'data' in data and data['data'] and data['data'][0]['code'] == 'SUCCESS':
                    logger.info(f"✅ PDF attachment uploaded: {filename}")
                    return True
                    
            except requests.exceptions.Timeout:
                logger.error(f"⏰ PDF upload timeout (attempt {attempt + 1})")
            except Exception as e:
                logger.error(f"❌ PDF upload error (attempt {attempt + 1}): {e}")
                
            if attempt < max_retries - 1:
                time.sleep(1)
                
        return False

    def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> Dict[str, Any]:
        """OPTIMIZED: Fast CRM save with timeout protection"""
        if not ZOHO_ENABLED:
            return {"success": False, "reason": "zoho_disabled"}
            
        if not session.email:
            return {"success": False, "reason": "no_email"}
            
        if not session.messages:
            return {"success": False, "reason": "no_messages"}
        
        try:
            logger.info(f"🔄 Starting optimized Zoho CRM save for session {session.session_id[:8]}")
            
            # Find or create contact
            contact_id = self._find_contact_by_email(session.email)
            if not contact_id:
                contact_id = self._create_contact(session.email, session.full_name)
            if not contact_id:
                return {"success": False, "reason": "contact_creation_failed"}

            # Update session with contact ID
            if not session.zoho_contact_id:
                session.zoho_contact_id = contact_id

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            note_title = f"FiFi AI Emergency Save - {timestamp} ({trigger_reason})"
            
            # Create optimized note content
            note_content = f"**Emergency Save Information:**\n"
            note_content += f"- Session ID: {session.session_id}\n"
            note_content += f"- User: {session.full_name or 'Unknown'} ({session.email})\n"
            note_content += f"- Save Trigger: {trigger_reason}\n"
            note_content += f"- Timestamp: {timestamp}\n"
            note_content += f"- Total Messages: {len(session.messages)}\n"
            note_content += f"- Questions Asked: {session.daily_question_count}\n\n"
            
            # Add conversation summary (limited for speed)
            note_content += "**Conversation Summary:**\n"
            for i, msg in enumerate(session.messages[:10]):  # Limit to first 10 messages for speed
                role = msg.get("role", "Unknown").capitalize()
                content = re.sub(r'<[^>]+>', '', msg.get("content", ""))
                if len(content) > 150:
                    content = content[:150] + "..."
                note_content += f"{i+1}. **{role}:** {content}\n"
                
            if len(session.messages) > 10:
                note_content += f"\n[{len(session.messages) - 10} additional messages - see PDF attachment]\n"
                
            # Add the note
            note_success = self._add_note(contact_id, note_title, note_content)
            
            # Generate and attach PDF
            pdf_success = False
            try:
                pdf_buffer = self.pdf_exporter.generate_chat_pdf(session)
                if pdf_buffer:
                    pdf_filename = f"FiFi_Chat_{session.session_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    pdf_success = self._upload_attachment(contact_id, pdf_buffer, pdf_filename)
                    pdf_buffer.close()
            except Exception as pdf_error:
                logger.warning(f"⚠️ PDF attachment failed: {pdf_error}")
                
            if note_success:
                logger.info(f"✅ Zoho CRM save successful (Note: {note_success}, PDF: {pdf_success})")
                return {
                    "success": True, 
                    "contact_id": contact_id,
                    "note_created": note_success,
                    "pdf_attached": pdf_success
                }
            else:
                return {"success": False, "reason": "note_creation_failed"}
                
        except Exception as e:
            logger.error(f"❌ CRM save failed: {e}")
            return {"success": False, "reason": "exception", "error": str(e)}

# Initialize managers
logger.info("🚀 Initializing Cloud Run optimized managers...")
db_manager = CloudRunOptimizedDatabaseManager(SQLITE_CLOUD_CONNECTION)
pdf_exporter = PDFExporter()
zoho_manager = OptimizedZohoCRMManager(pdf_exporter)
logger.info("✅ All managers initialized with Cloud Run optimizations")

# Helper functions
def is_crm_eligible(session: UserSession, is_emergency_save: bool = False) -> bool:
    """Check if session is eligible for CRM save"""
    try:
        if not session.email or not session.messages:
            return False
        
        if session.user_type not in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]:
            return False
        
        if session.daily_question_count < 1:
            return False
        
        # Emergency saves bypass time requirement
        if not is_emergency_save:
            start_time = session.created_at
            if session.last_question_time and session.last_question_time < start_time:
                start_time = session.last_question_time
            
            elapsed_time = datetime.now() - start_time
            elapsed_minutes = elapsed_time.total_seconds() / 60
            
            if elapsed_minutes < 15.0:
                return False
            
        return True
    except Exception as e:
        logger.error(f"❌ Error checking CRM eligibility: {e}")
        return False

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "FiFi Emergency API - Cloud Run Optimized",
        "status": "running",
        "version": "4.0.0-cloud-run-optimized",
        "optimizations": [
            "Fast cold start recovery",
            "Timeout protection for all operations", 
            "Streamlined database operations",
            "Reduced connection retries",
            "Memory fallback support",
            "Optimized CRM processing"
        ],
        "features": [
            "SQLite Cloud API Key Authentication",
            "Socket Error Resilience", 
            "Immediate Session Ending",
            "PDF Attachments",
            "Contact ID Tracking",
            "Session Cleanup"
        ],
        "performance": {
            "max_request_timeout": f"{MAX_REQUEST_TIMEOUT}s",
            "db_operation_timeout": f"{DB_OPERATION_TIMEOUT}s", 
            "crm_operation_timeout": f"{CRM_OPERATION_TIMEOUT}s"
        },
        "timestamp": datetime.now()
    }

@app.get("/health")
async def health_check():
    """OPTIMIZED: Fast health check with timeout protection"""
    try:
        with timeout_context(5):
            db_status = db_manager.test_connection()
        
        return {
            "status": "healthy" if db_status.get("status") == "healthy" else "degraded",
            "timestamp": datetime.now(),
            "database": db_status.get("status", "unknown"),
            "zoho": "enabled" if ZOHO_ENABLED else "disabled",
            "features": {
                "cloud_run_optimized": True,
                "timeout_protection": True,
                "fast_cold_start": True,
                "memory_fallback": True
            },
            "performance": {
                "health_check_timeout": "5s",
                "connection_retries": "3",
                "fast_failover": "enabled"
            }
        }
    except TimeoutError:
        return {
            "status": "timeout",
            "timestamp": datetime.now(),
            "message": "Health check timed out"
        }
    except Exception as e:
        return {
            "status": "unhealthy", 
            "timestamp": datetime.now(),
            "error": str(e)
        }

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions():
    """Clean up expired sessions"""
    try:
        logger.info("🧹 Cleanup endpoint called")
        
        with timeout_context(30):  # 30 second timeout for cleanup
            cleanup_result = db_manager.cleanup_expired_sessions(expiry_minutes=15)
        
        return {
            "success": cleanup_result["success"],
            "timestamp": datetime.now(),
            "result": cleanup_result
        }
        
    except TimeoutError:
        return {
            "success": False,
            "timestamp": datetime.now(),
            "error": "Cleanup operation timed out"
        }
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return {
            "success": False,
            "timestamp": datetime.now(),
            "error": str(e)
        }

# OPTIMIZED: Main Emergency Save Endpoint with Full Timeout Protection
@app.post("/emergency-save")
async def emergency_save(request: EmergencySaveRequest):
    """OPTIMIZED: Ultra-fast emergency save with comprehensive timeout protection"""
    
    start_time = time.time()
    
    try:
        logger.info(f"🚨 OPTIMIZED EMERGENCY SAVE: Session {request.session_id[:8]}, reason: {request.reason}")
        
        # Quick database health check with timeout
        try:
            with timeout_context(DB_OPERATION_TIMEOUT):
                db_status = db_manager.test_connection()
        except TimeoutError:
            logger.error("❌ Database health check timed out")
            return {
                "success": False,
                "message": "Database health check timed out during cold start",
                "session_id": request.session_id,
                "reason": "db_timeout",
                "timestamp": datetime.now(),
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
        
        if db_status["status"] not in ["healthy", "connection_ok_functionality_failed"]:
            return {
                "success": False,
                "message": f"Database not ready: {db_status.get('message', 'Unknown error')}",
                "session_id": request.session_id,
                "reason": "database_not_ready",
                "timestamp": datetime.now(),
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
        
        # Load session with timeout
        try:
            with timeout_context(DB_OPERATION_TIMEOUT):
                session = db_manager.load_session(request.session_id)
        except TimeoutError:
            logger.error(f"❌ Session load timed out for {request.session_id[:8]}")
            return {
                "success": False,
                "message": "Session load timed out",
                "session_id": request.session_id,
                "reason": "session_load_timeout",
                "timestamp": datetime.now(),
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
        
        if not session:
            return {
                "success": False,
                "message": "Session not found or not active",
                "session_id": request.session_id,
                "reason": "session_not_found",
                "timestamp": datetime.now(),
                "suggestions": [
                    "Verify session was created in Streamlit app",
                    "Check if session expired (15+ minutes inactive)",
                    "Confirm database connectivity"
                ],
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
        
        logger.info(f"✅ Session {session.session_id[:8]} loaded: Email={'SET' if session.email else 'NOT_SET'}, Type={session.user_type.value}, Messages={len(session.messages)}, Questions={session.daily_question_count}")
        
        # Quick eligibility check
        if not is_crm_eligible(session, is_emergency_save=True):
            return {
                "success": False,
                "message": "Session not eligible for CRM save",
                "session_id": request.session_id,
                "reason": "not_eligible",
                "timestamp": datetime.now(),
                "eligibility_details": {
                    "has_email": bool(session.email),
                    "has_messages": len(session.messages) > 0,
                    "user_type": session.user_type.value,
                    "daily_questions": session.daily_question_count,
                    "emergency_save": True
                },
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
        
        if session.timeout_saved_to_crm:
            return {
                "success": True,
                "message": "Session already saved to CRM",
                "session_id": request.session_id,
                "reason": "already_saved",
                "timestamp": datetime.now(),
                "performance": {"elapsed_seconds": time.time() - start_time}
            }

        # Check if session-ending
        is_session_ending = is_session_ending_reason(request.reason)
        logger.info(f"📋 Emergency save type: {'SESSION-ENDING' if is_session_ending else 'NON-SESSION-ENDING'}")
        
        # OPTIMIZED: Fast CRM save with timeout protection
        logger.info(f"📝 Starting FAST CRM save for session {session.session_id[:8]}...")
        
        try:
            with timeout_context(CRM_OPERATION_TIMEOUT):
                save_result = zoho_manager.save_chat_transcript_sync(session, f"Emergency Save: {request.reason}")
        except TimeoutError:
            logger.error(f"❌ CRM save timed out for session {session.session_id[:8]}")
            
            # Still end session if needed
            if is_session_ending:
                session.active = False
                session.last_activity = datetime.now()
                try:
                    with timeout_context(5):
                        db_manager.save_session(session)
                    logger.info(f"🔒 Session {session.session_id[:8]} marked inactive despite timeout")
                except TimeoutError:
                    logger.error("❌ Failed to save session state after CRM timeout")
            
            return {
                "success": False,
                "message": "CRM save timed out but session handled appropriately",
                "session_id": request.session_id,
                "reason": "crm_timeout",
                "session_ending": is_session_ending,
                "session_closed": is_session_ending,
                "timestamp": datetime.now(),
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
        
        # Handle successful save
        if save_result.get("success"):
            session.timeout_saved_to_crm = True
            session.last_activity = datetime.now()
            
            if is_session_ending:
                session.active = False
                logger.info(f"🔒 Session {session.session_id[:8]} marked inactive (reason: {request.reason})")
            
            if save_result.get("contact_id") and not session.zoho_contact_id:
                session.zoho_contact_id = save_result["contact_id"]
            
            # Quick session save with timeout
            try:
                with timeout_context(5):
                    db_manager.save_session(session)
            except TimeoutError:
                logger.warning("⚠️ Session save timed out but CRM save succeeded")
            
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Emergency save completed in {elapsed_time:.2f}s")
            
            return {
                "success": True,
                "message": f"Emergency save completed successfully in {elapsed_time:.2f}s",
                "session_id": request.session_id,
                "reason": request.reason,
                "session_ending": is_session_ending,
                "timestamp": datetime.now(),
                "performance": {
                    "total_time_seconds": elapsed_time,
                    "optimized": True,
                    "timeout_protected": True
                },
                "crm_result": {
                    "contact_id": save_result.get("contact_id"),
                    "note_created": save_result.get("note_created", False),
                    "pdf_attached": save_result.get("pdf_attached", False)
                },
                "session_info": {
                    "user_type": session.user_type.value,
                    "message_count": len(session.messages),
                    "daily_questions": session.daily_question_count,
                    "final_active_status": session.active
                },
                "features": {
                    "cloud_run_optimized": True,
                    "timeout_protection": True,
                    "fast_cold_start": True,
                    "immediate_session_ending": is_session_ending
                }
            }
        else:
            # Handle failed save
            if is_session_ending:
                session.active = False
                session.last_activity = datetime.now()
                try:
                    with timeout_context(5):
                        db_manager.save_session(session)
                    logger.info(f"🔒 Session {session.session_id[:8]} marked inactive despite CRM failure")
                except TimeoutError:
                    logger.error("❌ Failed to save session state after CRM failure")
            
            return {
                "success": False,
                "message": f"CRM save failed: {save_result.get('reason', 'unknown')}",
                "session_id": request.session_id,
                "reason": "crm_save_failed",
                "session_ending": is_session_ending,
                "session_closed": is_session_ending,
                "timestamp": datetime.now(),
                "error_details": save_result,
                "performance": {"elapsed_seconds": time.time() - start_time}
            }
            
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.critical(f"❌ Critical error in emergency save ({elapsed_time:.2f}s): {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Internal error during emergency save: {str(e)}",
            "session_id": request.session_id,
            "reason": "internal_error",
            "error_type": type(e).__name__,
            "timestamp": datetime.now(),
            "performance": {"elapsed_seconds": elapsed_time}
        }

# Legacy endpoint support
@app.post("/emergency-save-resilient")
async def emergency_save_resilient(request: EmergencySaveRequest):
    """Legacy endpoint - redirects to optimized emergency save"""
    logger.info("🔄 Legacy endpoint called, redirecting to optimized emergency save")
    return await emergency_save(request)

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting FiFi Emergency API - Cloud Run Optimized...")
    logger.info("⚡ Features: Ultra-Fast Cold Start + Timeout Protection + Memory Fallback + Optimized CRM")
    uvicorn.run(app, host="0.0.0.0", port=8000)
