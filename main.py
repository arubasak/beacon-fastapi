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

# FastAPI app initialization
app = FastAPI(title="FiFi Emergency API - Async Operations", version="3.6.0-simplified")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://fifi-eu.streamlit.app", "*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)

# OPTIONS handlers for preflight requests
@app.options("/emergency-save")
async def emergency_save_options():
    return {"status": "ok"}

@app.options("/cleanup-expired-sessions")
async def cleanup_options():
    return {"status": "ok"}

# Configuration from environment variables
SQLITE_CLOUD_CONNECTION = os.getenv("SQLITE_CLOUD_CONNECTION")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ENABLED = all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN])

# Optional imports check
SQLITECLOUD_AVAILABLE = False
try:
    import sqlitecloud
    SQLITECLOUD_AVAILABLE = True
    logger.info("✅ sqlitecloud SDK detected.")
except ImportError:
    logger.warning("❌ SQLiteCloud SDK not available. Using local SQLite fallback.")

# Global managers (initialized in startup event)
db_manager: 'ResilientDatabaseManager' = None
pdf_exporter: 'PDFExporter' = None
zoho_manager: 'ZohoCRMManager' = None

# FastAPI startup event for heavy initialization
@app.on_event("startup")
async def startup_event():
    global db_manager, pdf_exporter, zoho_manager
    logger.info("🚀 FastAPI startup event triggered - Initializing managers asynchronously...")

    pdf_exporter = PDFExporter()
    db_manager = ResilientDatabaseManager(SQLITE_CLOUD_CONNECTION)
    zoho_manager = ZohoCRMManager(pdf_exporter)

    try:
        logger.info("Attempting initial database connection test during startup (max 1s)...")
        await asyncio.wait_for(db_manager.test_connection(), timeout=1) 
        logger.info("Initial DB connection test in startup completed successfully.")
    except asyncio.TimeoutError:
        logger.warning("Initial DB connection test in startup timed out. Proceeding with application startup, DB will be in fallback memory mode.")
        await asyncio.to_thread(db_manager._fallback_to_memory_sync) 
    except Exception as e:
        logger.error(f"Error during initial DB connection test in startup: {e}", exc_info=True)
        await asyncio.to_thread(db_manager._fallback_to_memory_sync) 

    logger.info("✅ FastAPI startup initialization complete. App is ready to receive requests.")
    logger.info(f"🔧 CONFIG CHECK (after startup): SQLite Cloud: {'SET' if SQLITE_CLOUD_CONNECTION else 'MISSING'}, Zoho Enabled: {ZOHO_ENABLED}")


# Pydantic Models & DataClasses
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
    display_message_offset: int = 0
    reverification_pending: bool = False
    pending_user_type: Optional[UserType] = None
    pending_email: Optional[str] = None
    pending_full_name: Optional[str] = None
    pending_zoho_contact_id: Optional[str] = None
    pending_wp_token: Optional[str] = None

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
    session_ending_keywords = ['beforeunload', 'unload', 'close', 'refresh', 'timeout', 'parent_beforeunload', 'browser_close', 'tab_close', 'window_close', 'page_refresh', 'browser_refresh', 'session_timeout', 'inactivity']
    return any(keyword in reason.lower() for keyword in session_ending_keywords)

# Resilient Database Manager
class ResilientDatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock()
        self.conn = None
        self.connection_string = connection_string
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=2)
        self._connection_attempts = 0
        self._max_connection_attempts = 3
        self._consecutive_socket_errors = 0
        self._auth_method = None
        self.db_type = "memory"
        self.local_sessions = {}
        self._initialized_schema = False
        self._initialization_attempted_in_session = False

        logger.info("🔄 ResilientDatabaseManager initialized (LAZY ASYNC)")
        if connection_string: self._analyze_connection_string()

    def _analyze_connection_string(self):
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.connection_string)
            query_params = parse_qs(parsed.query)
            self._auth_method = "API_KEY" if 'apikey' in query_params else "USERNAME_PASSWORD" if parsed.username else "UNKNOWN"
        except Exception as e:
            logger.error(f"❌ Failed to analyze connection string: {e}")
            self._auth_method = "PARSE_ERROR"

    async def _ensure_connection(self, max_wait_seconds: int = 30):
        start_time = datetime.now()
        
        # Reuse healthy connection
        if self.conn and self.db_type != "memory" and await asyncio.to_thread(self._check_connection_health_sync):
            if not self._initialized_schema:
                try:
                    await asyncio.to_thread(self._init_complete_database_sync)
                    self._initialized_schema = True
                except Exception as e:
                    logger.error(f"❌ Schema check on reuse failed: {e}. Forcing re-connection.", exc_info=True)
                    self.conn = None; self.db_type = "memory"; self._initialized_schema = False
            if self.conn: return True

        # Handle memory mode or recent failures
        if self.db_type == "memory" and (datetime.now() - start_time).total_seconds() >= max_wait_seconds:
            logger.warning("⏰ Already in memory mode and out of time.")
            return True
        if self._initialization_attempted_in_session and (datetime.now() - self._last_health_check if self._last_health_check else timedelta(0)).total_seconds() < 5:
            logger.warning("⚠️ Recent initialization attempt detected, sticking to current mode.")
            return True

        self._initialization_attempted_in_session = True
        self._last_health_check = datetime.now()
        
        # Close existing connection if any
        if self.conn:
            logger.warning("⚠️ Existing connection unhealthy. Closing.")
            try: await asyncio.to_thread(self.conn.close)
            except Exception as e: logger.debug(f"⚠️ Error closing old DB connection: {e}")
            self.conn = None; self.db_type = "memory"; self._initialized_schema = False

        cloud_conn_successful = False
        if self.connection_string and SQLITECLOUD_AVAILABLE:
            try:
                await self._attempt_quick_cloud_connection_async(max_wait_seconds - (datetime.now() - start_time).total_seconds())
                if self.conn and self.db_type == "cloud": cloud_conn_successful = True
                else: logger.error(f"❌ SQLite Cloud connection attempt finished, but db_type is '{self.db_type}' (expected 'cloud'). Debugging further...")
            except Exception as e:
                logger.error(f"❌ SQLite Cloud connection attempt failed: {e}", exc_info=True)
                self.conn = None; self.db_type = "memory"
        else:
            logger.info(f"ℹ️ Skipping SQLite Cloud connection. String set: {bool(self.connection_string)}, SDK available: {SQLITECLOUD_AVAILABLE}.")

        if not cloud_conn_successful:
            logger.info("☁️ Cloud connection failed/unavailable, trying local SQLite...")
            await asyncio.to_thread(self._attempt_local_connection_sync)
            if self.db_type != "file": logger.error("❌ Local SQLite fallback connection also failed. Defaulting to in-memory.")

        if self.conn and self.db_type != "memory" and not self._initialized_schema:
            try:
                await asyncio.to_thread(self._init_complete_database_sync)
                self._initialized_schema = True
                logger.info("✅ Database schema initialized successfully.")
            except Exception as e:
                logger.error(f"❌ Schema initialization failed: {e}. Falling back to memory.", exc_info=True)
                await asyncio.to_thread(self._fallback_to_memory_sync)
                return True

        if not self.conn or self.db_type == "memory":
            logger.warning("🚨 No persistent connection could be established. Falling back to in-memory storage.")
            await asyncio.to_thread(self._fallback_to_memory_sync)
        return True

    async def _attempt_quick_cloud_connection_async(self, max_wait_seconds: float):
        max_wait_seconds = max(1.0, max_wait_seconds) 
        max_attempts = min(self._max_connection_attempts, 2)
        
        for attempt in range(max_attempts):
            if max_wait_seconds <= 0: logger.warning("⏰ No time left for cloud connection attempts"); return
            try:
                logger.info(f"🔄 QUICK SQLite Cloud connection attempt {attempt + 1}/{max_attempts}")
                if self.conn: await asyncio.to_thread(self.conn.close); self.conn = None
                
                self.conn = await asyncio.to_thread(sqlitecloud.connect, self.connection_string)
                test_result = await asyncio.to_thread(self.conn.execute, "SELECT 1 as connection_test")
                fetched_value = await asyncio.to_thread(test_result.fetchone)
                
                if fetched_value and str(fetched_value[0]) == '1': # Robust check using string conversion
                    logger.info(f"✅ QUICK SQLite Cloud connection established using {self._auth_method}!")
                    self.db_type = "cloud"; self._connection_attempts = 0; self._consecutive_socket_errors = 0
                    return
                else: raise Exception(f"Connection test failed - unexpected result: {fetched_value}")
            except Exception as e:
                logger.error(f"❌ QUICK SQLite Cloud connection attempt {attempt + 1} failed: {e}")
                if self.conn: await asyncio.to_thread(self.conn.close); self.conn = None
                if attempt < max_attempts - 1 and max_wait_seconds > 0.5: await asyncio.sleep(min(2, max_wait_seconds / (max_attempts - attempt)))
        logger.error(f"❌ All {max_attempts} QUICK SQLite Cloud connection attempts failed")
        self.db_type = "memory"

    def _attempt_local_connection_sync(self):
        try:
            logger.info("🔄 Attempting local SQLite connection as fallback...")
            self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
            test_result = self.conn.execute("SELECT 1 as test_local").fetchone()
            if test_result and test_result[0] == 1:
                logger.info(f"✅ Local SQLite connection established!")
                self.db_type = "file"
            else: raise Exception(f"Local connection test failed: {test_result}")
        except Exception as e:
            logger.error(f"❌ Local SQLite connection failed: {e}", exc_info=True)
            self._fallback_to_memory_sync()

    def _fallback_to_memory_sync(self):
        if self.conn: try: self.conn.close()
        except: pass
        self.conn = None; self.db_type = "memory"; self.local_sessions = {}; self._initialized_schema = True
        logger.warning("⚠️ Operating in in-memory mode due to connection issues")

    def _init_complete_database_sync(self):
        try:
            from sqlitecloud.exceptions import SQLiteCloudOperationalError as SQCError
        except ImportError:
            SQCError = type('DummySQCError', (sqlite3.OperationalError,), {})

        if hasattr(self.conn, 'row_factory'): self.conn.row_factory = None
        logger.info("🏗️ Creating database schema...")
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY, user_type TEXT DEFAULT 'guest', email TEXT, full_name TEXT,
                zoho_contact_id TEXT, created_at TEXT DEFAULT '', last_activity TEXT DEFAULT '',
                messages TEXT DEFAULT '[]', active INTEGER DEFAULT 1, wp_token TEXT,
                timeout_saved_to_crm INTEGER DEFAULT 0, fingerprint_id TEXT, fingerprint_method TEXT,
                visitor_type TEXT DEFAULT 'new_visitor', daily_question_count INTEGER DEFAULT 0,
                total_question_count INTEGER DEFAULT 0, last_question_time TEXT,
                question_limit_reached INTEGER DEFAULT 0, ban_status TEXT DEFAULT 'none',
                ban_start_time TEXT, ban_end_time TEXT, ban_reason TEXT, evasion_count INTEGER DEFAULT 0,
                current_penalty_hours INTEGER DEFAULT 0, escalation_level INTEGER DEFAULT 0,
                email_addresses_used TEXT DEFAULT '[]', email_switches_count INTEGER DEFAULT 0,
                browser_privacy_level TEXT, registration_prompted INTEGER DEFAULT 0,
                registration_link_clicked INTEGER DEFAULT 0, recognition_response TEXT,
                display_message_offset INTEGER DEFAULT 0, reverification_pending INTEGER DEFAULT 0,
                pending_user_type TEXT, pending_email TEXT, pending_full_name TEXT,
                pending_zoho_contact_id TEXT, pending_wp_token TEXT
            )
        ''')
        new_columns = [("display_message_offset", "INTEGER DEFAULT 0"), ("reverification_pending", "INTEGER DEFAULT 0"),
                       ("pending_user_type", "TEXT"), ("pending_email", "TEXT"), ("pending_full_name", "TEXT"),
                       ("pending_zoho_contact_id", "TEXT"), ("pending_wp_token", "TEXT")]
        for col_name, col_type in new_columns:
            try: self.conn.execute(f"ALTER TABLE sessions ADD COLUMN {col_name} {col_type}")
            except (sqlite3.OperationalError, SQCError) as e:
                if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower(): logger.debug(f"Column {col_name} already exists.")
                else: raise e
            except Exception as e: logger.error(f"❌ Error adding column {col_name}: {e}", exc_info=True); raise
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_session_lookup ON sessions(session_id, active)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fingerprint_id ON sessions(fingerprint_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON sessions(email)")
        self.conn.commit()
        logger.info("✅ Database schema and indexes created successfully.")
            
    def _check_connection_health_sync(self) -> bool:
        if self.db_type == "memory": return True
        if not self.conn: return False
        now = datetime.now()
        if self._last_health_check and now - self._last_health_check < self._health_check_interval and self._consecutive_socket_errors == 0: return True
        try:
            result = self.conn.execute("SELECT 1 as health_check").fetchone()
            if result and result[0] == 1: self._last_health_check = now; self._consecutive_socket_errors = 0; return True
            else: logger.error(f"❌ Database health check failed - unexpected result: {result}"); return False
        except Exception as e: logger.error(f"❌ Database health check failed: {e}"); self.conn = None; return False

    async def _execute_with_socket_retry_async(self, query: str, params: tuple = None, max_retries: int = 2):
        for attempt in range(max_retries):
            try:
                await self._ensure_connection(15)
                if self.db_type == "memory": raise Exception("Cannot execute SQL when in-memory mode is active for DB operations")
                if not self.conn: raise Exception("No database connection available after _ensure_connection")
                result = await asyncio.to_thread(self.conn.execute, query, params) if params else await asyncio.to_thread(self.conn.execute, query)
                if self._consecutive_socket_errors > 0: logger.info(f"✅ Query executed successfully after {self._consecutive_socket_errors} previous socket errors"); self._consecutive_socket_errors = 0
                return result
            except Exception as e:
                is_socket_error = "socket" in str(e).lower() or "connection" in str(e).lower() or "timeout" in str(e).lower()
                if is_socket_error:
                    self._consecutive_socket_errors += 1; logger.error(f"🔌 Socket error during query (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1: self.conn = None; await asyncio.sleep(1); continue
                else: logger.error(f"❌ Non-socket error during query: {e}")
                if attempt == max_retries - 1 or not is_socket_error: raise

    async def quick_status_check(self) -> Dict[str, Any]:
        return {"timestamp": datetime.now(), "auth_method": self._auth_method, "connection_attempts": self._connection_attempts,
                "socket_errors": self._consecutive_socket_errors, "db_type": self.db_type, "status": "quick_check_only"}

    async def test_connection(self) -> Dict[str, Any]:
        success = await self._ensure_connection(5)
        if not success: return {"status": "init_timeout", "type": self.db_type, "message": "DB init timed out", "timestamp": datetime.now()}
        result = {"timestamp": datetime.now(), "auth_method": self._auth_method, "connection_attempts": self._connection_attempts, "socket_errors": self._consecutive_socket_errors}
        if self.db_type == "memory": return {**result, "status": "healthy", "type": "memory", "session_count": len(self.local_sessions)}
        if not self.conn: return {**result, "status": "failed", "type": self.db_type, "message": "No DB connection available"}
        try:
            basic_result = await asyncio.to_thread(self.conn.execute, "SELECT 1 as connectivity_test")
            fetched_value = await asyncio.to_thread(basic_result.fetchone)
            if not fetched_value or str(fetched_value[0]) != '1': raise Exception(f"Connectivity test failed: {fetched_value}")
            
            sessions_check = await asyncio.to_thread(self.conn.execute, "SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'")
            sessions_check_fetched = await asyncio.to_thread(sessions_check.fetchone)
            if sessions_check_fetched:
                count_result = await asyncio.to_thread(self.conn.execute, "SELECT COUNT(*) FROM sessions")
                total_count = (await asyncio.to_thread(count_result.fetchone))[0]
                active_count_result = await asyncio.to_thread(self.conn.execute, "SELECT COUNT(*) FROM sessions WHERE active = 1")
                active_count = (await asyncio.to_thread(active_count_result.fetchone))[0]
                result["sessions_table"] = {"exists": True, "total_count": total_count, "active_count": active_count}
            else: result["sessions_table"] = {"exists": False}
            return {**result, "status": "healthy", "type": self.db_type, "message": f"Connected to {self.db_type} DB"}
        except Exception as e: return {**result, "status": "func_failed", "type": self.db_type, "message": f"Func test failed: {str(e)}", "error_type": type(e).__name__}

    async def load_session(self, session_id: str) -> Optional[UserSession]:
        with self.lock:
            if not await self._ensure_connection(15): logger.warning(f"⚠️ Conn timeout loading {session_id[:8]}, checking memory");
            if self.db_type == "memory":
                session = self.local_sessions.get(session_id); return copy.deepcopy(session) if session else None
            try:
                # Select all 38 columns as per UserSession dataclass
                cursor = await self._execute_with_socket_retry_async("SELECT session_id, user_type, email, full_name, zoho_contact_id, created_at, last_activity, messages, active, wp_token, timeout_saved_to_crm, fingerprint_id, fingerprint_method, visitor_type, daily_question_count, total_question_count, last_question_time, question_limit_reached, ban_status, ban_start_time, ban_end_time, ban_reason, evasion_count, current_penalty_hours, escalation_level, email_addresses_used, email_switches_count, browser_privacy_level, registration_prompted, registration_link_clicked, recognition_response, display_message_offset, reverification_pending, pending_user_type, pending_email, pending_full_name, pending_zoho_contact_id, pending_wp_token FROM sessions WHERE session_id = ? AND active = 1", (session_id,))
                row = await asyncio.to_thread(cursor.fetchone)
                if not row: return None
                
                # Safely access all fields with defaults for compatibility
                row_dict = {
                    "session_id": row[0], "user_type": UserType(row[1]) if row[1] else UserType.GUEST,
                    "email": row[2], "full_name": row[3], "zoho_contact_id": row[4],
                    "created_at": datetime.fromisoformat(row[5]) if row[5] else datetime.now(),
                    "last_activity": datetime.fromisoformat(row[6]) if row[6] else datetime.now(),
                    "messages": safe_json_loads(row[7], []), "active": bool(row[8]), "wp_token": row[9],
                    "timeout_saved_to_crm": bool(row[10]), "fingerprint_id": row[11],
                    "fingerprint_method": row[12], "visitor_type": row[13] or 'new_visitor',
                    "daily_question_count": row[14] or 0, "total_question_count": row[15] or 0,
                    "last_question_time": datetime.fromisoformat(row[16]) if row[16] else None,
                    "question_limit_reached": bool(row[17]), "ban_status": BanStatus(row[18]) if row[18] else BanStatus.NONE,
                    "ban_start_time": datetime.fromisoformat(row[19]) if row[19] else None,
                    "ban_end_time": datetime.fromisoformat(row[20]) if row[20] else None,
                    "ban_reason": row[21], "evasion_count": row[22] or 0,
                    "current_penalty_hours": row[23] or 0, "escalation_level": row[24] or 0,
                    "email_addresses_used": safe_json_loads(row[25], []), "email_switches_count": row[26] or 0,
                    "browser_privacy_level": row[27], "registration_prompted": bool(row[28]),
                    "registration_link_clicked": bool(row[29]), "recognition_response": row[30],
                    "display_message_offset": row[31] if len(row) > 31 else 0,
                    "reverification_pending": bool(row[32]) if len(row) > 32 else False,
                    "pending_user_type": UserType(row[33]) if len(row) > 33 and row[33] else None,
                    "pending_email": row[34] if len(row) > 34 else None,
                    "pending_full_name": row[35] if len(row) > 35 else None,
                    "pending_zoho_contact_id": row[36] if len(row) > 36 else None,
                    "pending_wp_token": row[37] if len(row) > 37 else None
                }
                return UserSession(**row_dict)
            except Exception as e: logger.error(f"❌ Failed to load session {session_id[:8]}: {e}", exc_info=True); return None

    async def save_session(self, session: UserSession):
        with self.lock:
            if not await self._ensure_connection(15): logger.warning(f"⚠️ Conn timeout saving {session.session_id[:8]}, using memory");
            if self.db_type == "memory": self.local_sessions[session.session_id] = copy.deepcopy(session); return
            try:
                json_messages = json.dumps(session.messages); json_emails_used = json.dumps(session.email_addresses_used)
                pending_user_type_value = session.pending_user_type.value if session.pending_user_type else None
                
                await self._execute_with_socket_retry_async('''
                    INSERT OR REPLACE INTO sessions (session_id, user_type, email, full_name, zoho_contact_id, 
                    created_at, last_activity, messages, active, wp_token, timeout_saved_to_crm, fingerprint_id, 
                    fingerprint_method, visitor_type, daily_question_count, total_question_count, last_question_time, 
                    question_limit_reached, ban_status, ban_start_time, ban_end_time, ban_reason, evasion_count, 
                    current_penalty_hours, escalation_level, email_addresses_used, email_switches_count, 
                    browser_privacy_level, registration_prompted, registration_link_clicked, recognition_response, 
                    display_message_offset, reverification_pending, pending_user_type, pending_email, pending_full_name,
                    pending_zoho_contact_id, pending_wp_token) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (session.session_id, session.user_type.value, session.email, session.full_name,
                     session.zoho_contact_id, session.created_at.isoformat(), session.last_activity.isoformat(),
                     json_messages, int(session.active), session.wp_token, int(session.timeout_saved_to_crm),
                     session.fingerprint_id, session.fingerprint_method, session.visitor_type,
                     session.daily_question_count, session.total_question_count, 
                     session.last_question_time.isoformat() if session.last_question_time else None,
                     int(session.question_limit_reached), session.ban_status.value,
                     session.ban_start_time.isoformat() if session.ban_start_time else None,
                     session.ban_end_time.isoformat() if session.ban_end_time else None,
                     session.ban_reason, session.evasion_count, session.current_penalty_hours,
                     session.escalation_level, json_emails_used, session.email_switches_count,
                     session.browser_privacy_level, int(session.registration_prompted),
                     int(session.registration_link_clicked), session.recognition_response, session.display_message_offset,
                     int(session.reverification_pending), pending_user_type_value, session.pending_email,
                     session.pending_full_name, session.pending_zoho_contact_id, session.pending_wp_token))
                await asyncio.to_thread(self.conn.commit)
            except Exception as e:
                logger.error(f"❌ Failed to save session {session.session_id[:8]}: {e}", exc_info=True)
                self.local_sessions[session.session_id] = copy.deepcopy(session); logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback")

    async def cleanup_expired_sessions(self, expiry_minutes: int = 5, limit: int = 5) -> Dict[str, Any]: # Default limit reduced to 5
        with self.lock:
            logger.info(f"🧹 Starting cleanup for sessions expired >{expiry_minutes}m, LIMIT {limit} per run.")
            if not await self._ensure_connection(15): logger.warning("⚠️ Conn timeout for cleanup.");
            
            if self.db_type == "memory":
                cutoff_time = datetime.now() - timedelta(minutes=expiry_minutes)
                processed_sessions = []; crm_eligible = []
                sorted_sessions = sorted(list(self.local_sessions.items()), key=lambda item: item[1].last_activity or datetime.min)
                for sid, sess in sorted_sessions[:limit]:
                    if sess.active and sess.last_activity < cutoff_time:
                        if not sess.timeout_saved_to_crm and is_crm_eligible(sess, False): crm_eligible.append(copy.deepcopy(sess))
                        sess.active = False; processed_sessions.append(sid)
                more_remaining = len(sorted_sessions) > limit
                return {"success": True, "cleaned_up_count": len(processed_sessions), "crm_eligible_count": len(crm_eligible), "storage_type": "memory", "more_sessions_remaining": more_remaining}
            
            try:
                cutoff_iso = (datetime.now() - timedelta(minutes=expiry_minutes)).isoformat()
                cursor = await self._execute_with_socket_retry_async(f"""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, created_at, last_activity, messages, active, wp_token, timeout_saved_to_crm, fingerprint_id, fingerprint_method, visitor_type, daily_question_count, total_question_count, last_question_time, question_limit_reached, ban_status, ban_start_time, ban_end_time, ban_reason, evasion_count, current_penalty_hours, escalation_level, email_addresses_used, email_switches_count, browser_privacy_level, registration_prompted, registration_link_clicked, recognition_response, display_message_offset, reverification_pending, pending_user_type, pending_email, pending_full_name, pending_zoho_contact_id, pending_wp_token
                    FROM sessions WHERE active = 1 AND last_activity < ? AND timeout_saved_to_crm = 0
                    AND (user_type = 'registered_user' OR user_type = 'email_verified_guest') AND email IS NOT NULL AND daily_question_count >= 1
                    ORDER BY last_activity ASC LIMIT {limit + 1}
                """, (cutoff_iso,))
                
                rows = await asyncio.to_thread(cursor.fetchall)
                more_remaining = len(rows) > limit
                sessions_to_process = rows[:limit]
                logger.info(f"🔍 Found {len(rows)} sessions (potential total). Processing {len(sessions_to_process)} this run. More remaining: {more_remaining}")
                if not sessions_to_process: return {"success": True, "cleaned_up_count": 0, "crm_eligible_count": 0, "storage_type": self.db_type, "more_sessions_remaining": more_remaining}
                
                crm_saved = 0; crm_failed = 0
                for row in sessions_to_process:
                    await asyncio.sleep(0) # Yield control
                    try:
                        # Reconstruct UserSession object for processing
                        session_obj = UserSession(session_id=row[0], user_type=UserType(row[1]), email=row[2], full_name=row[3], zoho_contact_id=row[4],
                            created_at=datetime.fromisoformat(row[5]), last_activity=datetime.fromisoformat(row[6]), messages=safe_json_loads(row[7]),
                            active=bool(row[8]), wp_token=row[9], timeout_saved_to_crm=bool(row[10]), fingerprint_id=row[11],
                            fingerprint_method=row[12], visitor_type=row[13] or 'new_visitor', daily_question_count=row[14] or 0,
                            total_question_count=row[15] or 0, last_question_time=datetime.fromisoformat(row[16]) if row[16] else None,
                            question_limit_reached=bool(row[17]), ban_status=BanStatus(row[18]) if row[18] else BanStatus.NONE,
                            ban_start_time=datetime.fromisoformat(row[19]) if row[19] else None,
                            ban_end_time=datetime.fromisoformat(row[20]) if row[20] else None, ban_reason=row[21],
                            evasion_count=row[22] or 0, current_penalty_hours=row[23] or 0, escalation_level=row[24] or 0,
                            email_addresses_used=safe_json_loads(row[25]), email_switches_count=row[26] or 0,
                            browser_privacy_level=row[27], registration_prompted=bool(row[28]),
                            registration_link_clicked=bool(row[29]), recognition_response=row[30],
                            display_message_offset=row[31] if len(row) > 31 else 0,
                            reverification_pending=bool(row[32]) if len(row) > 32 else False,
                            pending_user_type=UserType(row[33]) if len(row) > 33 and row[33] else None,
                            pending_email=row[34] if len(row) > 34 else None,
                            pending_full_name=row[35] if len(row) > 35 else None,
                            pending_zoho_contact_id=row[36] if len(row) > 36 else None,
                            pending_wp_token=row[37] if len(row) > 37 else None)

                        save_result = await zoho_manager.save_chat_transcript_sync(session_obj, "Automated Session Timeout Cleanup")
                        
                        session_obj.timeout_saved_to_crm = save_result.get("success", False)
                        session_obj.active = False # Always mark inactive after processing by cleanup
                        session_obj.last_activity = datetime.now()
                        if save_result.get("contact_id") and not session_obj.zoho_contact_id: session_obj.zoho_contact_id = save_result["contact_id"]
                        
                        await self.save_session(session_obj) # Persist final state
                        crm_saved = crm_saved + 1 if save_result.get("success") else crm_saved
                        crm_failed = crm_failed + 1 if not save_result.get("success") else crm_failed
                    except Exception as e:
                        logger.critical(f"❌ Critical error in background CRM processing for session {row[0][:8]}: {e}", exc_info=True)
                        crm_failed = crm_failed + 1
                        try:
                            # Attempt to mark inactive even on critical failure
                            temp_session_for_fail = await self.load_session(row[0]) 
                            if temp_session_for_fail:
                                temp_session_for_fail.active = False; temp_session_for_fail.last_activity = datetime.now()
                                await self.save_session(temp_session_for_fail)
                        except Exception as fe: logger.critical(f"❌ Failed to mark session inactive after critical error: {fe}")
                
                return {"success": True, "cleaned_up_count": len(sessions_to_process), "crm_eligible_count": crm_saved + crm_failed,
                        "storage_type": self.db_type, "crm_saved_count": crm_saved, "crm_failed_count": crm_failed,
                        "more_sessions_remaining": more_remaining}
            except Exception as e:
                logger.error(f"❌ Failed to cleanup expired sessions: {e}", exc_info=True)
                return {"success": False, "error": str(e), "storage_type": self.db_type, "more_sessions_remaining": True}

# PDF Exporter
class PDFExporter:
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='UserMessage', backColor=lightgrey, fontSize=10, leading=14, spaceAfter=6))

    async def generate_chat_pdf(self, session: UserSession) -> Optional[io.BytesIO]:
        try:
            buffer = io.BytesIO(); doc = SimpleDocTemplate(buffer, pagesize=letter)
            story = [Paragraph("FiFi AI Emergency Save Transcript", self.styles['Heading1']),
                     Paragraph(f"Session ID: {session.session_id}", self.styles['Normal']),
                     Paragraph(f"User: {session.full_name or 'Anonymous'} ({session.email or 'No email'})", self.styles['Normal']),
                     Paragraph(f"Created: {session.created_at.strftime('%Y-%m-%d %H:%M:%S')}", self.styles['Normal']), Spacer(1, 12)]
            for msg in session.messages:
                content = html.escape(str(msg.get('content', ''))); content = re.sub(r'<[^>]+>', '', content)
                style = self.styles['UserMessage'] if msg.get('role') == 'user' else self.styles['Normal']
                story.append(Spacer(1, 8)); story.append(Paragraph(f"<b>{msg.get('role', 'unknown').capitalize()}:</b> {content}", style))
            await asyncio.to_thread(doc.build, story); buffer.seek(0); return buffer
        except Exception as e: logger.error(f"❌ PDF generation failed: {e}", exc_info=True); return None

# Zoho CRM Manager
class ZohoCRMManager:
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token = None
        self._token_expiry = None
        self._http_client = httpx.AsyncClient(timeout=30)

    async def close_http_client(self):
        if self._http_client: await self._http_client.aclose(); logger.info("✅ httpx.AsyncClient for ZohoCRMManager closed.")

    async def _get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        if not ZOHO_ENABLED: return None
        if not force_refresh and self._access_token and self._token_expiry and datetime.now() < self._token_expiry: return self._access_token
        try:
            response = await self._http_client.post("https://accounts.zoho.com/oauth/v2/token", data={'refresh_token': ZOHO_REFRESH_TOKEN, 'client_id': ZOHO_CLIENT_ID, 'client_secret': ZOHO_CLIENT_SECRET, 'grant_type': 'refresh_token'}, timeout=15)
            response.raise_for_status()
            self._access_token = response.json().get('access_token'); self._token_expiry = datetime.now() + timedelta(minutes=50)
            return self._access_token
        except Exception as e: logger.error(f"❌ Failed to get Zoho access token: {e}", exc_info=True); return None

    async def _find_contact_by_email(self, email: str) -> Optional[str]:
        token = await self._get_access_token(); return None if not token else (await self._perform_zoho_get("Contacts/search", {'criteria': f'(Email:equals:{email})'}, token, "find contact"))

    async def _create_contact(self, email: str, full_name: Optional[str]) -> Optional[str]:
        token = await self._get_access_token(); return None if not token else (await self._perform_zoho_post("Contacts", {"data": [{"Last_Name": full_name or "Food Professional", "Email": email, "Lead_Source": "FiFi AI Emergency API"}]}, token, "create contact"))

    async def _add_note(self, contact_id: str, note_title: str, note_content: str) -> bool:
        token = await self._get_access_token(); return False if not token else (await self._perform_zoho_post("Notes", {"data": [{"Note_Title": note_title, "Note_Content": note_content[:32000], "Parent_Id": contact_id, "se_module": "Contacts"}]}, token, "add note"))

    async def _upload_attachment(self, contact_id: str, pdf_buffer: io.BytesIO, filename: str) -> bool:
        token = await self._get_access_token(); return False if not token else (await self._perform_zoho_upload(contact_id, pdf_buffer, filename, token))

    async def _perform_zoho_get(self, endpoint: str, params: Dict[str, Any], token: str, op_name: str, timeout: int = 10):
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {token}'}
            response = await self._http_client.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json(); return data['data'][0]['id'] if 'data' in data and data['data'] else None
        except Exception as e: logger.error(f"❌ Error {op_name} Zoho: {e}", exc_info=True); return None
            
    async def _perform_zoho_post(self, endpoint: str, json_data: Dict[str, Any], token: str, op_name: str, timeout: int = 10):
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {token}', 'Content-Type': 'application/json'}
            response = await self._http_client.post(f"{self.base_url}/{endpoint}", headers=headers, json=json_data, timeout=timeout)
            response.raise_for_status()
            return 'data' in response.json() and response.json()['data'][0]['code'] == 'SUCCESS'
        except Exception as e: logger.error(f"❌ Error {op_name} Zoho: {e}", exc_info=True); return False

    async def _perform_zoho_upload(self, contact_id: str, pdf_buffer: io.BytesIO, filename: str, token: str, max_retries: int = 2):
        upload_url = f"{self.base_url}/Contacts/{contact_id}/Attachments"
        for attempt in range(max_retries):
            try:
                headers = {'Authorization': f'Zoho-oauthtoken {token}'}
                pdf_buffer.seek(0); pdf_content = await asyncio.to_thread(pdf_buffer.read)
                response = await self._http_client.post(upload_url, headers=headers, files={'file': (filename, pdf_content, 'application/pdf')}, timeout=60)
                if response.status_code == 401: token = await self._get_access_token(force_refresh=True); continue # Token refresh on 401
                response.raise_for_status()
                return 'data' in response.json() and response.json()['data'][0]['code'] == 'SUCCESS'
            except Exception as e: logger.error(f"❌ Error uploading Zoho attachment (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True);
            if attempt < max_retries - 1: await asyncio.sleep(2 ** attempt)
        return False

    async def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> Dict[str, Any]:
        if not ZOHO_ENABLED or not session.email or not session.messages: return {"success": False, "reason": "not_eligible"}
        try:
            contact_id = session.zoho_contact_id or await self._find_contact_by_email(session.email)
            if not contact_id: contact_id = await self._create_contact(session.email, session.full_name)
            if not contact_id: return {"success": False, "reason": "contact_failed"}
            
            note_content = f"**Emergency Save Info:**\n- Session ID: {session.session_id}\n- User: {session.full_name or 'Unknown'} ({session.email})\n- User Type: {session.user_type.value}\n- Trigger: {trigger_reason}\n- Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n- Total Msgs: {len(session.messages)}\n- Questions: {session.daily_question_count}\n\n**Conversation Summary (see PDF):**\n"
            for i, msg in enumerate(session.messages):
                content = re.sub(r'<[^>]+>', '', msg.get("content", "")); note_content += f"\n{i+1}. **{msg.get('role','').capitalize()}:** {content}\n"
            note_success = await self._add_note(contact_id, f"FiFi AI Emergency Save - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({trigger_reason})", note_content)
            
            pdf_success = False; pdf_buffer = await self.pdf_exporter.generate_chat_pdf(session)
            if pdf_buffer: pdf_success = await self._upload_attachment(contact_id, pdf_buffer, f"FiFi_Chat_{session.session_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
            else: logger.warning(f"⚠️ Failed to generate PDF for session {session.session_id[:8]}")
            
            return {"success": note_success and pdf_success, "contact_id": contact_id, "note_created": note_success, "pdf_attached": pdf_success}
        except Exception as e: logger.error(f"❌ CRM save process failed for {session.session_id[:8]}: {e}", exc_info=True); return {"success": False, "reason": "exception", "error": str(e)}

# Helper functions for CRM eligibility
def is_crm_eligible(session: UserSession, is_emergency_save: bool = False) -> bool:
    if not session.email or not session.messages: return False
    if session.user_type not in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]: return False
    if session.daily_question_count < 1: return False
    if not is_emergency_save:
        elapsed_minutes = (datetime.now() - (session.last_activity or session.created_at)).total_seconds() / 60
        if elapsed_minutes < 5.0: return False
    return True

# Background tasks
async def _perform_emergency_crm_save(session_id: str, reason: str):
    if db_manager is None or zoho_manager is None: logger.critical(f"❌ Managers not initialized for background task {session_id[:8]}."); return
    try:
        session = await db_manager.load_session(session_id)
        if not session or not is_crm_eligible(session, is_emergency_save=True) or session.timeout_saved_to_crm: return
        
        save_result = await zoho_manager.save_chat_transcript_sync(session, reason)
        session.timeout_saved_to_crm = save_result.get("success", False)
        if is_session_ending_reason(reason) or not save_result.get("success"): session.active = False # Force inactive on session-ending reason OR CRM save failure
        session.last_activity = datetime.now()
        if save_result.get("contact_id") and not session.zoho_contact_id: session.zoho_contact_id = save_result["contact_id"]
        await db_manager.save_session(session)
    except Exception as e: logger.critical(f"❌ Critical error in background CRM save task for {session_id[:8]}: {e}", exc_info=True)

async def _perform_full_cleanup_in_background():
    if db_manager is None or zoho_manager is None: logger.critical("❌ Managers not initialized for full cleanup task."); return
    try:
        cleanup_result = await db_manager.cleanup_expired_sessions(expiry_minutes=5, limit=5) # Reduced default limit to 5
        if not cleanup_result.get("success"): logger.error(f"❌ Background cleanup - Database cleanup failed: {cleanup_result}"); return
    except Exception as e: logger.critical(f"❌ Critical error in background cleanup: {e}", exc_info=True)

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "FiFi Emergency API - Async Operations Enabled (Simplified)",
        "status": "running",
        "version": "3.6.0-simplified",
        "info": "Consolidated & streamlined for clarity while maintaining core functionality & robustness."
    }

@app.get("/health")
async def health_check():
    if db_manager is None or zoho_manager is None: return {"status": "initializing", "message": "Managers not initialized.", "timestamp": datetime.now()}
    try:
        db_status = await db_manager.test_connection()
        return {"status": "healthy", "timestamp": datetime.now(), "database": db_status, "zoho": "enabled" if ZOHO_ENABLED else "disabled", "sqlitecloud_sdk": "available" if SQLITECLOUD_AVAILABLE else "not_available"}
    except Exception as e: return {"status": "unhealthy", "timestamp": datetime.now(), "error": str(e)}

@app.get("/diagnostics")
async def comprehensive_diagnostics():
    if db_manager is None or zoho_manager is None: return {"status": "initializing", "message": "Managers not initialized.", "timestamp": datetime.now()}
    try:
        diagnostics = {"timestamp": datetime.now(), "version": "3.6.0-simplified"}
        diagnostics["database_status"] = await db_manager.test_connection()
        diagnostics["zoho_enabled"] = ZOHO_ENABLED
        diagnostics["sqlitecloud_sdk_available"] = SQLITECLOUD_AVAILABLE
        return diagnostics
    except Exception as e: return {"status": "diagnostics_failed", "error": str(e)}

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions_endpoint(background_tasks: BackgroundTasks):
    if db_manager is None or zoho_manager is None: return {"success": False, "message": "Managers not initialized.", "queued_for_background_processing": False}
    background_tasks.add_task(_perform_full_cleanup_in_background)
    return {"success": True, "message": "Cleanup queued for background processing", "queued_for_background_processing": True}

@app.post("/emergency-save")
async def emergency_save_endpoint(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    if db_manager is None or zoho_manager is None: return {"success": False, "message": "Managers not initialized.", "queued_for_background_processing": False}
    background_tasks.add_task(_perform_emergency_crm_save, request.session_id, f"Async Emergency Save: {request.reason}")
    return {"success": True, "message": "Emergency save queued", "session_id": request.session_id, "reason": request.reason}

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting FiFi Emergency API - ASYNC OPERATIONS ENABLED (Simplified)...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
