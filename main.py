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
# Set level to DEBUG to see all the new detailed messages
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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

# Graceful fallback for optional imports (copied from fifi.py)
SQLITECLOUD_AVAILABLE = False
try:
    import sqlitecloud
    SQLITECLOUD_AVAILABLE = True
    logger.info("sqlitecloud SDK detected.")
except ImportError:
    logger.warning("SQLiteCloud SDK not available. Emergency beacon will use local SQLite if not configured.")


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
        logger.warning(f"Failed to decode JSON data (truncated): {str(data)[:100]}...")
        return default_value

# Database Manager (Modified for more debugging logs)
class DatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock()
        self.conn = None
        self.connection_string = connection_string
        self._last_health_check = None
        self._health_check_interval = timedelta(minutes=5)
        
        logger.info("🔄 INITIALIZING DATABASE MANAGER")
        
        self.db_type = "memory" # Default to memory initially
        self.local_sessions = {} # Ensure this is always initialized

        # Prioritize SQLite Cloud if configured and available
        if connection_string and SQLITECLOUD_AVAILABLE:
            try:
                logger.info("Attempting to connect to SQLite Cloud...")
                self.conn = sqlitecloud.connect(connection_string)
                self.conn.execute("SELECT 1").fetchone() # Test connection
                logger.info(f"✅ SQLite Cloud connection established! Test result: {self.conn.execute('SELECT 1').fetchone()}")
                self.db_type = "cloud"
            except Exception as e:
                logger.error(f"❌ SQLite Cloud connection failed during init: {e}", exc_info=True)
                self.conn = None
        elif connection_string:
            logger.warning("❌ SQLite Cloud connection string provided but sqlitecloud library is not available.")
        else:
            logger.info("No SQLite Cloud connection string provided, skipping cloud connection attempt.")
        
        # Fallback to local SQLite if cloud connection failed or not attempted
        if not self.conn:
            logger.info("Attempting to connect to local SQLite...")
            try:
                self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
                self.conn.execute("SELECT 1").fetchone() # Test connection
                logger.info("✅ Local SQLite connection established!")
                self.db_type = "file"
            except Exception as e:
                logger.error(f"❌ Local SQLite connection failed during init: {e}", exc_info=True)
                self.conn = None
                self.db_type = "memory" # Ensure fall back to memory if local fails

        # Initialize database schema after determining connection type
        if self.conn:
            try:
                self._init_complete_database()
                logger.info("✅ Database schema initialization completed successfully.")
            except Exception as e:
                logger.critical(f"Database schema initialization failed after connection: {e}", exc_info=True)
                logger.critical("🚨 Database schema could not be initialized. Falling back to in-memory storage.")
                self.conn = None
                self.db_type = "memory"
        else:
            logger.critical("🚨 ALL DATABASE CONNECTIONS FAILED. OPERATING IN NON-PERSISTENT IN-MEMORY STORAGE MODE.")


    def _init_complete_database(self):
        """Initialize database schema with all columns upfront (copied from fifi.py)"""
        with self.lock:
            try:
                if hasattr(self.conn, 'row_factory'): 
                    self.conn.row_factory = None # Ensure row_factory is not set for raw tuple fetching

                logger.debug("Executing CREATE TABLE IF NOT EXISTS...")
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
                
                logger.debug("Creating indexes if not exist...")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_session_lookup ON sessions(session_id, active)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fingerprint_id ON sessions(fingerprint_id)")
                self.conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON sessions(email)")
                
                self.conn.commit()
                logger.info("✅ Database schema ready and indexes created.")
                
            except Exception as e:
                logger.error(f"Database schema creation/indexing failed: {e}", exc_info=True)
                raise # Re-raise to fall back to memory if schema init fails

    def _check_connection_health(self) -> bool:
        """Check if database connection is healthy."""
        logger.debug(f"Checking DB connection health. Current DB type: {self.db_type}")
        if self.db_type == "memory":
            logger.debug("Running in-memory, connection is always 'healthy'.")
            return True # In-memory is always "healthy" for this check
        if not self.conn:
            logger.debug("No DB connection object present for non-memory DB type.")
            return False
            
        now = datetime.now()
        if (self._last_health_check and 
            now - self._last_health_check < self._health_check_interval):
            logger.debug(f"Skipping health check, within {self._health_check_interval.total_seconds() / 60} min interval.")
            return True
            
        try:
            logger.debug("Executing SELECT 1 to test DB connection...")
            self.conn.execute("SELECT 1").fetchone()
            self._last_health_check = now
            logger.info("✅ DB health check passed.")
            return True
        except Exception as e:
            logger.error(f"❌ DB health check failed during SELECT 1: {e}", exc_info=True)
            self.conn = None # Mark connection as bad
            return False
    
    def _ensure_connection(self): 
        """Ensure database connection is healthy, reconnect if needed."""
        if self._check_connection_health():
            logger.debug("Connection is healthy, no reconnection needed.")
            return
            
        logger.warning("Database connection unhealthy or not established, attempting reconnection...")
        old_conn = self.conn
        self.conn = None # Reset connection before trying to close

        if old_conn:
            try:
                old_conn.close()
                logger.debug("Closed old DB connection successfully.")
            except Exception as e:
                logger.debug(f"Error closing old DB connection: {e}")
        
        # Attempt reconnection
        if self.db_type == "cloud" and SQLITECLOUD_AVAILABLE:
            try:
                logger.info("Attempting to reconnect to SQLite Cloud...")
                self.conn = sqlitecloud.connect(self.connection_string)
                self.conn.execute("SELECT 1").fetchone()
                logger.info("✅ Reconnected to SQLite Cloud successfully!")
                self._init_complete_database() # Ensure schema is still valid
            except Exception as e:
                logger.error(f"❌ Reconnection to SQLite Cloud failed: {e}", exc_info=True)
                self.conn = None
        elif self.db_type == "file":
            try:
                logger.info("Attempting to reconnect to local SQLite...")
                self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
                self.conn.execute("SELECT 1").fetchone()
                logger.info("✅ Reconnected to local SQLite successfully!")
                self._init_complete_database() # Ensure schema is still valid
            except Exception as e:
                logger.error(f"❌ Reconnection to local SQLite failed: {e}", exc_info=True)
                self.conn = None
            
        if not self.conn:
            logger.critical("🚨 Database reconnection failed, falling back to in-memory storage for this instance.")
            self.db_type = "memory"
            # local_sessions is always initialized in __init__
        else:
            logger.info(f"Database reconnected successfully. Current type: {self.db_type}")


    def load_session(self, session_id: str) -> Optional[UserSession]:
        """Load session with complete SQLite Cloud compatibility and connection health check"""
        with self.lock:
            self._ensure_connection() 

            if self.db_type == "memory":
                session = self.local_sessions.get(session_id)
                if session and isinstance(session.user_type, str):
                    try:
                        session.user_type = UserType(session.user_type)
                    except ValueError:
                        session.user_type = UserType.GUEST
                logger.debug(f"Loaded session {session_id[:8]} from in-memory (type: {session.user_type.value if session else 'None'})")
                return copy.deepcopy(session)
            
            if not self.conn:
                logger.error(f"Cannot load session {session_id[:8]}: No active database connection after _ensure_connection.")
                return None

            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                logger.debug(f"Attempting to load session {session_id[:8]} from {self.db_type} DB with active={session_id[:8]}...")
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
                
                if not row:
                    logger.info(f"No active session found for ID {session_id[:8]}.")
                    return None
                
                expected_cols = 31
                if len(row) < expected_cols:
                    logger.error(f"Row has insufficient columns: {len(row)} (expected {expected_cols}) for session {session_id[:8]}. Data corruption suspected.")
                    return None
                
                messages_data = safe_json_loads(row[7], [])
                logger.debug(f"Raw DB data for {session_id[:8]}: user_type={row[1]}, email={row[2]}, messages_len={len(messages_data)}, daily_q={row[14]}, active={bool(row[8])}")
                
                try:
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
                        email_addresses_used=safe_json_loads(row[25], []),
                        email_switches_count=row[26] or 0,
                        browser_privacy_level=row[27],
                        registration_prompted=bool(row[28]),
                        registration_link_clicked=bool(row[29]),
                        recognition_response=row[30]
                    )
                    
                    logger.info(f"Successfully loaded and parsed session {session_id[:8]}: user_type={user_session.user_type.value}, messages={len(user_session.messages)}")
                    return user_session
                    
                except Exception as e:
                    logger.error(f"Failed to create UserSession object from row for session {session_id[:8]}: {e}", exc_info=True)
                    logger.error(f"Problematic row data (first 10 fields): {str(row[:10])}")
                    return None
                    
            except Exception as e:
                logger.error(f"Database query failed for session {session_id[:8]} during load_session: {e}", exc_info=True)
                return None

    def save_session(self, session: UserSession):
        """Save session with SQLite Cloud compatibility and connection health check"""
        with self.lock:
            self._ensure_connection()
            if self.db_type == "memory":
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.debug(f"Saved session {session.session_id[:8]} to in-memory.")
                return
            
            if not self.conn:
                logger.error(f"Cannot save session {session.session_id[:8]}: No active database connection after _ensure_connection.")
                return

            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                logger.debug(f"Attempting to save session {session.session_id[:8]} to {self.db_type} DB.")
                try:
                    json_messages = json.dumps(session.messages)
                    json_emails_used = json.dumps(session.email_addresses_used)
                except (TypeError, ValueError) as e:
                    logger.error(f"Session data not JSON serializable for {session.session_id[:8]}: {e}. Resetting to empty lists for save.")
                    json_messages = "[]"
                    json_emails_used = "[]"
                
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
                logger.debug(f"Successfully saved session {session.session_id[:8]}. Active: {session.active}, SavedToCRM: {session.timeout_saved_to_crm}")
            except Exception as e:
                logger.error(f"Failed to save session {session.session_id[:8]} during save_session: {e}", exc_info=True)
                # Fallback to in-memory on save failure if possible (for debug purposes)
                if not hasattr(self, 'local_sessions'):
                    self.local_sessions = {}
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                logger.warning(f"Fallback: Saved session {session.session_id[:8]} to in-memory storage due to DB error.")


    def get_all_active_sessions(self) -> List[UserSession]:
        """Get all active sessions for cleanup operations"""
        with self.lock:
            self._ensure_connection()
            if self.db_type == "memory":
                return [copy.deepcopy(s) for s in self.local_sessions.values() if s.active]
            
            if not self.conn:
                logger.error("Cannot get all active sessions: No active database connection after _ensure_connection.")
                return []

            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None

                logger.debug(f"Attempting to get all active sessions from {self.db_type} DB.")
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
                        logger.warning(f"Row has insufficient columns: {len(row)} (expected {expected_cols}). Skipping.")
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
                        logger.error(f"Error converting row to session in get_all_active_sessions: {e}", exc_info=True)
                        continue
                logger.debug(f"Retrieved {len(sessions)} active sessions.")
                return sessions
            except Exception as e:
                logger.error(f"Failed to get active sessions during get_all_active_sessions: {e}", exc_info=True)
                return []

# PDF Exporter (Unchanged - already robust enough for FastAPI context)
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
            logger.error(f"PDF generation failed: {e}", exc_info=True)
            return None

# Zoho CRM Manager (Unchanged - relies on external configs and token refresh)
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
        
        logger.info("Requesting new Zoho access token...")
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
            self._token_expiry = datetime.now() + timedelta(minutes=50) # Tokens are valid for 1 hour
            logger.info("Successfully obtained Zoho access token.")
            return self._access_token
        except requests.exceptions.Timeout:
            logger.error("Zoho token request timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Failed to get Zoho access token: {e}", exc_info=True)
            return None

    def _find_contact_by_email(self, email: str) -> Optional[str]:
        access_token = self._get_access_token()
        if not access_token:
            return None
        
        logger.debug(f"Searching Zoho for contact with email: {email}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
            params = {'criteria': f'(Email:equals:{email})'}
            response = requests.get(f"{self.base_url}/Contacts/search", headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                contact_id = data['data'][0]['id']
                logger.info(f"Found existing Zoho contact: {contact_id}")
                return contact_id
            logger.debug(f"No Zoho contact found for email: {email}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"Zoho contact search for {email} timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error finding contact by email {email}: {e}", exc_info=True)
        return None

    def _create_contact(self, email: str, full_name: Optional[str]) -> Optional[str]:
        access_token = self._get_access_token()
        if not access_token:
            return None

        logger.info(f"Creating new Zoho contact for email: {email}")
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
                logger.info(f"Created new Zoho contact: {contact_id}")
                return contact_id
            
            logger.error(f"Zoho contact creation failed with response: {data}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"Zoho contact creation for {email} timed out.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Error creating contact for {email}: {e}", exc_info=True)
        return None

    def _add_note(self, contact_id: str, note_title: str, note_content: str) -> bool:
        access_token = self._get_access_token()
        if not access_token:
            return False

        logger.info(f"Adding note '{note_title}' to Zoho contact {contact_id}")
        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}', 'Content-Type': 'application/json'}
            
            if len(note_content) > 32000:
                logger.warning(f"Note content for {contact_id} exceeds 32000 chars. Truncating.")
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
                logger.info(f"Successfully added Zoho note: {note_title}")
                return True
            logger.error(f"Zoho note creation failed with response: {data}")
            return False
        except requests.exceptions.Timeout:
            logger.error(f"Zoho note '{note_title}' creation for {contact_id} timed out.", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error adding note '{note_title}' to Zoho contact {contact_id}: {e}", exc_info=True)
            return False

    def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> bool:
        if not ZOHO_ENABLED:
            logger.info("Zoho is not enabled. Skipping Zoho CRM save.")
            return False
        if not session.email:
            logger.info(f"Session {session.session_id[:8]} has no email. Skipping Zoho CRM save.")
            return False
        if not session.messages:
            logger.info(f"Session {session.session_id[:8]} has no messages. Skipping Zoho CRM save.")
            return False
        
        try:
            logger.info(f"🔄 Starting Zoho CRM save for session {session.session_id[:8]} (Reason: {trigger_reason})")
            
            contact_id = self._find_contact_by_email(session.email)
            if not contact_id:
                contact_id = self._create_contact(session.email, session.full_name)
            if not contact_id:
                logger.error(f"Failed to find or create Zoho contact for {session.email}. Aborting CRM save.")
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
                
                if len(content) > 500: # Truncate individual messages for note content
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
            logger.error(f"Emergency CRM save process failed for session {session.session_id[:8]}: {e}", exc_info=True)
            return False

# Initialize managers (these will now use the more robust DatabaseManager)
db_manager = DatabaseManager(SQLITE_CLOUD_CONNECTION)
pdf_exporter = PDFExporter()
zoho_manager = ZohoCRMManager(pdf_exporter)

# Helper functions
def is_crm_eligible(session: UserSession) -> bool:
    """Enhanced eligibility check for CRM saves (copied from fifi.py logic)."""
    try:
        if not session.email or not session.messages:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: Missing email ({bool(session.email)}) or messages ({bool(session.messages)})")
            return False
        
        # Check if already saved to avoid duplicates
        # IMPORTANT: If the session was already saved (timeout_saved_to_crm=True)
        # AND this is *not* a manual save (which would re-evaluate eligibility)
        # we might want to skip. But for emergency beacon, we *always* try.
        # This function is primarily for the Streamlit side deciding to save.
        # For the beacon, if it got here, we want to proceed.
        # The `_perform_emergency_crm_save` will set timeout_saved_to_crm=True
        # so subsequent *beacon* calls for the same session might then return true here.
        # For now, let's keep it consistent with Streamlit's checks.
        
        # User type eligibility: registered_user OR email_verified_guest
        if session.user_type not in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: User type {session.user_type.value} not eligible.")
            return False
        
        # Question count requirement: at least 1 question asked
        if session.daily_question_count < 1:
            logger.debug(f"CRM Eligibility for {session.session_id[:8]}: No questions asked ({session.daily_question_count}).")
            return False
        
        # 15-minute eligibility check
        # Use the earliest of session creation time or first question time
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
        logger.error(f"Error checking CRM eligibility for session {session.session_id[:8]}: {e}", exc_info=True)
        return False

# API Endpoints
@app.get("/")
async def root():
    return {"message": "FiFi Emergency API", "status": "running"}

@app.get("/health")
async def health_check():
    db_status = db_manager.test_connection()
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "database": db_status.get("status", "unknown"),
        "database_type": db_status.get("type", "unknown"),
        "zoho": "enabled" if ZOHO_ENABLED else "disabled"
    }

@app.get("/debug-db")
async def debug_database():
    """Debug endpoint to test database connection and session access"""
    try:
        # Test connection
        db_status = db_manager.test_connection()
        
        # Test recent sessions
        recent_sessions = []
        try:
            if db_manager.conn:
                cursor = db_manager.conn.execute("""
                    SELECT session_id, email, user_type, daily_question_count, 
                           created_at, active, timeout_saved_to_crm, last_activity
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
                        "last_activity": row[7]
                    })
        except Exception as query_error:
            recent_sessions = f"Query failed: {str(query_error)} (Check DB status in 'database_status')"
            logger.error(f"Debug DB query for recent sessions failed: {query_error}", exc_info=True)
        
        return {
            "database_status": db_status,
            "recent_sessions": recent_sessions,
            "env_check": {
                "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING",
                "ZOHO_CLIENT_ID": "SET" if ZOHO_CLIENT_ID else "MISSING",
                "ZOHO_CLIENT_SECRET": "SET" if ZOHO_CLIENT_SECRET else "MISSING",
                "ZOHO_REFRESH_TOKEN": "SET" if ZOHO_REFRESH_TOKEN else "MISSING"
            },
            "zoho_status": "enabled" if ZOHO_ENABLED else "disabled"
        }
    except Exception as e:
        logger.critical(f"Critical error in debug_database endpoint: {e}", exc_info=True)
        return {
            "error": str(e),
            "database_status": "FAILED (Critical Endpoint Error)",
            "env_check": {
                "SQLITE_CLOUD_CONNECTION": "SET" if SQLITE_CLOUD_CONNECTION else "MISSING"
            }
        }

@app.post("/emergency-save")
async def emergency_save(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    try:
        logger.info(f"🚨 EMERGENCY SAVE: Request for session {request.session_id[:8]}, reason: {request.reason}")
        
        session = db_manager.load_session(request.session_id)
        if not session:
            logger.error(f"Session {request.session_id[:8]} not found or not active in DB after load attempt.")
            return {
                "success": False,
                "message": "Session not found or not active. Check Streamlit app for session state.",
                "session_id": request.session_id,
                "reason": "session_not_found"
            }
        
        logger.info(f"Session {session.session_id[:8]} loaded for emergency save: email={session.email}, user_type={session.user_type.value}, messages={len(session.messages)}, questions={session.daily_question_count}")
        
        if not is_crm_eligible(session):
            logger.info(f"Session {request.session_id[:8]} not eligible for CRM save based on current criteria (e.g., questions asked, duration).")
            return {
                "success": False,
                "message": "Session not eligible for CRM save based on usage criteria.",
                "session_id": request.session_id,
                "reason": "not_eligible"
            }
        
        if session.timeout_saved_to_crm:
            logger.info(f"Session {request.session_id[:8]} already marked as saved to CRM. Skipping duplicate save request.")
            return {
                "success": True,
                "message": "Session already saved to CRM, no action needed.",
                "session_id": request.session_id,
                "reason": "already_saved"
            }

        # Perform CRM save in background to avoid timeout
        background_tasks.add_task(
            _perform_emergency_crm_save,
            session,
            f"Emergency Save: {request.reason}"
        )
        
        logger.info(f"✅ Emergency save queued for {request.session_id[:8]}")
        return {
            "success": True,
            "message": "Emergency save queued successfully",
            "session_id": request.session_id,
            "saved_to_crm": True
        }
            
    except Exception as e:
        logger.critical(f"Critical error in emergency_save endpoint for session {request.session_id[:8]}: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Internal server error during emergency save: {str(e)}",
            "session_id": request.session_id,
            "reason": "internal_error"
        }

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions():
    """
    Endpoint to manually trigger cleanup of expired sessions.
    Designed for external cron job or similar.
    """
    try:
        logger.info("🧹 SESSION CLEANUP: Starting")
        
        results = {"processed_sessions": 0, "crm_saved": 0, "marked_inactive": 0, "errors_processing": 0}
        active_sessions = db_manager.get_all_active_sessions()
        logger.info(f"Found {len(active_sessions)} potentially active sessions for cleanup.")
        
        for session in active_sessions:
            try:
                results["processed_sessions"] += 1
                time_since_activity = datetime.now() - session.last_activity
                if time_since_activity.total_seconds() >= 900:  # 15 minutes
                    logger.info(f"Session {session.session_id[:8]} found inactive for {time_since_activity.total_seconds() / 60:.1f} minutes. Processing for cleanup.")
                    
                    if is_crm_eligible(session) and not session.timeout_saved_to_crm:
                        logger.info(f"Session {session.session_id[:8]} is CRM eligible and not yet saved. Attempting auto-save.")
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
                        logger.debug(f"Session {session.session_id[:8]} not CRM eligible or already saved during cleanup.")
                    
                    session.active = False
                    session.last_activity = datetime.now() # Update activity to reflect cleanup time
                    db_manager.save_session(session)
                    results["marked_inactive"] += 1
                    logger.info(f"Session {session.session_id[:8]} marked inactive during cleanup.")
                else:
                    logger.debug(f"Session {session.session_id[:8]} is still active ({time_since_activity.total_seconds() / 60:.1f} min inactivity). Skipping cleanup.")
                    
            except Exception as e:
                logger.error(f"Error processing session {session.session_id[:8]} during cleanup: {e}", exc_info=True)
                results["errors_processing"] += 1
        
        logger.info(f"🧹 SESSION CLEANUP COMPLETE. Results: {results}")
        return {**results, "timestamp": datetime.now()}
        
    except Exception as e:
        logger.critical(f"Critical error during cleanup_expired_sessions endpoint execution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during cleanup: {str(e)}")

async def _perform_emergency_crm_save(session, reason: str):
    """Background task to perform CRM save and update session status"""
    try:
        logger.info(f"Background task: Starting CRM save for session {session.session_id[:8]} (Reason: {reason})")
        save_success = zoho_manager.save_chat_transcript_sync(session, reason)
        
        if save_success:
            session.timeout_saved_to_crm = True
            session.last_activity = datetime.now() # Update last activity as it was just processed
            db_manager.save_session(session) # Persist the timeout_saved_to_crm flag
            logger.info(f"✅ Background CRM save successful and flag updated for session {session.session_id[:8]}")
        else:
            logger.error(f"❌ Background CRM save failed for session {session.session_id[:8]} in _perform_emergency_crm_save.")
            
    except Exception as e:
        logger.critical(f"Critical error in background CRM save for session {session.session_id[:8]}: {e}", exc_info=True)

if __name__ == "__main__":
    import uvicorn
    # Use reload=True for local development, but ensure it's False for Cloud Run deployments
    uvicorn.run(app, host="0.0.0.0", port=8000)
