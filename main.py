# main.py - Consolidated FiFi Backend API
# Serves as a beacon for emergency saves, session cleanup, and fingerprinting.
# Version: 4.0.1 (Fixed SQLiteCloud compatibility)

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import json
import sqlite3
import copy
import httpx
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from enum import Enum
from dataclasses import dataclass, field
import io
import html
import re
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import lightgrey

# --- 1. Configuration & Logging ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(title="FiFi Backend API", version="4.0.1")

# CORS middleware for Streamlit frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://fifi-eu.streamlit.app", "*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)

# Configuration from environment variables
SQLITE_CLOUD_CONNECTION = os.getenv("SQLITE_CLOUD_CONNECTION")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_ENABLED = all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN])

# Optional SDK import check
SQLITECLOUD_AVAILABLE = False
try:
    import sqlitecloud
    SQLITECLOUD_AVAILABLE = True
    logger.info("✅ sqlitecloud SDK detected.")
except ImportError:
    logger.warning("❌ SQLiteCloud SDK not available. Local file/memory fallback will be used.")

# Global managers (initialized during startup)
db_manager = None
pdf_exporter = None
zoho_manager = None

# --- 2. Pydantic Models & Data Classes ---

class EmergencySaveRequest(BaseModel):
    session_id: str
    reason: str
    timestamp: Optional[int] = None

class FingerprintPayload(BaseModel):
    session_id: str
    fingerprint_id: str
    method: str
    privacy: str
    working_methods: List[str]

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
    """Consolidated UserSession dataclass with all fields from both code versions."""
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
    declined_recognized_email_at: Optional[datetime] = None
    timeout_detected_at: Optional[datetime] = None
    timeout_reason: Optional[str] = None

# --- 3. Core Service Classes (Database, PDF, CRM) ---

def safe_json_loads(data: Optional[str], default_value: Any = None) -> Any:
    """Safely decode a JSON string, returning a default value on failure."""
    if data is None or data == "":
        return default_value
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Failed to decode JSON data: {str(data)[:100]}...")
        return default_value

def is_session_ending_reason(reason: str) -> bool:
    """Check if a reason string indicates a session is terminating."""
    session_ending_keywords = ['beforeunload', 'unload', 'close', 'refresh', 'timeout', 'browser_close', 'tab_close']
    return any(keyword in reason.lower() for keyword in session_ending_keywords)

class ResilientDatabaseManager:
    """Manages database connections with resilience and fallbacks."""
    def __init__(self, connection_string: Optional[str]):
        self.conn = None
        self.connection_string = connection_string
        self.db_type = "memory"
        self.local_sessions = {}
        self._initialized_schema = False
        logger.info("🔄 ResilientDatabaseManager initialized (will connect on first use).")

    async def _ensure_connection(self):
        """Ensure a valid database connection exists, trying cloud, then local file, then memory."""
        if self.conn and self.db_type != "memory":
            try:
                await asyncio.to_thread(self.conn.execute, "SELECT 1")
                return
            except Exception:
                logger.warning("⚠️ Database connection health check failed. Reconnecting...")
                self.conn = None
                self.db_type = "memory"
        
        # Try SQLite Cloud if configured
        if self.connection_string and SQLITECLOUD_AVAILABLE:
            try:
                self.conn = await asyncio.to_thread(sqlitecloud.connect, self.connection_string)
                self.db_type = "cloud"
                logger.info("✅ Connected to SQLite Cloud.")
            except Exception as e:
                logger.error(f"❌ SQLite Cloud connection failed: {e}. Falling back...")
                self.conn = None

        # Fallback to local file if cloud fails or is not configured
        if not self.conn:
            try:
                self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
                self.db_type = "file"
                logger.warning("⚠️ Connected to local SQLite file as a fallback.")
            except Exception as e:
                logger.error(f"❌ Local SQLite file connection failed: {e}. Falling back to in-memory.")
                self.conn = None

        # Ultimate fallback: in-memory dictionary
        if not self.conn:
            self.db_type = "memory"
            self._initialized_schema = True
            logger.error("🚨 CRITICAL: No persistent DB connection. Operating in IN-MEMORY mode.")
            return

        # Initialize schema if we have a new persistent connection
        if not self._initialized_schema:
            try:
                await asyncio.to_thread(self._init_complete_database_sync)
                self._initialized_schema = True
            except Exception as e:
                logger.critical(f"❌ FAILED to initialize database schema: {e}. Reverting to in-memory.", exc_info=True)
                self.conn = None
                self.db_type = "memory"

    def _init_complete_database_sync(self):
        """Initializes or updates the database schema. Runs in a thread."""
        logger.info("🏗️ Initializing/updating database schema...")
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY, user_type TEXT, email TEXT, full_name TEXT,
                zoho_contact_id TEXT, created_at TEXT, last_activity TEXT,
                messages TEXT, active INTEGER, wp_token TEXT, timeout_saved_to_crm INTEGER,
                fingerprint_id TEXT, fingerprint_method TEXT, visitor_type TEXT,
                daily_question_count INTEGER, total_question_count INTEGER, last_question_time TEXT,
                question_limit_reached INTEGER, ban_status TEXT, ban_start_time TEXT,
                ban_end_time TEXT, ban_reason TEXT, evasion_count INTEGER,
                current_penalty_hours INTEGER, escalation_level INTEGER,
                email_addresses_used TEXT, email_switches_count INTEGER,
                browser_privacy_level TEXT, registration_prompted INTEGER,
                registration_link_clicked INTEGER, recognition_response TEXT,
                display_message_offset INTEGER, reverification_pending INTEGER,
                pending_user_type TEXT, pending_email TEXT, pending_full_name TEXT,
                pending_zoho_contact_id TEXT, pending_wp_token TEXT
            )
        ''')
        
        # Add new columns safely
        new_columns = [
            ("declined_recognized_email_at", "TEXT"),
            ("timeout_detected_at", "TEXT"),
            ("timeout_reason", "TEXT")
        ]
        for col_name, col_type in new_columns:
            try:
                cursor.execute(f"ALTER TABLE sessions ADD COLUMN {col_name} {col_type}")
                logger.info(f"Added column: {col_name}")
            except Exception as e:
                if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower():
                    pass
                else:
                    raise e
        
        self.conn.commit()
        logger.info("✅ Database schema is up to date.")

    async def test_connection(self):
        """Performs a connection and schema check."""
        await self._ensure_connection()
        return {"db_type": self.db_type, "schema_initialized": self._initialized_schema}

    def _row_to_dict(self, cursor, row) -> Dict[str, Any]:
        """Convert a database row to a dictionary, handling both SQLite and SQLiteCloud."""
        if row is None:
            return None
            
        if self.db_type == "cloud":
            # SQLiteCloud returns tuples, use cursor.description
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        else:
            # Regular SQLite with row_factory returns sqlite3.Row objects
            return dict(row)

    def _build_session_from_row(self, row_dict: Dict) -> UserSession:
        """Helper method to build UserSession object from a dictionary-like database row."""
        if not row_dict:
            return None
            
        return UserSession(
            session_id=row_dict.get('session_id'),
            user_type=UserType(row_dict['user_type']) if row_dict.get('user_type') else UserType.GUEST,
            email=row_dict.get('email'),
            full_name=row_dict.get('full_name'),
            zoho_contact_id=row_dict.get('zoho_contact_id'),
            created_at=datetime.fromisoformat(row_dict['created_at']) if row_dict.get('created_at') else datetime.now(),
            last_activity=datetime.fromisoformat(row_dict['last_activity']) if row_dict.get('last_activity') else datetime.now(),
            messages=safe_json_loads(row_dict.get('messages'), []),
            active=bool(row_dict.get('active')),
            wp_token=row_dict.get('wp_token'),
            timeout_saved_to_crm=bool(row_dict.get('timeout_saved_to_crm')),
            fingerprint_id=row_dict.get('fingerprint_id'),
            fingerprint_method=row_dict.get('fingerprint_method'),
            visitor_type=row_dict.get('visitor_type', 'new_visitor'),
            recognition_response=row_dict.get('recognition_response'),
            daily_question_count=row_dict.get('daily_question_count', 0),
            total_question_count=row_dict.get('total_question_count', 0),
            last_question_time=datetime.fromisoformat(row_dict['last_question_time']) if row_dict.get('last_question_time') else None,
            question_limit_reached=bool(row_dict.get('question_limit_reached')),
            ban_status=BanStatus(row_dict['ban_status']) if row_dict.get('ban_status') else BanStatus.NONE,
            ban_start_time=datetime.fromisoformat(row_dict['ban_start_time']) if row_dict.get('ban_start_time') else None,
            ban_end_time=datetime.fromisoformat(row_dict['ban_end_time']) if row_dict.get('ban_end_time') else None,
            ban_reason=row_dict.get('ban_reason'),
            evasion_count=row_dict.get('evasion_count', 0),
            current_penalty_hours=row_dict.get('current_penalty_hours', 0),
            escalation_level=row_dict.get('escalation_level', 0),
            email_addresses_used=safe_json_loads(row_dict.get('email_addresses_used'), []),
            email_switches_count=row_dict.get('email_switches_count', 0),
            browser_privacy_level=row_dict.get('browser_privacy_level'),
            registration_prompted=bool(row_dict.get('registration_prompted')),
            registration_link_clicked=bool(row_dict.get('registration_link_clicked')),
            display_message_offset=row_dict.get('display_message_offset', 0),
            reverification_pending=bool(row_dict.get('reverification_pending')),
            pending_user_type=UserType(row_dict['pending_user_type']) if row_dict.get('pending_user_type') else None,
            pending_email=row_dict.get('pending_email'),
            pending_full_name=row_dict.get('pending_full_name'),
            pending_zoho_contact_id=row_dict.get('pending_zoho_contact_id'),
            pending_wp_token=row_dict.get('pending_wp_token'),
            declined_recognized_email_at=datetime.fromisoformat(row_dict['declined_recognized_email_at']) if row_dict.get('declined_recognized_email_at') else None,
            timeout_detected_at=datetime.fromisoformat(row_dict['timeout_detected_at']) if row_dict.get('timeout_detected_at') else None,
            timeout_reason=row_dict.get('timeout_reason')
        )

    async def load_session(self, session_id: str) -> Optional[UserSession]:
        await self._ensure_connection()
        if self.db_type == "memory":
            return copy.deepcopy(self.local_sessions.get(session_id))
        try:
            # Set row factory only for regular SQLite, not SQLiteCloud
            if self.db_type == "file":
                self.conn.row_factory = sqlite3.Row
                
            cursor = await asyncio.to_thread(
                self.conn.execute,
                "SELECT * FROM sessions WHERE session_id = ? AND active = 1",
                (session_id,)
            )
            row = await asyncio.to_thread(cursor.fetchone)
            
            # Convert row to dict based on db type
            row_dict = self._row_to_dict(cursor, row)
            
            # Reset row factory if we set it
            if self.db_type == "file":
                self.conn.row_factory = None
                
            return self._build_session_from_row(row_dict) if row_dict else None
        except Exception as e:
            logger.error(f"❌ Failed to load session {session_id[:8]}: {e}", exc_info=True)
            return None

    async def save_session(self, session: UserSession):
        await self._ensure_connection()
        if self.db_type == "memory":
            self.local_sessions[session.session_id] = copy.deepcopy(session)
            return
        
        session_dict = {
            "session_id": session.session_id,
            "user_type": session.user_type.value,
            "email": session.email,
            "full_name": session.full_name,
            "zoho_contact_id": session.zoho_contact_id,
            "created_at": session.created_at.isoformat(),
            "last_activity": session.last_activity.isoformat(),
            "messages": json.dumps(session.messages),
            "active": int(session.active),
            "wp_token": session.wp_token,
            "timeout_saved_to_crm": int(session.timeout_saved_to_crm),
            "fingerprint_id": session.fingerprint_id,
            "fingerprint_method": session.fingerprint_method,
            "visitor_type": session.visitor_type,
            "daily_question_count": session.daily_question_count,
            "total_question_count": session.total_question_count,
            "last_question_time": session.last_question_time.isoformat() if session.last_question_time else None,
            "question_limit_reached": int(session.question_limit_reached),
            "ban_status": session.ban_status.value,
            "ban_start_time": session.ban_start_time.isoformat() if session.ban_start_time else None,
            "ban_end_time": session.ban_end_time.isoformat() if session.ban_end_time else None,
            "ban_reason": session.ban_reason,
            "evasion_count": session.evasion_count,
            "current_penalty_hours": session.current_penalty_hours,
            "escalation_level": session.escalation_level,
            "email_addresses_used": json.dumps(session.email_addresses_used),
            "email_switches_count": session.email_switches_count,
            "browser_privacy_level": session.browser_privacy_level,
            "registration_prompted": int(session.registration_prompted),
            "registration_link_clicked": int(session.registration_link_clicked),
            "recognition_response": session.recognition_response,
            "display_message_offset": session.display_message_offset,
            "reverification_pending": int(session.reverification_pending),
            "pending_user_type": session.pending_user_type.value if session.pending_user_type else None,
            "pending_email": session.pending_email,
            "pending_full_name": session.pending_full_name,
            "pending_zoho_contact_id": session.pending_zoho_contact_id,
            "pending_wp_token": session.pending_wp_token,
            "declined_recognized_email_at": session.declined_recognized_email_at.isoformat() if session.declined_recognized_email_at else None,
            "timeout_detected_at": session.timeout_detected_at.isoformat() if session.timeout_detected_at else None,
            "timeout_reason": session.timeout_reason
        }

        columns = ', '.join(session_dict.keys())
        placeholders = ', '.join(['?'] * len(session_dict))
        
        try:
            await asyncio.to_thread(
                self.conn.execute,
                f"INSERT OR REPLACE INTO sessions ({columns}) VALUES ({placeholders})",
                tuple(session_dict.values())
            )
            await asyncio.to_thread(self.conn.commit)
        except Exception as e:
            logger.error(f"❌ Failed to save session {session.session_id[:8]}: {e}", exc_info=True)
            logger.warning(f"⚠️ Saved session {session.session_id[:8]} to memory as fallback.")
            self.local_sessions[session.session_id] = copy.deepcopy(session)

    async def cleanup_expired_sessions(self, expiry_minutes: int = 5, limit: int = 10):
        logger.info(f"🧹 Starting cleanup for sessions inactive for >{expiry_minutes}m.")
        await self._ensure_connection()
        if self.db_type == "memory":
            logger.warning("Cleanup skipped: in-memory mode does not support automated cleanup.")
            return {"success": False, "reason": "in_memory_mode"}
        
        cutoff_iso = (datetime.now() - timedelta(minutes=expiry_minutes)).isoformat()
        try:
            # Set row factory only for regular SQLite
            if self.db_type == "file":
                self.conn.row_factory = sqlite3.Row
                
            cursor = await asyncio.to_thread(
                self.conn.execute,
                "SELECT * FROM sessions WHERE active = 1 AND last_activity < ? ORDER BY last_activity ASC LIMIT ?",
                (cutoff_iso, limit)
            )
            rows = await asyncio.to_thread(cursor.fetchall)
            
            # Reset row factory
            if self.db_type == "file":
                self.conn.row_factory = None
            
            cleaned_count = 0
            for row in rows:
                row_dict = self._row_to_dict(cursor, row)
                session = self._build_session_from_row(row_dict)
                
                if is_crm_eligible(session):
                    # Save to CRM before deactivating
                    save_result = await zoho_manager.save_chat_transcript(session, "Automated Session Timeout Cleanup")
                    session.timeout_saved_to_crm = save_result.get("success", False)

                session.active = False
                await self.save_session(session)
                cleaned_count += 1
            
            logger.info(f"✅ Cleanup complete. Processed {cleaned_count} sessions.")
            return {"success": True, "cleaned_up_count": cleaned_count}
        except Exception as e:
            logger.error(f"❌ Failed to cleanup expired sessions: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

class PDFExporter:
    """Generates PDF chat transcripts."""
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(
            name='UserMessage',
            backColor=lightgrey,
            fontSize=10,
            leading=14,
            spaceAfter=6
        ))

    async def generate_chat_pdf(self, session: UserSession) -> Optional[io.BytesIO]:
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter)
            story = [
                Paragraph("FiFi AI Chat Transcript", self.styles['Heading1']),
                Paragraph(f"Session ID: {session.session_id}", self.styles['Normal']),
                Paragraph(f"User: {session.full_name or 'Anonymous'} ({session.email or 'No email'})", self.styles['Normal']),
                Paragraph(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", self.styles['Normal']),
                Spacer(1, 12)
            ]
            for msg in session.messages:
                content = html.escape(str(msg.get('content', '')))
                content = re.sub(r'<[^>]+>', '', content)
                role = msg.get('role', 'unknown').capitalize()
                style = self.styles['UserMessage'] if role == 'User' else self.styles['Normal']
                story.extend([Spacer(1, 8), Paragraph(f"<b>{role}:</b> {content}", style)])
            
            await asyncio.to_thread(doc.build, story)
            buffer.seek(0)
            return buffer
        except Exception as e:
            logger.error(f"❌ PDF generation failed for session {session.session_id[:8]}: {e}", exc_info=True)
            return None

class ZohoCRMManager:
    """Manages interactions with the Zoho CRM API."""
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._http_client = httpx.AsyncClient(timeout=30)

    async def _get_access_token(self) -> Optional[str]:
        if not ZOHO_ENABLED:
            return None
        if self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._access_token
        try:
            logger.info("🔄 Refreshing Zoho access token...")
            response = await self._http_client.post(
                "https://accounts.zoho.com/oauth/v2/token",
                data={
                    'refresh_token': ZOHO_REFRESH_TOKEN,
                    'client_id': ZOHO_CLIENT_ID,
                    'client_secret': ZOHO_CLIENT_SECRET,
                    'grant_type': 'refresh_token'
                }
            )
            response.raise_for_status()
            data = response.json()
            self._access_token = data['access_token']
            self._token_expiry = datetime.now() + timedelta(minutes=50)
            logger.info("✅ Zoho token refreshed successfully.")
            return self._access_token
        except Exception as e:
            logger.error(f"❌ Failed to get Zoho access token: {e}", exc_info=True)
            return None

    async def _find_or_create_contact(self, session: UserSession) -> Optional[str]:
        """Finds a contact by email or creates a new one."""
        if not session.email:
            return None
        token = await self._get_access_token()
        if not token:
            return None
        headers = {'Authorization': f'Zoho-oauthtoken {token}'}
        
        # Search for existing contact
        try:
            search_res = await self._http_client.get(
                f"{self.base_url}/Contacts/search",
                headers=headers,
                params={'email': session.email}
            )
            if search_res.status_code == 200 and search_res.json().get('data'):
                return search_res.json()['data'][0]['id']
        except Exception as e:
            logger.error(f"Error searching for Zoho contact {session.email}: {e}")

        # Create new contact if not found
        try:
            create_res = await self._http_client.post(
                f"{self.base_url}/Contacts",
                headers=headers,
                json={
                    "data": [{
                        "Last_Name": session.full_name or "Food Professional",
                        "Email": session.email,
                        "Lead_Source": "FiFi AI Chat"
                    }]
                }
            )
            create_res.raise_for_status()
            return create_res.json()['data'][0]['details']['id']
        except Exception as e:
            logger.error(f"Error creating Zoho contact {session.email}: {e}")
            return None

    async def save_chat_transcript(self, session: UserSession, trigger_reason: str) -> Dict[str, Any]:
        """Saves a chat transcript as a PDF attachment and a note in Zoho CRM."""
        if not ZOHO_ENABLED:
            return {"success": False, "reason": "zoho_disabled"}
        if not is_crm_eligible(session):
            return {"success": False, "reason": "not_eligible"}

        contact_id = session.zoho_contact_id or await self._find_or_create_contact(session)
        if not contact_id:
            return {"success": False, "reason": "contact_failed"}

        pdf_buffer = await self.pdf_exporter.generate_chat_pdf(session)
        if not pdf_buffer:
            return {"success": False, "reason": "pdf_generation_failed"}
        
        token = await self._get_access_token()
        if not token:
            return {"success": False, "reason": "auth_failed"}
        
        # Upload PDF as attachment
        try:
            filename = f"FiFi_Chat_{session.session_id[:8]}_{datetime.now().strftime('%Y%m%d')}.pdf"
            upload_res = await self._http_client.post(
                f"{self.base_url}/Contacts/{contact_id}/Attachments",
                headers={'Authorization': f'Zoho-oauthtoken {token}'},
                files={'file': (filename, pdf_buffer, 'application/pdf')}
            )
            upload_res.raise_for_status()
            pdf_success = upload_res.json()['data'][0]['code'] == 'SUCCESS'
        except Exception as e:
            logger.error(f"❌ Failed to upload PDF for session {session.session_id[:8]}: {e}")
            pdf_success = False
            
        return {"success": pdf_success, "contact_id": contact_id, "pdf_attached": pdf_success}

# --- 4. Helper Functions & Background Tasks ---

def is_crm_eligible(session: UserSession) -> bool:
    """Determine if a session is eligible for saving to CRM."""
    return (
        session.email is not None and
        len(session.messages) > 1 and
        session.user_type in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]
    )

async def _perform_emergency_crm_save(session_id: str, reason: str):
    """Background task to load a session and save it to CRM."""
    logger.info(f"Background task started: Emergency save for {session_id[:8]}, reason: {reason}")
    session = await db_manager.load_session(session_id)
    if not session:
        logger.error(f"Emergency save failed: Session {session_id[:8]} not found.")
        return
    
    if session.timeout_saved_to_crm:
        logger.info(f"Emergency save skipped: Session {session_id[:8]} already saved.")
        return

    save_result = await zoho_manager.save_chat_transcript(session, reason)
    
    session.last_activity = datetime.now()
    session.timeout_saved_to_crm = save_result.get("success", False)
    if is_session_ending_reason(reason):
        session.active = False
    if save_result.get("contact_id") and not session.zoho_contact_id:
        session.zoho_contact_id = save_result["contact_id"]
    
    await db_manager.save_session(session)
    logger.info(f"Background task finished for {session_id[:8]}. Success: {save_result.get('success')}")

async def _perform_full_cleanup_in_background():
    """Background task for cleaning up all expired sessions."""
    logger.info("Background task started: Full cleanup of expired sessions.")
    await db_manager.cleanup_expired_sessions()
    logger.info("Background task finished: Full cleanup.")

# --- 5. FastAPI Startup & API Endpoints ---

@app.on_event("startup")
async def startup_event():
    """Initializes all global manager instances when the application starts."""
    global db_manager, pdf_exporter, zoho_manager
    logger.info("🚀 FastAPI startup: Initializing managers...")
    pdf_exporter = PDFExporter()
    db_manager = ResilientDatabaseManager(SQLITE_CLOUD_CONNECTION)
    zoho_manager = ZohoCRMManager(pdf_exporter)
    await db_manager.test_connection()
    logger.info(f"✅ FastAPI startup complete. DB Mode: {db_manager.db_type}, Zoho Enabled: {ZOHO_ENABLED}")

@app.options("/{path:path}")
async def options_handler(path: str):
    return {"status": "ok"}

@app.get("/")
async def root():
    return {
        "message": "FiFi Backend API is running.",
        "version": app.version,
        "database_status": db_manager.db_type if db_manager else "initializing",
        "zoho_status": "enabled" if ZOHO_ENABLED else "disabled"
    }

@app.get("/health")
async def health_check():
    db_status = await db_manager.test_connection()
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "database": db_status
    }

@app.post("/emergency-save")
async def emergency_save_endpoint(request: EmergencySaveRequest, background_tasks: BackgroundTasks):
    """Queues a background task to save a chat session to the CRM upon user exit."""
    background_tasks.add_task(_perform_emergency_crm_save, request.session_id, request.reason)
    return {
        "success": True,
        "message": "Emergency save queued.",
        "session_id": request.session_id
    }

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions_endpoint(background_tasks: BackgroundTasks):
    """Queues a background task to find and process inactive sessions."""
    background_tasks.add_task(_perform_full_cleanup_in_background)
    return {
        "success": True,
        "message": "Cleanup task queued."
    }

@app.post("/fingerprint")
async def receive_fingerprint(payload: FingerprintPayload):
    """Receives fingerprint data and updates the corresponding session."""
    logger.info(f"Received fingerprint for session: {payload.session_id[:8]} using method '{payload.method}'")
    
    if not db_manager:
        logger.error("Database manager not initialized during fingerprint request.")
        raise HTTPException(status_code=503, detail="Service not ready")
        
    try:
        session = await db_manager.load_session(payload.session_id)
        if not session:
            logger.error(f"Session not found for fingerprint update: {payload.session_id}")
            raise HTTPException(status_code=404, detail="Session not found")

        # Update session object with fingerprint data
        session.fingerprint_id = payload.fingerprint_id
        session.fingerprint_method = payload.method
        session.browser_privacy_level = payload.privacy
        session.last_activity = datetime.now()
        
        await db_manager.save_session(session)
        logger.info(f"✅ Successfully updated fingerprint for session {payload.session_id[:8]}")
        return {
            "status": "success",
            "message": "Fingerprint data received and updated."
        }

    except Exception as e:
        logger.error(f"❌ Failed to process fingerprint for session {payload.session_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while processing fingerprint")

# --- 6. Main Execution Block ---

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"🚀 Starting FiFi Backend API on port {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
