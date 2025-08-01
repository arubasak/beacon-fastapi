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
    messages: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    timeout_saved_to_crm: bool = False
    daily_question_count: int = 0
    total_question_count: int = 0
    last_question_time: Optional[datetime] = None
    ban_status: BanStatus = BanStatus.NONE
    ban_end_time: Optional[datetime] = None
    email_addresses_used: List[str] = field(default_factory=list)

# Utility functions
def safe_json_loads(data: Optional[str], default_value: Any = None) -> Any:
    if data is None or data == "":
        return default_value
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return default_value

# Database Manager
class DatabaseManager:
    def __init__(self, connection_string: Optional[str]):
        self.lock = threading.Lock()
        self.conn = None
        
        # Try SQLite Cloud first
        if connection_string:
            try:
                import sqlitecloud
                self.conn = sqlitecloud.connect(connection_string)
                self.conn.execute("SELECT 1").fetchone()
                logger.info("✅ SQLite Cloud connection established!")
                self.db_type = "cloud"
            except Exception as e:
                logger.error(f"SQLite Cloud failed: {e}")
                self.conn = None
        
        # Fallback to local SQLite
        if not self.conn:
            try:
                self.conn = sqlite3.connect("fifi_sessions_emergency.db", check_same_thread=False)
                self.conn.execute("SELECT 1").fetchone()
                logger.info("✅ Local SQLite connection established!")
                self.db_type = "file"
            except Exception as e:
                logger.error(f"Local SQLite failed: {e}")
                self.conn = None
                self.db_type = "memory"
                self.local_sessions = {}

    def load_session(self, session_id: str) -> Optional[UserSession]:
        with self.lock:
            if self.db_type == "memory":
                return copy.deepcopy(self.local_sessions.get(session_id))
            
            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                cursor = self.conn.execute("""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id, 
                           created_at, last_activity, messages, active, timeout_saved_to_crm,
                           daily_question_count, total_question_count, last_question_time,
                           ban_status, ban_end_time, email_addresses_used
                    FROM sessions WHERE session_id = ? AND active = 1
                """, (session_id,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                return UserSession(
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
                    daily_question_count=row[10] or 0,
                    total_question_count=row[11] or 0,
                    last_question_time=datetime.fromisoformat(row[12]) if row[12] else None,
                    ban_status=BanStatus(row[13]) if row[13] else BanStatus.NONE,
                    ban_end_time=datetime.fromisoformat(row[14]) if row[14] else None,
                    email_addresses_used=safe_json_loads(row[15], [])
                )
            except Exception as e:
                logger.error(f"Failed to load session {session_id[:8]}: {e}")
                return None

    def save_session(self, session: UserSession):
        with self.lock:
            if self.db_type == "memory":
                self.local_sessions[session.session_id] = copy.deepcopy(session)
                return
            
            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                self.conn.execute("""
                    REPLACE INTO sessions (
                        session_id, user_type, email, full_name, zoho_contact_id,
                        created_at, last_activity, messages, active, timeout_saved_to_crm,
                        daily_question_count, total_question_count, last_question_time,
                        ban_status, ban_end_time, email_addresses_used
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    session.session_id, session.user_type.value, session.email, session.full_name,
                    session.zoho_contact_id, session.created_at.isoformat(),
                    session.last_activity.isoformat(), json.dumps(session.messages),
                    int(session.active), int(session.timeout_saved_to_crm),
                    session.daily_question_count, session.total_question_count,
                    session.last_question_time.isoformat() if session.last_question_time else None,
                    session.ban_status.value,
                    session.ban_end_time.isoformat() if session.ban_end_time else None,
                    json.dumps(session.email_addresses_used)
                ))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Failed to save session {session.session_id[:8]}: {e}")

    def get_all_active_sessions(self) -> List[UserSession]:
        with self.lock:
            if self.db_type == "memory":
                return [copy.deepcopy(s) for s in self.local_sessions.values() if s.active]
            
            try:
                if hasattr(self.conn, 'row_factory'):
                    self.conn.row_factory = None
                
                cursor = self.conn.execute("""
                    SELECT session_id, user_type, email, full_name, zoho_contact_id,
                           created_at, last_activity, messages, active, timeout_saved_to_crm,
                           daily_question_count, total_question_count, last_question_time,
                           ban_status, ban_end_time, email_addresses_used
                    FROM sessions WHERE active = 1 ORDER BY last_activity ASC
                """)
                
                sessions = []
                for row in cursor.fetchall():
                    try:
                        session = UserSession(
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
                            daily_question_count=row[10] or 0,
                            total_question_count=row[11] or 0,
                            last_question_time=datetime.fromisoformat(row[12]) if row[12] else None,
                            ban_status=BanStatus(row[13]) if row[13] else BanStatus.NONE,
                            ban_end_time=datetime.fromisoformat(row[14]) if row[14] else None,
                            email_addresses_used=safe_json_loads(row[15], [])
                        )
                        sessions.append(session)
                    except Exception as e:
                        logger.error(f"Error converting row to session: {e}")
                        continue
                return sessions
            except Exception as e:
                logger.error(f"Failed to get active sessions: {e}")
                return []

# PDF Exporter
class PDFExporter:
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.styles.add(ParagraphStyle(name='UserMessage', backColor=lightgrey))

    def generate_chat_pdf(self, session: UserSession) -> Optional[io.BytesIO]:
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter)
            story = [Paragraph("FiFi AI Chat Transcript", self.styles['Heading1'])]
            
            for msg in session.messages:
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
            logger.error(f"PDF generation failed: {e}")
            return None

# Zoho CRM Manager
class ZohoCRMManager:
    def __init__(self, pdf_exporter: PDFExporter):
        self.pdf_exporter = pdf_exporter
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self._access_token = None
        self._token_expiry = None

    def _get_access_token(self) -> Optional[str]:
        if not ZOHO_ENABLED:
            return None

        if self._access_token and self._token_expiry and datetime.now() < self._token_expiry:
            return self._access_token
        
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
            return self._access_token
        except Exception as e:
            logger.error(f"Failed to get Zoho access token: {e}")
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
                return data['data'][0]['id']
        except Exception as e:
            logger.error(f"Error finding contact by email {email}: {e}")
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
                    "Lead_Source": "FiFi AI Assistant Emergency Save"
                }]
            }
            response = requests.post(f"{self.base_url}/Contacts", headers=headers, json=contact_data, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data'][0]['code'] == 'SUCCESS':
                return data['data'][0]['details']['id']
        except Exception as e:
            logger.error(f"Error creating contact for {email}: {e}")
        return None

    def _add_note(self, contact_id: str, note_title: str, note_content: str) -> bool:
        access_token = self._get_access_token()
        if not access_token:
            return False

        try:
            headers = {'Authorization': f'Zoho-oauthtoken {access_token}', 'Content-Type': 'application/json'}
            
            if len(note_content) > 32000:
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
            
            return 'data' in data and data['data'][0]['code'] == 'SUCCESS'
        except Exception as e:
            logger.error(f"Error adding note: {e}")
            return False

    def save_chat_transcript_sync(self, session: UserSession, trigger_reason: str) -> bool:
        if not ZOHO_ENABLED or not session.email or not session.messages:
            return False
        
        try:
            contact_id = self._find_contact_by_email(session.email)
            if not contact_id:
                contact_id = self._create_contact(session.email, session.full_name)
            if not contact_id:
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
            note_content += f"- Questions Asked: {session.daily_question_count}\n\n"
            note_content += "**Conversation Transcript:**\n"
            
            for i, msg in enumerate(session.messages):
                role = msg.get("role", "Unknown").capitalize()
                content = re.sub(r'<[^>]+>', '', msg.get("content", ""))
                
                if len(content) > 500:
                    content = content[:500] + "..."
                    
                note_content += f"\n{i+1}. **{role}:** {content}\n"
                
            return self._add_note(contact_id, note_title, note_content)
        except Exception as e:
            logger.error(f"Emergency CRM save failed: {e}")
            return False

# Initialize managers
db_manager = DatabaseManager(SQLITE_CLOUD_CONNECTION)
pdf_exporter = PDFExporter()
zoho_manager = ZohoCRMManager(pdf_exporter)

# Helper functions
def is_crm_eligible(session: UserSession) -> bool:
    try:
        if not session.email or not session.messages:
            return False
        
        if session.user_type not in [UserType.REGISTERED_USER, UserType.EMAIL_VERIFIED_GUEST]:
            return False
            
        if session.daily_question_count < 1:
            return False
            
        # 15-minute check
        elapsed_time = datetime.now() - session.created_at
        if elapsed_time.total_seconds() / 60 < 15.0:
            return False
            
        return True
    except Exception:
        return False

# API Endpoints
@app.get("/")
async def root():
    return {"message": "FiFi Emergency API", "status": "running"}

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "database": "connected" if db_manager.conn else "disconnected",
        "zoho": "enabled" if ZOHO_ENABLED else "disabled"
    }

@app.post("/emergency-save")
async def emergency_save(request: EmergencySaveRequest):
    try:
        logger.info(f"🚨 EMERGENCY SAVE: Request for session {request.session_id[:8]}")
        
        session = db_manager.load_session(request.session_id)
        if not session:
            logger.error(f"Session {request.session_id[:8]} not found")
            return {"success": False, "reason": "session_not_found"}
        
        if not is_crm_eligible(session):
            logger.info(f"Session {request.session_id[:8]} not eligible for CRM")
            return {"success": False, "reason": "not_eligible"}
        
        save_success = zoho_manager.save_chat_transcript_sync(
            session, f"Emergency Save: {request.reason}"
        )
        
        if save_success:
            session.timeout_saved_to_crm = True
            session.last_activity = datetime.now()
            db_manager.save_session(session)
            logger.info(f"✅ Emergency save successful for {request.session_id[:8]}")
            return {"success": True, "saved_to_crm": True}
        else:
            logger.error(f"❌ Emergency save failed for {request.session_id[:8]}")
            return {"success": False, "reason": "crm_failed"}
            
    except Exception as e:
        logger.error(f"Emergency save error: {e}")
        return {"success": False, "reason": "internal_error"}

@app.post("/cleanup-expired-sessions")
async def cleanup_expired_sessions():
    try:
        logger.info("🧹 SESSION CLEANUP: Starting")
        
        results = {"processed": 0, "crm_saved": 0, "marked_inactive": 0, "errors": 0}
        active_sessions = db_manager.get_all_active_sessions()
        
        for session in active_sessions:
            try:
                time_since_activity = datetime.now() - session.last_activity
                if time_since_activity.total_seconds() >= 900:  # 15 minutes
                    results["processed"] += 1
                    
                    if is_crm_eligible(session) and not session.timeout_saved_to_crm:
                        save_success = zoho_manager.save_chat_transcript_sync(
                            session, "Cleanup - Expired Session"
                        )
                        if save_success:
                            session.timeout_saved_to_crm = True
                            results["crm_saved"] += 1
                    
                    session.active = False
                    db_manager.save_session(session)
                    results["marked_inactive"] += 1
                    
            except Exception as e:
                logger.error(f"Error cleaning session {session.session_id[:8]}: {e}")
                results["errors"] += 1
        
        logger.info(f"🧹 CLEANUP COMPLETE: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
