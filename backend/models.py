"""
Database models for ECHO authentication and user management.
"""

import logging
import os
from datetime import datetime
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, EmailStr
from sqlalchemy import Boolean, Column, DateTime, Enum as SQLEnum, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

logger = logging.getLogger(__name__)
Base = declarative_base()


def get_database_url() -> str:
    """Resolve a local working database for development and keep startup resilient."""
    configured = os.getenv("DATABASE_URL")
    if configured:
        return configured

    project_root = Path(__file__).resolve().parents[1]
    sqlite_path = project_root / "echo_dev.db"
    return f"sqlite:///{sqlite_path}"


DATABASE_URL = get_database_url()
engine_kwargs = {"connect_args": {"check_same_thread": False}} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine)

try:
    Base.metadata.create_all(bind=engine)
except Exception as exc:  # pragma: no cover - local fallback behaviour
    logger.warning("Database schema creation deferred during startup: %s", exc)


class UserRole(str, Enum):
    """User role enumeration"""
    ADMIN = "admin"
    FACULTY = "faculty"
    STUDENT = "student"


# ========================
# Pydantic Models (API)
# ========================

class UserBase(BaseModel):
    """Base user model"""
    email: EmailStr
    full_name: str
    user_id: str  # Faculty ID or Student ID


class StudentCreate(UserBase):
    """Model for faculty creating a student"""
    student_id: str
    faculty_id: str  # Which faculty is creating


class FacultyCreate(UserBase):
    """Model for admin creating faculty"""
    faculty_id: str


class PasswordChange(BaseModel):
    """Model for changing password"""
    new_password: str


class LoginRequest(BaseModel):
    """Login request model"""
    user_id: str  # Student ID or Faculty ID
    password: str


class LoginResponse(BaseModel):
    """Login response model"""
    access_token: str
    token_type: str
    user_role: UserRole
    user_id: str
    full_name: str


class UserResponse(BaseModel):
    """User response model"""
    user_id: str
    email: str
    full_name: str
    role: UserRole
    created_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


# ========================
# SQLAlchemy Models (DB)
# ========================

class User(Base):
    """Main user table"""
    __tablename__ = "users"

    id = Column(String(50), primary_key=True)  # student_id or faculty_id
    email = Column(String(255), unique=True, nullable=False, index=True)
    full_name = Column(String(255), nullable=False)
    role = Column(SQLEnum(UserRole), nullable=False, index=True)
    
    # Password hashing
    hashed_password = Column(String(255), nullable=False)
    password_changed = Column(Boolean, default=False)  # True if user changed password
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Status
    is_active = Column(Boolean, default=True)
    
    # For students: which faculty created them
    created_by = Column(String(50), nullable=True)  # faculty_id who created this student

    def __repr__(self):
        return f"<User {self.id} ({self.role})>"

