"""
Authentication routes — registration and login endpoints.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.auth import (
    hash_password,
    verify_password,
    create_access_token,
    decode_token,
    generate_default_student_password,
    generate_default_faculty_password,
)
from backend.models import User, UserRole, SessionLocal, StudentCreate, FacultyCreate, LoginRequest, LoginResponse, UserResponse, PasswordChange

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = logging.getLogger(__name__)


def get_db():
    """Dependency: get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(token: str, db: Session = Depends(get_db)) -> User:
    """Dependency: get current user from JWT token"""
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )
    
    user_id = payload.get("sub")
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    return user


# ========================
# ADMIN ENDPOINTS
# ========================

@router.post("/admin/create-faculty", response_model=dict)
def admin_create_faculty(
    faculty: FacultyCreate,
    db: Session = Depends(get_db)
):
    """
    Admin endpoint to create a faculty account.
    Admin generates password and provides it to faculty.
    """
    # Check if faculty already exists
    existing_user = db.query(User).filter(User.id == faculty.faculty_id).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Faculty {faculty.faculty_id} already exists"
        )
    
    # Check if email already exists
    existing_email = db.query(User).filter(User.email == faculty.email).first()
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Email {faculty.email} already in use"
        )
    
    # Generate default password
    default_password = generate_default_faculty_password(faculty.faculty_id)
    hashed_password = hash_password(default_password)
    
    # Create faculty user
    new_faculty = User(
        id=faculty.faculty_id,
        email=faculty.email,
        full_name=faculty.full_name,
        role=UserRole.FACULTY,
        hashed_password=hashed_password,
        password_changed=False,
        created_by="admin"
    )
    
    db.add(new_faculty)
    db.commit()
    db.refresh(new_faculty)
    
    logger.info(f"Faculty {faculty.faculty_id} created by admin")
    
    return {
        "status": "success",
        "message": f"Faculty {faculty.faculty_id} created successfully",
        "faculty_id": faculty.faculty_id,
        "email": faculty.email,
        "temporary_password": default_password,
        "note": "Share this password with the faculty. They should log in and change it."
    }


# ========================
# FACULTY ENDPOINTS
# ========================

@router.post("/faculty/create-student", response_model=dict)
def faculty_create_student(
    student: StudentCreate,
    token: str,
    db: Session = Depends(get_db)
):
    """
    Faculty endpoint to create a student account.
    Faculty generates password and provides it to student.
    """
    # Verify the request is from a faculty member
    current_user = get_current_user(token, db)
    if current_user.role != UserRole.FACULTY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only faculty can create students"
        )
    
    # Check if student already exists
    existing_user = db.query(User).filter(User.id == student.student_id).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Student {student.student_id} already exists"
        )
    
    # Check if email already exists
    existing_email = db.query(User).filter(User.email == student.email).first()
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Email {student.email} already in use"
        )
    
    # Generate default password
    default_password = generate_default_student_password(student.student_id)
    hashed_password = hash_password(default_password)
    
    # Create student user
    new_student = User(
        id=student.student_id,
        email=student.email,
        full_name=student.full_name,
        role=UserRole.STUDENT,
        hashed_password=hashed_password,
        password_changed=False,
        created_by=current_user.id  # Faculty ID who created this student
    )
    
    db.add(new_student)
    db.commit()
    db.refresh(new_student)
    
    logger.info(f"Student {student.student_id} created by faculty {current_user.id}")
    
    return {
        "status": "success",
        "message": f"Student {student.student_id} created successfully",
        "student_id": student.student_id,
        "email": student.email,
        "temporary_password": default_password,
        "note": "Share this password with the student. They should log in and change it."
    }


# ========================
# LOGIN ENDPOINT
# ========================

@router.post("/login", response_model=LoginResponse)
def login(
    credentials: LoginRequest,
    db: Session = Depends(get_db)
):
    """
    Universal login endpoint for students and faculty.
    User ID can be: Student ID, Faculty ID, or Admin ID
    """
    # Find user by ID
    user = db.query(User).filter(User.id == credentials.user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID or password"
        )
    
    # Verify password
    if not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID or password"
        )
    
    # Create access token
    access_token = create_access_token(data={"sub": user.id, "role": user.role})
    
    logger.info(f"User {user.id} ({user.role}) logged in successfully")
    
    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        user_role=user.role,
        user_id=user.id,
        full_name=user.full_name
    )


# ========================
# PASSWORD MANAGEMENT
# ========================

@router.post("/change-password")
def change_password(
    password_change: PasswordChange,
    token: str,
    db: Session = Depends(get_db)
):
    """
    Change user password after login.
    User must be authenticated.
    """
    current_user = get_current_user(token, db)
    
    # Validate new password
    if len(password_change.new_password) < 8:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must be at least 8 characters long"
        )
    
    # Hash and update password
    current_user.hashed_password = hash_password(password_change.new_password)
    current_user.password_changed = True
    
    db.commit()
    logger.info(f"User {current_user.id} changed password")
    
    return {
        "status": "success",
        "message": "Password changed successfully"
    }


# ========================
# USER PROFILE
# ========================

@router.get("/profile", response_model=UserResponse)
def get_profile(
    token: str,
    db: Session = Depends(get_db)
):
    """
    Get current user's profile.
    """
    current_user = get_current_user(token, db)
    return current_user


@router.get("/users/{user_id}", response_model=UserResponse)
def get_user(
    user_id: str,
    token: str,
    db: Session = Depends(get_db)
):
    """
    Get user by ID (only accessible by faculty/admin).
    """
    current_user = get_current_user(token, db)
    
    # Only faculty and admin can view other users
    if current_user.role == UserRole.STUDENT and current_user.id != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Students can only view their own profile"
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return user


# ========================
# VERIFY TOKEN
# ========================

@router.get("/verify-token")
def verify_token(
    token: str,
    db: Session = Depends(get_db)
):
    """
    Verify if token is valid.
    """
    try:
        current_user = get_current_user(token, db)
        return {
            "status": "valid",
            "user_id": current_user.id,
            "role": current_user.role,
            "full_name": current_user.full_name
        }
    except HTTPException:
        return {
            "status": "invalid",
            "message": "Token is invalid or expired"
        }
