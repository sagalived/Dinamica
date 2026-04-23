from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

from backend.config import APP_NAME
from backend.database import Base, engine, get_db
from backend.models import AppUser, Building, Client, Company, Creditor, DirectoryUser
from backend.schemas import AuthResponse, DashboardSummary, LoginRequest, RegisterRequest, UserResponse
from backend.security import create_access_token, decode_access_token, hash_password, verify_password
from backend.services.analytics import build_dashboard_summary
from backend.services.bootstrap import ensure_seed_data
from backend.routers import sienge, kanban, logistics
from backend.dependencies import get_current_user

app = FastAPI(title=APP_NAME, version="2.0.0")

# Register routers
app.include_router(sienge.router)
app.include_router(kanban.router)
app.include_router(logistics.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


@app.on_event("startup")
def on_startup() -> None:
    app.state.database_ready = False
    app.state.database_error = None
    try:
        Base.metadata.create_all(bind=engine)
        with Session(engine) as db:
            ensure_seed_data(db)
        app.state.database_ready = True
    except SQLAlchemyError as exc:
        app.state.database_error = str(exc)


def require_database_ready() -> None:
    if getattr(app.state, "database_ready", False):
        return
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=f"PostgreSQL indisponivel no momento. {getattr(app.state, 'database_error', '')}".strip(),
    )





@app.get("/api/health")
def health() -> dict:
    return {
        "status": "ok",
        "message": "FastAPI online com PostgreSQL, JWT e Pandas.",
        "database_ready": getattr(app.state, "database_ready", False),
        "database_error": getattr(app.state, "database_error", None),
    }


@app.post("/api/auth/register", response_model=UserResponse)
def register(
    payload: RegisterRequest,
    _: None = Depends(require_database_ready),
    db: Session = Depends(get_db),
) -> AppUser:
    existing = db.scalar(select(AppUser).where(AppUser.email == payload.email))
    if existing is not None:
        raise HTTPException(status_code=400, detail="Email ja cadastrado.")

    user = AppUser(
        email=payload.email,
        full_name=payload.full_name,
        department=payload.department,
        role=payload.role,
        password_hash=hash_password(payload.password),
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@app.post("/api/auth/login", response_model=AuthResponse)
def login(
    payload: LoginRequest,
    _: None = Depends(require_database_ready),
    db: Session = Depends(get_db),
) -> dict:
    user = db.scalar(select(AppUser).where(AppUser.email == payload.email))
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Credenciais invalidas.")

    token = create_access_token(user.email)
    return {"access_token": token, "token_type": "bearer", "user": user}


@app.get("/api/dashboard/summary", response_model=DashboardSummary)
def dashboard_summary(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    return build_dashboard_summary(db)


@app.get("/api/admin/users", response_model=list[UserResponse])
def list_admin_users(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[AppUser]:
    return db.scalars(select(AppUser).order_by(AppUser.full_name)).all()


@app.get("/api/directory/users")
def list_directory_users(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(DirectoryUser).order_by(DirectoryUser.name)).all()
    return [
        {"id": row.id, "name": row.name, "email": row.email, "active": row.active}
        for row in rows
    ]


@app.get("/api/companies")
def list_companies(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Company).order_by(Company.name)).all()
    return [
        {"id": row.id, "name": row.name, "trade_name": row.trade_name, "cnpj": row.cnpj}
        for row in rows
    ]


@app.get("/api/buildings")
def list_buildings(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Building).order_by(Building.name)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "company_id": row.company_id,
            "company_name": row.company_name,
            "cnpj": row.cnpj,
            "address": row.address,
            "building_type": row.building_type,
        }
        for row in rows
    ]


@app.get("/api/creditors")
def list_creditors(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Creditor).order_by(Creditor.name)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "trade_name": row.trade_name,
            "cnpj": row.cnpj,
            "city": row.city,
            "state": row.state,
            "active": row.active,
        }
        for row in rows
    ]


@app.get("/api/clients")
def list_clients(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Client).order_by(Client.name)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "fantasy_name": row.fantasy_name,
            "cnpj_cpf": row.cnpj_cpf,
            "city": row.city,
            "state": row.state,
            "email": row.email,
            "phone": row.phone,
            "status": row.status,
        }
        for row in rows
    ]


@app.post("/api/admin/backup/drive")
def backup_to_google_drive(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """
    Backup data to Google Drive.
    Currently a stub — requires Google API credentials and integration.
    """
    try:
        # TODO: Implement actual Google Drive API integration
        # For now, just return a success response with feature flag disabled
        return {
            "status": "pending",
            "message": "Backup Google Drive em desenvolvimento",
            "error": "Feature não disponível nesta versão",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backup failed: {str(e)}")
