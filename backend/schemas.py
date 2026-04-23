from pydantic import BaseModel, ConfigDict, EmailStr
from datetime import datetime


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RegisterRequest(BaseModel):
    email: EmailStr
    full_name: str
    password: str
    department: str | None = None
    role: str = "admin"


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    email: EmailStr
    full_name: str
    department: str | None
    role: str
    is_active: bool


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class SummaryCard(BaseModel):
    label: str
    value: int


class DashboardSummary(BaseModel):
    cards: list[SummaryCard]
    companies_by_buildings: list[dict]
    creditor_states: list[dict]
    client_cities: list[dict]
    active_directory_users: int


# ========== KANBAN SCHEMAS ==========
class SprintRequest(BaseModel):
    name: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    color: str = "blue"
    building_id: int


class SprintResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    building_id: int
    name: str
    start_date: datetime | None
    end_date: datetime | None
    color: str
    created_by: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class CardRequest(BaseModel):
    title: str
    description: str | None = None
    status: str = "todo"
    priority: str = "medium"
    responsible: str | None = None
    due_date: datetime | None = None
    tags: str | None = None
    sprint_id: int
    building_id: int


class AttachmentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    card_id: int
    filename: str
    file_size: int | None
    mime_type: str | None
    uploaded_by: str
    created_at: datetime


class CardResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    sprint_id: int
    building_id: int
    title: str
    description: str | None
    status: str
    priority: str
    responsible: str | None
    due_date: datetime | None
    tags: str | None
    created_by: str
    order: int
    created_at: datetime
    updated_at: datetime
    attachments: list[AttachmentResponse] = []


# ========== LOGISTICS SCHEMAS ==========
class LogisticsLocationRequest(BaseModel):
    code: str
    name: str
    address: str
    latitude: float | None = None
    longitude: float | None = None
    location_type: str | None = None
    source: str | None = None


class LogisticsLocationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    code: str
    name: str
    address: str
    latitude: float | None
    longitude: float | None
    location_type: str | None
    source: str | None
    created_by: str | None
    created_at: datetime
    updated_at: datetime


class RouteDistanceRequest(BaseModel):
    origin: dict  # { address: str, lat?: float, lng?: float }
    destination: dict  # { address: str, lat?: float, lng?: float }


class RouteDistanceResponse(BaseModel):
    distance_km: float
    provider: str
    origin: str
    destination: str


# ========== SIENGE BOOTSTRAP SCHEMAS ==========
class BootstrapResponse(BaseModel):
    obras: list[dict]
    usuarios: list[dict]
    credores: list[dict]
    companies: list[dict]
    pedidos: list[dict]
    financeiro: list[dict]
    receber: list[dict]
    itensPedidos: dict
    saldoBancario: float | None = None
    latestSync: dict | None = None


class FetchItemsRequest(BaseModel):
    ids: list[int]


class FetchQuotationsRequest(BaseModel):
    ids: list[int]
