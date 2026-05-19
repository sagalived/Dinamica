from pydantic import BaseModel, ConfigDict, EmailStr, Field, AliasChoices
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


class MeResponse(BaseModel):
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
    start_date: datetime | None = Field(default=None, validation_alias=AliasChoices("start_date", "startDate"))
    end_date: datetime | None = Field(default=None, validation_alias=AliasChoices("end_date", "endDate"))
    color: str = "blue"
    building_id: int = Field(validation_alias=AliasChoices("building_id", "buildingId"))

    # Integração Bitrix24
    bitrix_group_id: int | None = Field(default=None, validation_alias=AliasChoices("bitrix_group_id", "bitrixGroupId"))

    # Integração Bitrix24 CRM (Smart Process)
    bitrix_crm_entity_type_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("bitrix_crm_entity_type_id", "bitrixCrmEntityTypeId"),
    )
    bitrix_crm_category_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("bitrix_crm_category_id", "bitrixCrmCategoryId"),
    )

    # Vínculo de contrato
    contract_ref: str | None = Field(default=None, validation_alias=AliasChoices("contract_ref", "contractRef"))


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

    bitrix_group_id: int | None = None
    bitrix_last_pull_at: datetime | None = None

    bitrix_crm_entity_type_id: int | None = None
    bitrix_crm_category_id: int | None = None
    bitrix_crm_last_pull_at: datetime | None = None

    contract_ref: str | None = None


class CardRequest(BaseModel):
    title: str
    description: str | None = None
    status: str = "planned"
    priority: str = "medium"
    responsible: str | None = None
    due_date: datetime | None = Field(default=None, validation_alias=AliasChoices("due_date", "dueDate"))
    tags: str | None = None
    sprint_id: int = Field(validation_alias=AliasChoices("sprint_id", "sprintId"))
    building_id: int = Field(validation_alias=AliasChoices("building_id", "buildingId"))


# ========== CATALOG SCHEMAS ==========
class BuildingCreateRequest(BaseModel):
    name: str
    company_id: int | None = Field(default=None, validation_alias=AliasChoices("company_id", "companyId"))
    company_name: str | None = Field(default=None, validation_alias=AliasChoices("company_name", "companyName"))
    cnpj: str | None = None
    address: str | None = None
    building_type: str | None = Field(default=None, validation_alias=AliasChoices("building_type", "buildingType"))
    active: bool = True


class BuildingCatalogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    company_id: int | None = None
    company_name: str | None = None
    cnpj: str | None = None
    address: str | None = None
    building_type: str | None = None
    active: bool


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

    bitrix_task_id: int | None = None
    bitrix_last_pull_at: datetime | None = None

    bitrix_crm_entity_type_id: int | None = None
    bitrix_crm_category_id: int | None = None
    bitrix_crm_item_id: int | None = None
    bitrix_crm_stage_id: str | None = None
    bitrix_crm_last_pull_at: datetime | None = None


# ========== JURIDICO / CONTRATOS ==========


class ContractRequest(BaseModel):
    building_id: int = Field(validation_alias=AliasChoices("building_id", "buildingId"))

    cost_center_code: str | None = Field(default=None, validation_alias=AliasChoices("cost_center_code", "costCenterCode"))
    stage_label: str | None = Field(default=None, validation_alias=AliasChoices("stage_label", "stageLabel"))
    enterprise_label: str | None = Field(default=None, validation_alias=AliasChoices("enterprise_label", "enterpriseLabel"))

    contract_number: str | None = Field(default=None, validation_alias=AliasChoices("contract_number", "contractNumber"))
    title: str

    kind: str = Field(default="privado", validation_alias=AliasChoices("kind", "contract_kind", "contractKind"))

    owner_legal_name: str | None = Field(default=None, validation_alias=AliasChoices("owner_legal_name", "ownerLegalName"))
    owner_cnpj: str | None = Field(default=None, validation_alias=AliasChoices("owner_cnpj", "ownerCnpj"))
    owner_address: str | None = Field(default=None, validation_alias=AliasChoices("owner_address", "ownerAddress"))
    owner_representatives: str | None = Field(default=None, validation_alias=AliasChoices("owner_representatives", "ownerRepresentatives"))

    supplier_legal_name: str | None = Field(default=None, validation_alias=AliasChoices("supplier_legal_name", "supplierLegalName"))
    supplier_cnpj: str | None = Field(default=None, validation_alias=AliasChoices("supplier_cnpj", "supplierCnpj"))
    supplier_address: str | None = Field(default=None, validation_alias=AliasChoices("supplier_address", "supplierAddress"))
    supplier_representatives: str | None = Field(default=None, validation_alias=AliasChoices("supplier_representatives", "supplierRepresentatives"))

    object_text: str | None = Field(default=None, validation_alias=AliasChoices("object_text", "objectText"))
    start_date: datetime | None = Field(default=None, validation_alias=AliasChoices("start_date", "startDate"))
    end_date: datetime | None = Field(default=None, validation_alias=AliasChoices("end_date", "endDate"))

    total_value: float | None = Field(default=None, validation_alias=AliasChoices("total_value", "totalValue"))
    retention_percent: float | None = Field(default=None, validation_alias=AliasChoices("retention_percent", "retentionPercent"))

    guarantee_text: str | None = Field(default=None, validation_alias=AliasChoices("guarantee_text", "guaranteeText"))
    insurance_text: str | None = Field(default=None, validation_alias=AliasChoices("insurance_text", "insuranceText"))
    status: str = "draft"

    notes: str | None = None


class ContractDocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    contract_id: int
    doc_type: str
    title: str | None
    filename: str
    file_size: int | None
    mime_type: str | None
    uploaded_by: str | None
    version: int
    created_at: datetime


class ContractResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    building_id: int
    cost_center_code: str | None
    stage_label: str | None
    enterprise_label: str | None

    contract_number: str | None
    title: str

    kind: str

    owner_legal_name: str | None
    owner_cnpj: str | None
    owner_address: str | None
    owner_representatives: str | None

    supplier_legal_name: str | None
    supplier_cnpj: str | None
    supplier_address: str | None
    supplier_representatives: str | None

    object_text: str | None
    start_date: datetime | None
    end_date: datetime | None

    total_value: float | None
    retention_percent: float | None
    guarantee_text: str | None
    insurance_text: str | None
    status: str

    notes: str | None
    created_by: str | None
    sprint_id: int | None

    created_at: datetime
    updated_at: datetime

    documents: list[ContractDocumentResponse] = []


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
    pedidos: list[dict] = []
    financeiro: list[dict] = []
    receber: list[dict] = []
    itensPedidos: dict = {}
    saldoBancario: float | None = None
    latestSync: dict | None = None
    cacheReady: bool = False
    cacheCounts: dict = {}


class FetchItemsRequest(BaseModel):
    ids: list[int]


class FetchQuotationsRequest(BaseModel):
    ids: list[int]
