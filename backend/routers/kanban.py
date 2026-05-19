from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select, delete
from datetime import datetime
from pathlib import Path
import os

from backend.database import get_db
from backend.models import AppUser, Sprint, Card, Attachment, Building
from backend.schemas import SprintRequest, SprintResponse, CardRequest, CardResponse, AttachmentResponse
from backend.dependencies import get_current_user

router = APIRouter(prefix="/api/kanban", tags=["kanban"])

# Path for file uploads
UPLOAD_DIR = Path("uploads/attachments")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_card_status(status_value: str | None) -> str:
    raw = (status_value or "").strip().lower()
    if raw == "todo":
        return "planned"
    if raw == "doing":
        return "in_progress"
    return status_value or "planned"


@router.get("")
def list_sprints_by_building(
    building_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """
    Get all sprints for a given building.
    Returns: { buildings: { [buildingId]: [sprints] } }
    """
    sprints = db.scalars(
        select(Sprint)
        .where(Sprint.building_id == building_id)
        .order_by(Sprint.created_at.desc())
    ).all()
    
    sprints_data = [
        {
            "id": s.id,
            "buildingId": s.building_id,
            "name": s.name,
            "startDate": s.start_date.isoformat() if s.start_date else None,
            "endDate": s.end_date.isoformat() if s.end_date else None,
            "color": s.color,
            "createdBy": s.created_by,
            "isActive": s.is_active,
            "createdAt": s.created_at.isoformat(),
            "updatedAt": s.updated_at.isoformat() if getattr(s, "updated_at", None) else None,
            "bitrixGroupId": getattr(s, "bitrix_group_id", None),
            "bitrixLastPullAt": s.bitrix_last_pull_at.isoformat() if getattr(s, "bitrix_last_pull_at", None) else None,
            "bitrixCrmEntityTypeId": getattr(s, "bitrix_crm_entity_type_id", None),
            "bitrixCrmCategoryId": getattr(s, "bitrix_crm_category_id", None),
            "bitrixCrmLastPullAt": s.bitrix_crm_last_pull_at.isoformat() if getattr(s, "bitrix_crm_last_pull_at", None) else None,
            "contractRef": getattr(s, "contract_ref", None),
            "cards": [
                {
                    "id": c.id,
                    "sprintId": c.sprint_id,
                    "buildingId": c.building_id,
                    "title": c.title,
                    "description": c.description,
                    "status": _normalize_card_status(c.status),
                    "priority": c.priority,
                    "responsible": c.responsible,
                    "dueDate": c.due_date.isoformat() if c.due_date else None,
                    "tags": c.tags,
                    "createdBy": c.created_by,
                    "order": c.order,
                    "createdAt": c.created_at.isoformat(),
                    "updatedAt": c.updated_at.isoformat() if getattr(c, "updated_at", None) else None,
                    "bitrixTaskId": getattr(c, "bitrix_task_id", None),
                    "bitrixLastPullAt": c.bitrix_last_pull_at.isoformat() if getattr(c, "bitrix_last_pull_at", None) else None,
                    "bitrixCrmEntityTypeId": getattr(c, "bitrix_crm_entity_type_id", None),
                    "bitrixCrmCategoryId": getattr(c, "bitrix_crm_category_id", None),
                    "bitrixCrmItemId": getattr(c, "bitrix_crm_item_id", None),
                    "bitrixCrmStageId": getattr(c, "bitrix_crm_stage_id", None),
                    "bitrixCrmLastPullAt": c.bitrix_crm_last_pull_at.isoformat() if getattr(c, "bitrix_crm_last_pull_at", None) else None,
                    "attachments": [
                        {
                            "id": a.id,
                            "cardId": a.card_id,
                            "filename": a.filename,
                            "fileSize": a.file_size,
                            "mimeType": a.mime_type,
                            "uploadedBy": a.uploaded_by,
                            "createdAt": a.created_at.isoformat(),
                        }
                        for a in c.attachments
                    ],
                }
                for c in s.cards
            ],
        }
        for s in sprints
    ]
    
    return {
        "buildings": {
            building_id: sprints_data
        }
    }


@router.get("/overview")
def list_sprints_overview(
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Retorna todas as sprints com resumo de prazos/atrasos para painel de alertas."""
    now = datetime.utcnow()

    sprints = db.scalars(
        select(Sprint)
        .options(selectinload(Sprint.cards))
        .order_by(Sprint.created_at.desc())
    ).all()

    building_ids = {s.building_id for s in sprints if s.building_id is not None}
    building_rows = db.scalars(select(Building).where(Building.id.in_(building_ids))).all() if building_ids else []
    building_name_by_id = {b.id: b.name for b in building_rows}

    done_statuses = {"done", "concluido", "concluído", "completed", "finalizado"}

    overview = []
    for s in sprints:
        cards_data = []
        overdue_cards = 0
        open_cards = 0

        for c in s.cards:
            status_normalized = str(c.status or "").strip().lower()
            is_done = status_normalized in done_statuses
            is_card_overdue = bool(c.due_date and c.due_date < now and not is_done)
            if not is_done:
                open_cards += 1
            if is_card_overdue:
                overdue_cards += 1

            cards_data.append(
                {
                    "id": c.id,
                    "title": c.title,
                    "status": c.status,
                    "priority": c.priority,
                    "responsible": c.responsible,
                    "dueDate": c.due_date.isoformat() if c.due_date else None,
                    "overdue": is_card_overdue,
                }
            )

        sprint_overdue = bool(s.end_date and s.end_date < now and open_cards > 0)

        overview.append(
            {
                "id": s.id,
                "buildingId": s.building_id,
                "buildingName": building_name_by_id.get(s.building_id) or f"Obra {s.building_id}",
                "name": s.name,
                "startDate": s.start_date.isoformat() if s.start_date else None,
                "endDate": s.end_date.isoformat() if s.end_date else None,
                "color": s.color,
                "isActive": s.is_active,
                "createdAt": s.created_at.isoformat(),
                "overdue": sprint_overdue,
                "stats": {
                    "totalCards": len(cards_data),
                    "openCards": open_cards,
                    "overdueCards": overdue_cards,
                },
                "cards": cards_data,
            }
        )

    return {
        "sprints": overview,
        "summary": {
            "totalSprints": len(overview),
            "overdueSprints": len([s for s in overview if s["overdue"]]),
            "overdueCards": sum(s["stats"]["overdueCards"] for s in overview),
        },
    }


@router.post("/sprint")
def create_sprint(
    payload: SprintRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> SprintResponse:
    """Create a new sprint."""
    sprint = Sprint(
        building_id=payload.building_id,
        name=payload.name,
        start_date=payload.start_date,
        end_date=payload.end_date,
        color=payload.color,
        created_by=current_user.email,
        is_active=True,
    )
    if hasattr(payload, "model_fields_set") and "bitrix_group_id" in payload.model_fields_set:
        sprint.bitrix_group_id = payload.bitrix_group_id
    if hasattr(payload, "model_fields_set") and "bitrix_crm_entity_type_id" in payload.model_fields_set:
        sprint.bitrix_crm_entity_type_id = payload.bitrix_crm_entity_type_id
    if hasattr(payload, "model_fields_set") and "bitrix_crm_category_id" in payload.model_fields_set:
        sprint.bitrix_crm_category_id = payload.bitrix_crm_category_id
    if hasattr(payload, "model_fields_set") and "contract_ref" in payload.model_fields_set:
        sprint.contract_ref = payload.contract_ref
    db.add(sprint)
    db.commit()
    db.refresh(sprint)
    return SprintResponse.model_validate(sprint)


@router.patch("/sprint/{sprint_id}")
def update_sprint(
    sprint_id: int,
    payload: SprintRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> SprintResponse:
    """Update a sprint."""
    sprint = db.scalar(select(Sprint).where(Sprint.id == sprint_id))
    if not sprint:
        raise HTTPException(status_code=404, detail="Sprint not found")
    
    sprint.name = payload.name
    sprint.start_date = payload.start_date
    sprint.end_date = payload.end_date
    sprint.color = payload.color
    if hasattr(payload, "model_fields_set") and "bitrix_group_id" in payload.model_fields_set:
        sprint.bitrix_group_id = payload.bitrix_group_id
    if hasattr(payload, "model_fields_set") and "bitrix_crm_entity_type_id" in payload.model_fields_set:
        sprint.bitrix_crm_entity_type_id = payload.bitrix_crm_entity_type_id
    if hasattr(payload, "model_fields_set") and "bitrix_crm_category_id" in payload.model_fields_set:
        sprint.bitrix_crm_category_id = payload.bitrix_crm_category_id
    if hasattr(payload, "model_fields_set") and "contract_ref" in payload.model_fields_set:
        sprint.contract_ref = payload.contract_ref
    
    db.commit()
    db.refresh(sprint)
    return SprintResponse.model_validate(sprint)


@router.delete("/sprint/{sprint_id}")
def delete_sprint(
    sprint_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Delete a sprint and all its cards."""
    sprint = db.scalar(select(Sprint).where(Sprint.id == sprint_id))
    if not sprint:
        raise HTTPException(status_code=404, detail="Sprint not found")
    
    # Delete all cards and attachments
    cards = db.scalars(select(Card).where(Card.sprint_id == sprint_id)).all()
    for card in cards:
        # Delete attachments
        attachments = db.scalars(select(Attachment).where(Attachment.card_id == card.id)).all()
        for att in attachments:
            if os.path.exists(att.file_path):
                os.remove(att.file_path)
            db.delete(att)
        db.delete(card)
    
    db.delete(sprint)
    db.commit()
    return {"status": "ok", "message": "Sprint deleted"}


@router.post("/card")
def create_card(
    payload: CardRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CardResponse:
    """Create a new card in a sprint."""
    # Verify sprint exists
    sprint = db.scalar(select(Sprint).where(Sprint.id == payload.sprint_id))
    if not sprint:
        raise HTTPException(status_code=404, detail="Sprint not found")
    
    card = Card(
        sprint_id=payload.sprint_id,
        building_id=payload.building_id,
        title=payload.title,
        description=payload.description,
        status=payload.status,
        priority=payload.priority,
        responsible=payload.responsible,
        due_date=payload.due_date,
        tags=payload.tags,
        created_by=current_user.email,
        order=0,
    )
    db.add(card)
    db.commit()
    db.refresh(card)
    return CardResponse.model_validate(card)


@router.patch("/card/{card_id}")
def update_card(
    card_id: int,
    payload: CardRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CardResponse:
    """Update a card."""
    card = db.scalar(select(Card).where(Card.id == card_id))
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    
    card.title = payload.title
    card.description = payload.description
    card.status = payload.status
    card.priority = payload.priority
    card.responsible = payload.responsible
    card.due_date = payload.due_date
    card.tags = payload.tags
    
    db.commit()
    db.refresh(card)
    return CardResponse.model_validate(card)


@router.delete("/card/{card_id}")
def delete_card(
    card_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Delete a card and all its attachments."""
    card = db.scalar(select(Card).where(Card.id == card_id))
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    
    # Delete attachments
    attachments = db.scalars(select(Attachment).where(Attachment.card_id == card_id)).all()
    for att in attachments:
        if os.path.exists(att.file_path):
            os.remove(att.file_path)
        db.delete(att)
    
    db.delete(card)
    db.commit()
    return {"status": "ok", "message": "Card deleted"}


@router.post("/upload")
def upload_attachment(
    card_id: int,
    file: UploadFile = File(...),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> AttachmentResponse:
    """Upload an attachment to a card."""
    card = db.scalar(select(Card).where(Card.id == card_id))
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    
    try:
        # Save file
        filename = f"{card_id}_{file.filename}"
        file_path = UPLOAD_DIR / filename
        
        with open(file_path, "wb") as f:
            content = file.file.read()
            f.write(content)
        
        # Create attachment record
        attachment = Attachment(
            card_id=card_id,
            filename=file.filename,
            file_path=str(file_path),
            file_size=len(content),
            mime_type=file.content_type,
            uploaded_by=current_user.email,
        )
        db.add(attachment)
        db.commit()
        db.refresh(attachment)
        return AttachmentResponse.model_validate(attachment)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.delete("/upload")
def delete_attachment(
    card_id: int,
    filename: str,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Delete an attachment from a card."""
    card = db.scalar(select(Card).where(Card.id == card_id))
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    
    attachment = db.scalar(
        select(Attachment).where(
            (Attachment.card_id == card_id) & (Attachment.filename == filename)
        )
    )
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")
    
    try:
        if os.path.exists(attachment.file_path):
            os.remove(attachment.file_path)
        db.delete(attachment)
        db.commit()
        return {"status": "ok", "message": "Attachment deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
