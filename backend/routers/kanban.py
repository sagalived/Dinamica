from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from pathlib import Path
import os

from backend.database import get_db
from backend.models import AppUser, Sprint, Card, Attachment
from backend.schemas import SprintRequest, SprintResponse, CardRequest, CardResponse, AttachmentResponse
from backend.dependencies import get_current_user

router = APIRouter(prefix="/api/kanban", tags=["kanban"])

# Path for file uploads
UPLOAD_DIR = Path("uploads/attachments")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


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
            "cards": [
                {
                    "id": c.id,
                    "sprintId": c.sprint_id,
                    "buildingId": c.building_id,
                    "title": c.title,
                    "description": c.description,
                    "status": c.status,
                    "priority": c.priority,
                    "responsible": c.responsible,
                    "dueDate": c.due_date.isoformat() if c.due_date else None,
                    "tags": c.tags,
                    "createdBy": c.created_by,
                    "order": c.order,
                    "createdAt": c.created_at.isoformat(),
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
