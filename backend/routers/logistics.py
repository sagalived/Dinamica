from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import select
import math

from backend.database import get_db
from backend.models import AppUser, LogisticsLocation
from backend.schemas import LogisticsLocationRequest, LogisticsLocationResponse, RouteDistanceRequest, RouteDistanceResponse
from backend.dependencies import get_current_user

router = APIRouter(prefix="/api/sienge/logistics", tags=["logistics"])


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance in kilometers between two lat/lon points using Haversine formula.
    """
    R = 6371  # Earth radius in km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


@router.get("/locations")
def list_locations(
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """
    Get all logistics locations.
    Returns: { results: [locations] }
    """
    locations = db.scalars(select(LogisticsLocation)).all()
    
    locations_data = [
        LogisticsLocationResponse.model_validate(loc).model_dump()
        for loc in locations
    ]
    
    return {
        "results": locations_data
    }


@router.post("/locations")
def create_location(
    payload: LogisticsLocationRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Create a new logistics location."""
    # Check if code already exists
    existing = db.scalar(
        select(LogisticsLocation).where(LogisticsLocation.code == payload.code)
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail="Location code already exists"
        )
    
    location = LogisticsLocation(
        code=payload.code,
        name=payload.name,
        address=payload.address,
        latitude=payload.latitude,
        longitude=payload.longitude,
        location_type=payload.location_type,
        source=payload.source,
        created_by=current_user.email,
    )
    db.add(location)
    db.commit()
    db.refresh(location)
    
    return {
        "location": LogisticsLocationResponse.model_validate(location).model_dump()
    }


@router.post("/route-distance")
def calculate_route_distance(
    payload: RouteDistanceRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """
    Calculate distance between two locations.
    Accepts origin and destination as dicts with address and optional lat/lng.
    If lat/lng not provided, uses simple approximation.
    """
    try:
        origin = payload.origin
        destination = payload.destination
        
        # Extract coordinates
        origin_lat = origin.get("lat")
        origin_lon = origin.get("lng")
        dest_lat = destination.get("lat")
        dest_lon = destination.get("lng")
        
        # If we have both coordinates, calculate using Haversine
        if all([origin_lat, origin_lon, dest_lat, dest_lon]):
            distance_km = haversine_distance(origin_lat, origin_lon, dest_lat, dest_lon)
            return {
                "distanceKm": round(distance_km, 2),
                "provider": "haversine",
                "origin": origin.get("address", "Unknown"),
                "destination": destination.get("address", "Unknown"),
            }
        
        # Fallback: return error or 0
        return {
            "distanceKm": 0,
            "provider": "error",
            "error": "Coordinates not provided for both locations",
            "origin": origin.get("address", "Unknown"),
            "destination": destination.get("address", "Unknown"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Route calculation failed: {str(e)}")
