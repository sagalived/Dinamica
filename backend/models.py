from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, ForeignKey, Float
from sqlalchemy.orm import relationship

from backend.database import Base


class TimestampMixin:
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class AppUser(TimestampMixin, Base):
    __tablename__ = "app_users"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    full_name = Column(String(255), nullable=False)
    department = Column(String(120), nullable=True)
    role = Column(String(50), default="admin", nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)


class DirectoryUser(TimestampMixin, Base):
    __tablename__ = "directory_users"

    id = Column(String(80), primary_key=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    active = Column(Boolean, default=True, nullable=False)


class Company(TimestampMixin, Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), index=True, nullable=False)
    trade_name = Column(String(255), nullable=True)
    cnpj = Column(String(40), nullable=True)


class Building(TimestampMixin, Base):
    __tablename__ = "buildings"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), index=True, nullable=False)
    company_id = Column(Integer, nullable=True, index=True)
    company_name = Column(String(255), nullable=True)
    cnpj = Column(String(40), nullable=True)
    address = Column(Text, nullable=True)
    created_by = Column(String(120), nullable=True)
    modified_by = Column(String(120), nullable=True)
    building_type = Column(String(120), nullable=True)


class Creditor(TimestampMixin, Base):
    __tablename__ = "creditors"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), index=True, nullable=False)
    trade_name = Column(String(255), nullable=True)
    cnpj = Column(String(40), nullable=True)
    city = Column(String(120), nullable=True)
    state = Column(String(5), nullable=True)
    active = Column(Boolean, default=True, nullable=False)


class Client(TimestampMixin, Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), index=True, nullable=False)
    fantasy_name = Column(String(255), nullable=True)
    cnpj_cpf = Column(String(40), nullable=True)
    city = Column(String(120), nullable=True)
    state = Column(String(5), nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(40), nullable=True)
    status = Column(String(50), nullable=True)


class Sprint(TimestampMixin, Base):
    __tablename__ = "sprints"

    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, nullable=False, index=True)
    name = Column(String(255), nullable=False)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    color = Column(String(20), default="blue", nullable=False)
    created_by = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)


class Card(TimestampMixin, Base):
    __tablename__ = "cards"

    id = Column(Integer, primary_key=True)
    sprint_id = Column(Integer, ForeignKey("sprints.id"), nullable=False, index=True)
    building_id = Column(Integer, nullable=False, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String(50), default="todo", nullable=False)
    priority = Column(String(20), default="medium", nullable=False)
    responsible = Column(String(255), nullable=True)
    due_date = Column(DateTime, nullable=True)
    tags = Column(String(500), nullable=True)
    created_by = Column(String(255), nullable=False)
    order = Column(Integer, default=0, nullable=False)
    
    sprint = relationship("Sprint", backref="cards")


class Attachment(TimestampMixin, Base):
    __tablename__ = "attachments"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("cards.id"), nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer, nullable=True)
    mime_type = Column(String(100), nullable=True)
    uploaded_by = Column(String(255), nullable=False)
    
    card = relationship("Card", backref="attachments")


class LogisticsLocation(TimestampMixin, Base):
    __tablename__ = "logistics_locations"

    id = Column(Integer, primary_key=True)
    code = Column(String(100), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    address = Column(Text, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    location_type = Column(String(50), nullable=True)
    source = Column(String(100), nullable=True)
    created_by = Column(String(255), nullable=True)
