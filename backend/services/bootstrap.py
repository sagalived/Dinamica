import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.config import DATA_DIR
from backend.models import AppUser, Building, Client, Company, Creditor, DirectoryUser
from backend.security import hash_password


def _read_json(filename: str) -> list[dict]:
    file_path = DATA_DIR / filename
    if not file_path.exists():
        return []
    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    return data if isinstance(data, list) else []


def ensure_seed_data(db: Session) -> None:
    if db.scalar(select(AppUser).limit(1)) is None:
        db.add(
            AppUser(
                email="admin@dinamica.com",
                full_name="Administrador Dinamica",
                department="Tecnologia",
                role="admin",
                password_hash=hash_password("admin"),
                is_active=True,
            )
        )

    if db.scalar(select(DirectoryUser).limit(1)) is None:
        for item in _read_json("usuarios.json"):
            db.merge(
                DirectoryUser(
                    id=str(item.get("id") or ""),
                    name=item.get("name") or "Sem nome",
                    email=item.get("email"),
                    active=bool(item.get("active", True)),
                )
            )

    if db.scalar(select(Company).limit(1)) is None:
        for item in _read_json("empresas.json"):
            db.merge(
                Company(
                    id=int(item.get("id")),
                    name=item.get("name") or "Sem nome",
                    trade_name=item.get("tradeName"),
                    cnpj=item.get("cnpj"),
                )
            )

    if db.scalar(select(Building).limit(1)) is None:
        for item in _read_json("obras.json"):
            db.merge(
                Building(
                    id=int(item.get("id")),
                    name=item.get("name") or "Sem nome",
                    company_id=item.get("companyId"),
                    company_name=item.get("companyName"),
                    cnpj=item.get("cnpj"),
                    address=item.get("adress"),
                    created_by=item.get("createdBy"),
                    modified_by=item.get("modifiedBy"),
                    building_type=item.get("buildingTypeDescription"),
                )
            )

    if db.scalar(select(Creditor).limit(1)) is None:
        for item in _read_json("credores.json"):
            address = item.get("address") or {}
            db.merge(
                Creditor(
                    id=int(item.get("id")),
                    name=item.get("name") or "Sem nome",
                    trade_name=item.get("tradeName"),
                    cnpj=item.get("cnpj"),
                    city=address.get("cityName"),
                    state=address.get("state"),
                    active=bool(item.get("active", True)),
                )
            )

    if db.scalar(select(Client).limit(1)) is None:
        for item in _read_json("clientes.json"):
            db.merge(
                Client(
                    id=int(item.get("codigoCliente")),
                    name=item.get("nomeCliente") or "Sem nome",
                    fantasy_name=item.get("nomeFantasia"),
                    cnpj_cpf=item.get("cnpjCpf"),
                    city=item.get("enderecoMunicipio"),
                    state=item.get("enderecoUf"),
                    email=item.get("email"),
                    phone=item.get("telefonePrincipal"),
                    status=item.get("situacaoCliente"),
                )
            )

    db.commit()
