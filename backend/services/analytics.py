import pandas as pd
from sqlalchemy.orm import Session

from backend.models import Building, Client, Company, Creditor, DirectoryUser


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows if rows else [])


def build_dashboard_summary(db: Session) -> dict:
    buildings_df = _frame(
        [
            {"company_name": row.company_name or "Sem empresa"}
            for row in db.query(Building).all()
        ]
    )
    creditors_df = _frame(
        [
            {"state": row.state or "N/D"}
            for row in db.query(Creditor).all()
        ]
    )
    clients_df = _frame(
        [
            {"city": row.city or "N/D"}
            for row in db.query(Client).all()
        ]
    )

    cards = [
        {"label": "Empresas", "value": db.query(Company).count()},
        {"label": "Obras", "value": db.query(Building).count()},
        {"label": "Credores", "value": db.query(Creditor).count()},
        {"label": "Clientes", "value": db.query(Client).count()},
    ]

    companies_by_buildings = (
        buildings_df.value_counts(["company_name"]).reset_index(name="total").to_dict("records")
        if not buildings_df.empty
        else []
    )
    creditor_states = (
        creditors_df.value_counts(["state"]).reset_index(name="total").to_dict("records")
        if not creditors_df.empty
        else []
    )
    client_cities = (
        clients_df.value_counts(["city"]).head(10).reset_index(name="total").to_dict("records")
        if not clients_df.empty
        else []
    )

    active_directory_users = db.query(DirectoryUser).filter(DirectoryUser.active.is_(True)).count()

    return {
        "cards": cards,
        "companies_by_buildings": companies_by_buildings,
        "creditor_states": creditor_states,
        "client_cities": client_cities,
        "active_directory_users": active_directory_users,
    }
