from __future__ import annotations

import os
import tempfile
from pathlib import Path

import requests
import flet as ft

from backend.config import API_HOST, API_PORT, BASE_DIR, FLET_HOST, FLET_PORT

API_BASE = f"http://{API_HOST}:{API_PORT}/api"
FLET_TEMP_DIR = Path(BASE_DIR) / ".tmp" / "flet"
FLET_TEMP_DIR.mkdir(parents=True, exist_ok=True)
os.environ["TEMP"] = str(FLET_TEMP_DIR)
os.environ["TMP"] = str(FLET_TEMP_DIR)
tempfile.tempdir = str(FLET_TEMP_DIR)


class ApiClient:
    def __init__(self) -> None:
        self.token: str | None = None

    def login(self, email: str, password: str) -> dict:
        response = requests.post(
            f"{API_BASE}/auth/login",
            json={"email": email, "password": password},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        self.token = payload["access_token"]
        return payload

    def get(self, path: str) -> list | dict:
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        response = requests.get(f"{API_BASE}{path}", headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()


def build_data_table(title: str, items: list[dict], columns: list[str]) -> ft.Control:
    return ft.Container(
        bgcolor="#0f172a",
        border_radius=18,
        padding=16,
        content=ft.Column(
            controls=[
                ft.Text(title, size=20, weight=ft.FontWeight.BOLD, color="#f8fafc"),
                ft.DataTable(
                    bgcolor="#111827",
                    border=ft.border.all(1, "#1f2937"),
                    heading_row_color="#172554",
                    columns=[ft.DataColumn(ft.Text(column, color="#e2e8f0")) for column in columns],
                    rows=[
                        ft.DataRow(
                            cells=[
                                ft.DataCell(
                                    ft.Text(
                                        str(item.get(column, "")),
                                        color="#e5e7eb",
                                        max_lines=2,
                                        overflow=ft.TextOverflow.ELLIPSIS,
                                    )
                                )
                                for column in columns
                            ]
                        )
                        for item in items[:20]
                    ],
                ),
            ],
            spacing=12,
        ),
    )


def main(page: ft.Page) -> None:
    page.title = "Dinamica Mobile"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 20
    page.bgcolor = "#020617"
    page.window_width = 420
    page.window_height = 900
    page.scroll = ft.ScrollMode.AUTO

    client = ApiClient()

    email_field = ft.TextField(
        label="Email",
        value="admin@dinamica.com",
        border_radius=16,
        bgcolor="#111827",
        color="#f8fafc",
    )
    password_field = ft.TextField(
        label="Senha",
        value="admin",
        password=True,
        can_reveal_password=True,
        border_radius=16,
        bgcolor="#111827",
        color="#f8fafc",
    )
    message = ft.Text(color="#fca5a5")

    dashboard = ft.Column(spacing=16, visible=False)

    def show_dashboard() -> None:
        summary = client.get("/dashboard/summary")
        companies = client.get("/companies")
        buildings = client.get("/buildings")
        creditors = client.get("/creditors")

        cards = ft.ResponsiveRow(
            controls=[
                ft.Container(
                    col={"xs": 6, "sm": 3},
                    padding=16,
                    border_radius=18,
                    bgcolor="#172554",
                    content=ft.Column(
                        [
                            ft.Text(card["label"], color="#bfdbfe"),
                            ft.Text(str(card["value"]), size=28, weight=ft.FontWeight.BOLD, color="#ffffff"),
                        ]
                    ),
                )
                for card in summary["cards"]
            ]
        )

        analytics = ft.Container(
            bgcolor="#0f172a",
            border_radius=18,
            padding=16,
            content=ft.Column(
                [
                    ft.Text("Analytics com Pandas", size=20, weight=ft.FontWeight.BOLD, color="#f8fafc"),
                    ft.Text(
                        f"Usuarios ativos no diretorio: {summary['active_directory_users']}",
                        color="#cbd5e1",
                    ),
                    ft.Text("Top cidades de clientes", color="#93c5fd"),
                    *[
                        ft.Text(f"{item['city']}: {item['total']}", color="#e2e8f0")
                        for item in summary["client_cities"][:5]
                    ],
                ],
                spacing=8,
            ),
        )

        dashboard.controls = [
            ft.Text("Dinamica Platform", size=28, weight=ft.FontWeight.BOLD, color="#f8fafc"),
            ft.Text(
                f"API em http://{API_HOST}:{API_PORT} | Flet em http://{FLET_HOST}:{FLET_PORT}",
                color="#94a3b8",
            ),
            cards,
            analytics,
            build_data_table("Empresas", companies, ["id", "name", "trade_name"]),
            build_data_table("Obras", buildings, ["id", "name", "company_name"]),
            build_data_table("Credores", creditors, ["id", "name", "city", "state"]),
        ]
        dashboard.visible = True
        login_card.visible = False
        page.update()

    def handle_login(_: ft.ControlEvent) -> None:
        message.value = ""
        try:
            client.login(email_field.value.strip(), password_field.value)
            show_dashboard()
        except Exception as exc:
            message.value = f"Falha no login: {exc}"
            page.update()

    login_card = ft.Container(
        border_radius=28,
        padding=24,
        bgcolor="#0f172a",
        content=ft.Column(
            [
                ft.Text("Dinamica Mobile", size=30, weight=ft.FontWeight.BOLD, color="#f8fafc"),
                ft.Text("Flet + FastAPI + PostgreSQL + JWT/Bcrypt", color="#93c5fd"),
                email_field,
                password_field,
                ft.ElevatedButton(
                    "Entrar",
                    bgcolor="#ea580c",
                    color="#ffffff",
                    style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=16)),
                    on_click=handle_login,
                ),
                message,
                ft.Text("Usuario padrao: admin@dinamica.com / admin", color="#94a3b8", size=12),
            ],
            spacing=16,
        ),
    )

    page.add(login_card, dashboard)


def launch_flet() -> None:
    ft.app(target=main, view=ft.AppView.WEB_BROWSER, host=FLET_HOST, port=FLET_PORT)


if __name__ == "__main__":
    launch_flet()
