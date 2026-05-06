from pathlib import Path

from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)
DATA_DIR = BASE_DIR / "data"

APP_NAME = "Dinamica Platform"
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("PORT", "8000"))
FLET_HOST = os.getenv("FLET_HOST", "127.0.0.1")
FLET_PORT = int(os.getenv("FLET_PORT", "8550"))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:admin@127.0.0.1:5432/Sienge",
)
JWT_SECRET = os.getenv("JWT_SECRET", "change-this-secret-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "480"))
SIENGE_SYNC_INTERVAL_MINUTES = int(os.getenv("SIENGE_SYNC_INTERVAL_MINUTES", "360"))
