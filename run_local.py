import threading
import time

import uvicorn

from backend.config import API_HOST, API_PORT
from flet_app import launch_flet


def _run_api() -> None:
    uvicorn.run("backend.main:app", host=API_HOST, port=API_PORT, reload=False)


def run_local() -> None:
    api_thread = threading.Thread(target=_run_api, daemon=True)
    api_thread.start()
    time.sleep(2)
    launch_flet()
