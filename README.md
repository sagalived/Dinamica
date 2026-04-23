# Dinamica

Projeto migrado para a stack:

- `FastAPI` para a API
- `Pandas` para analytics e resumos
- `Flet` para interface mobile/web
- `JWT + Bcrypt` para autenticacao
- `PostgreSQL` para persistencia

Os JSONs existentes em `data/` sao usados como carga inicial do banco na primeira subida.

## Rodar localmente

Pre-requisitos:

- Python 3.11+
- PostgreSQL ativo em `localhost`

Passos:

1. Instale as dependencias:
   `pip install -r requirements.txt`
2. Crie um `.env` a partir de `.env.example`
3. Ajuste `DATABASE_URL` para sua instancia Postgres local
4. Inicie o projeto:
   `python app.py`

Endpoints locais:

- API FastAPI: `http://127.0.0.1:8000`
- App Flet: `http://127.0.0.1:8550`

Credencial inicial:

- Email: `admin@dinamica.com`
- Senha: `admin`

## Principais arquivos

- [app.py](/c:/Users/dinam/OneDrive/Documentos/GitHub/Dinamica/app.py)
- [backend/main.py](/c:/Users/dinam/OneDrive/Documentos/GitHub/Dinamica/backend/main.py)
- [backend/services/bootstrap.py](/c:/Users/dinam/OneDrive/Documentos/GitHub/Dinamica/backend/services/bootstrap.py)
- [backend/services/analytics.py](/c:/Users/dinam/OneDrive/Documentos/GitHub/Dinamica/backend/services/analytics.py)
- [flet_app.py](/c:/Users/dinam/OneDrive/Documentos/GitHub/Dinamica/flet_app.py)
