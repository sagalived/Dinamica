Este projeto roda 100% em Python.

O frontend ja esta compilado na pasta `dist` e e servido diretamente pelo FastAPI.

## Rodar localmente

Pre-requisitos:
- Python 3.11+

1. Instale dependencias do backend:
   `pip install -r requirements.txt`
2. Configure variaveis de ambiente com base no arquivo `.env.example`.
3. Inicie a aplicacao:
   `python app.py`

Aplicacao (frontend + backend): `http://localhost:8000`

## Build de producao

1. Instale dependencias Python:
   `pip install -r requirements.txt`
2. Suba o backend Python:
   `python app.py`
