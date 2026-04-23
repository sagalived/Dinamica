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

## Deploy no Render

O arquivo `render.yaml` ja esta preparado para publicar como Web Service.

Variaveis obrigatorias para integrar com o Sienge:
- `SIENGE_USERNAME`
- `SIENGE_PASSWORD`
- `SIENGE_INSTANCE`

Variaveis opcionais:
- `GOOGLE_MAPS_API_KEY`
- `AUTO_SYNC_ON_BOOT` (padrao configurado: `true`)
- `CORS_ALLOW_ORIGINS` (padrao configurado: `*`)

Observacoes:
- Nao precisa cadastrar `PORT` manualmente no Render; ele injeta essa variavel automaticamente.
- O app usa `python app.py` e le a porta pelo ambiente, entao sobe normalmente no Render.
- O frontend e compilado no deploy com `npm ci && npm run build`.
