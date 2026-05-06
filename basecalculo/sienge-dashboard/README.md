# Dashboard Sienge - Receita e Margem

## Como rodar

```bash
npm install
cp .env.example .env
npm run dev
```

Abra o endereço exibido no terminal, normalmente:

```txt
http://localhost:5173
```

## Configure as credenciais

Este frontend consome dados via endpoints do backend em `/api` (ex.: `/api/sienge/bootstrap`).
As credenciais do Sienge devem ficar no backend (no `.env`), não no bundle do navegador.

Edite o arquivo `.env` (credenciais de teste/local):

```env
SIENGE_INSTANCE=SEU_CLIENTE
SIENGE_BASE_URL=https://api.sienge.com.br/SEU_CLIENTE
SIENGE_ACCESS_NAME=seu-access-name
SIENGE_TOKEN=seu-token
```

## Fórmulas usadas

```txt
Receita Operacional = soma das entradas operacionais
Margem de Contribuição = Receita Operacional - Custos Variáveis
% MC Geral = Margem de Contribuição / Receita Operacional * 100
```

## Atenção

O Sienge pode exigir proxy/backend por causa de CORS. Se o navegador bloquear a chamada, crie um backend intermediário para chamar a API com segurança.
