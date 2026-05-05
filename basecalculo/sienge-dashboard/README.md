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

Edite o arquivo `.env`:

```env
VITE_SIENGE_BASE_URL=https://SEU_CLIENTE.sienge.com.br
VITE_SIENGE_ACCESS_NAME=seu-access-name
VITE_SIENGE_TOKEN=seu-token
```

## Fórmulas usadas

```txt
Receita Operacional = soma das entradas operacionais
Margem de Contribuição = Receita Operacional - Custos Variáveis
% MC Geral = Margem de Contribuição / Receita Operacional * 100
```

## Atenção

O Sienge pode exigir proxy/backend por causa de CORS. Se o navegador bloquear a chamada, crie um backend intermediário para chamar a API com segurança.
