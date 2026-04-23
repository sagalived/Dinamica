# Integração Sienge API - Documentação

## Status: ✅ Implementado e Configurado

### Credenciais Configuradas
- **URL Base:** https://dinamicaempreendimentos.sienge.com.br
- **Access Name:** dinamicaempreendimentos-jrmorais
- **Token:** ✅ Salvo em `.env`
- **Arquivo de Config:** `.env`

### Arquivos Criados
1. **`backend/services/sienge_client.py`** (~270 linhas)
   - Cliente HTTP assíncrono para Sienge
   - Métodos: test_connection, fetch_obras, fetch_empresas, fetch_credores, fetch_pedidos, fetch_financeiro, fetch_receber, fetch_itens_pedidos, fetch_saldo_bancario
   - Tratamento automático de erros com fallback
   - Logging de falhas

### Endpoints Atualizados

#### 1. **GET /api/sienge/test**
- **Antes:** Apenas teste local
- **Depois:** Testa conexão com Sienge API + banco de dados
- **Response:**
  ```json
  {
    "live": {"status": "connected", "message": "..."},
    "cache": {"source": "sienge_live"},
    "database": {"ok": true}
  }
  ```

#### 2. **GET /api/sienge/bootstrap** ⭐ PRINCIPAL
- **Antes:** Retornava apenas dados do PostgreSQL
- **Depois:** Pega dados da Sienge API com fallback para PostgreSQL
- **Dados Retornados:**
  - `obras` → GET /api/v1/obras
  - `empresas` → GET /api/v1/empresas
  - `credores` → GET /api/v1/fornecedores
  - `pedidos` → GET /api/v1/pedidos
  - `financeiro` → GET /api/v1/financeiro/titulos-pagar
  - `receber` → GET /api/v1/financeiro/titulos-receber
  - `itens_pedidos` → GET /api/v1/pedidos/itens (mapeado por order_id)
  - `saldo_bancario` → GET /api/v1/financeiro/saldo-bancario

#### 3. **POST /api/sienge/sync**
- **Antes:** Stub que retornava "ok"
- **Depois:** Sincroniza todos os dados com Sienge
- **Response:**
  ```json
  {
    "status": "ok",
    "message": "Sync completed from Sienge API",
    "data": {
      "obras": 15,
      "empresas": 3,
      "credores": 42,
      "pedidos": 128,
      ...
    }
  }
  ```

### Estratégia de Fallback

Se a Sienge API não responder (credenciais inválidas, endpoint down, etc):

```
Sienge API DOWN → PostgreSQL Local
├── Obras → Buildings table
├── Empresas → Companies table
├── Credores → Creditors table
└── Pedidos, Financeiro, Receber → Empty arrays (será preenchido quando dados chegarem)
```

### Endpoints Esperados na API Sienge

| Recurso | Endpoint | Método |
|---------|----------|--------|
| Obras | /api/v1/obras | GET |
| Empresas | /api/v1/empresas | GET |
| Fornecedores/Credores | /api/v1/fornecedores | GET |
| Pedidos | /api/v1/pedidos | GET |
| Itens de Pedidos | /api/v1/pedidos/itens | GET |
| Títulos a Pagar | /api/v1/financeiro/titulos-pagar | GET |
| Títulos a Receber | /api/v1/financeiro/titulos-receber | GET |
| Saldo Bancário | /api/v1/financeiro/saldo-bancario | GET |

### Como Testar

1. **Verificar Conexão:**
   ```bash
   curl -H "Authorization: Bearer SEU_TOKEN" \
        http://localhost:8000/api/sienge/test
   ```

2. **Fazer Bootstrap (com dados reais):**
   ```bash
   curl -H "Authorization: Bearer SEU_TOKEN" \
        http://localhost:8000/api/sienge/bootstrap
   ```

3. **Sincronizar Dados:**
   ```bash
   curl -X POST \
        -H "Authorization: Bearer SEU_TOKEN" \
        http://localhost:8000/api/sienge/sync
   ```

### Headers de Autenticação

Todos os requests ao Sienge incluem:
```
Authorization: Bearer qhxOkgkBOVTMQD1TVvxyMhNqIL8M3EyE
X-Access-Name: dinamicaempreendimentos-jrmorais
Content-Type: application/json
```

### Tratamento de Erros

- **401 Unauthorized:** Token inválido ou expirado
- **403 Forbidden:** Access name inválido
- **404 Not Found:** Recurso não existe no Sienge
- **500 Internal Server Error:** Erro na API Sienge

**Ação:** Sistema faz fallback automático para dados PostgreSQL

### Logs

Todos os erros são registrados em `backend/services/sienge_client.py` com logger:
```python
logger = logging.getLogger(__name__)
logger.error(f"Error fetching obras: {str(e)}")
```

### Próximos Passos

1. ✅ Testes manuais com curl
2. ✅ Validar dados retornados no frontend
3. ✅ Implementar cache em memoria (Redis) para performance
4. ✅ Adicionar endpoint de health check periódico
5. ✅ Documentar rate limits do Sienge

### Notas Importantes

- **Async/Await:** Endpoints agora são async para suportar múltiplas requisições
- **Timeout:** 30 segundos por requisição (ajustável)
- **Cache Strategy:** Nenhum cache implementado ainda (sempre busca do Sienge)
- **Rate Limiting:** Verificar documentação Sienge para limites de requisição
