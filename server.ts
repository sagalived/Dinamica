import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import axios from "axios";
import dotenv from "dotenv";
import fs from "fs/promises";
import { existsSync, mkdirSync } from "fs";

dotenv.config();

async function startServer() {
  const app = express();
  const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

  app.use(express.json());

  // =========================================================
  // CONFIGURAÇÃO SIENGE
  // =========================================================
  const SIENGE_USERNAME = process.env.SIENGE_USERNAME || "dinamicaempreendimentos-jrmorais";
  const SIENGE_PASSWORD = process.env.SIENGE_PASSWORD || "5jT2uxIW6YYAPL2epk9QUUvCEGM2eX9z";
  const SIENGE_INSTANCE = (process.env.SIENGE_INSTANCE || "dinamicaempreendimentos").split(".")[0];

  if (!SIENGE_USERNAME || !SIENGE_PASSWORD || !SIENGE_INSTANCE) {
    console.error("❌ Variáveis SIENGE_USERNAME / SIENGE_PASSWORD / SIENGE_INSTANCE não configuradas.");
  }

  const SIENGE_BASE_URL = `https://api.sienge.com.br/${SIENGE_INSTANCE}/public/api/v1`;
  const siengeAuth = Buffer.from(`${SIENGE_USERNAME}:${SIENGE_PASSWORD}`).toString("base64");

  const siengeAPI = axios.create({
    baseURL: SIENGE_BASE_URL,
    headers: {
      Authorization: `Basic ${siengeAuth}`,
      "Content-Type": "application/json",
      Accept: "application/json",
      "User-Agent": "DinamicaDashboard/1.0"
    },
    timeout: 60000
  });

  // =========================================================
  // PERSISTÊNCIA / CACHE
  // =========================================================
  const DATA_DIR = path.join(process.cwd(), "data");
  if (!existsSync(DATA_DIR)) {
    mkdirSync(DATA_DIR);
  }

  async function saveToFile(filename: string, data: any) {
    try {
      const filePath = path.join(DATA_DIR, filename);

      if (filename.endsWith(".csv")) {
        const content = typeof data === "string" ? data : String(data ?? "");
        await fs.writeFile(filePath, content, "utf-8");
        return;
      }

      await fs.writeFile(filePath, JSON.stringify(data, null, 2), "utf-8");
    } catch (e) {
      console.error(`[Cache] Erro ao salvar ${filename}:`, e);
    }
  }

  async function readFromFile(filename: string) {
    const filePath = path.join(DATA_DIR, filename);
    if (!existsSync(filePath)) return null;

    try {
      const content = await fs.readFile(filePath, "utf-8");
      if (filename.endsWith(".csv")) return content;
      return JSON.parse(content);
    } catch (e) {
      console.error(`[Cache] Erro ao ler ${filename}:`, e);
      return null;
    }
  }

  // =========================================================
  // HELPERS
  // =========================================================
  function csvEscape(value: any): string {
    if (value === null || value === undefined) return "";
    const str = String(value).replace(/"/g, '""');
    return `"${str}"`;
  }

  function normalizeArrayPayload(data: any): any[] {
    if (!data) return [];
    if (Array.isArray(data)) return data;
    if (Array.isArray(data.results)) return data.results;
    return [];
  }

  const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

  async function safeGet(endpoint: string, params?: Record<string, any>, retries = 3): Promise<any> {
    try {
      const res = await siengeAPI.get(endpoint, { params });
      return res;
    } catch (e: any) {
      const status = e?.response?.status;
      if (status === 429 && retries > 0) {
        await sleep(3000);
        return safeGet(endpoint, params, retries - 1);
      }
      const body = e?.response?.data || e?.message;
      console.warn(`⚠️ [Sienge] Falha em ${endpoint} (${status || "sem status"})`, JSON.stringify(body));
      return null;
    }
  }

  async function safeFetchAll(endpoint: string, params?: Record<string, any>) {
    try {
      const allResults: any[] = [];
      let offset = 0;
      const limit = 200;

      while (true) {
        let response: any = null;
        let retries = 5;
        
        while (retries > 0) {
          try {
            response = await siengeAPI.get(endpoint, {
              params: { ...params, limit, offset }
            });
            break;
          } catch (e: any) {
            if (e?.response?.status === 429) {
              retries--;
              await sleep(3000); // Aguarda antes de tentar de novo
            } else {
              throw e; // Lança o erro se não for 429
            }
          }
        }

        if (!response) break; // Passou dos retries todos
        
        const results = normalizeArrayPayload(response.data);
        if (!results.length) break;

        allResults.push(...results);
        offset += results.length;

        const returnedLimit = response.data?.metadata?.limit || limit;
        if (results.length < returnedLimit) break;
      }

      return { data: { results: allResults } };
    } catch (e: any) {
      const status = e?.response?.status;
      const body = e?.response?.data || e?.message;
      console.warn(`⚠️ [Sienge] Falha em ${endpoint} (${status || "sem status"})`, JSON.stringify(body));
      return null;
    }
  }

  // =========================================================
  // SINCRONIZAÇÃO
  // =========================================================
  let isSyncing = false;

  async function syncAllData() {
    if (isSyncing) {
      console.log("⏳ [Sync] Já existe uma sincronização em andamento.");
      return false;
    }

    isSyncing = true;
    console.log("🔄 [Sync] Iniciando sincronização completa com Sienge...");

    try {
      const startDateStr = "2019-01-01";
      const endDateStr = "2030-12-31";

      // -----------------------------------------------------
      // 1) DADOS PRINCIPAIS
      // -----------------------------------------------------
      const [
        enterprisesRes,
        purchaseOrdersRes,
        purchaseOrdersRestRes,
        billsRes,
        accountsStatementsRes,
        companiesRes,
        unitsRes,
        usersRes,
        creditorsRes
      ] = await Promise.all([
        safeGet("/enterprises"),
        safeFetchAll("/purchase-orders"),
        safeGet("/purchase-orders", { limit: 200, offset: 0 }),
        safeFetchAll("/bills"),
        safeFetchAll("/accounts-statements"),
        safeGet("/companies"),
        safeGet("/units-of-measure"),
        safeGet("/users"),
        safeFetchAll("/creditors")
      ]);

      let obras = enterprisesRes ? normalizeArrayPayload(enterprisesRes.data) : [];
      let pedidos = purchaseOrdersRes ? normalizeArrayPayload(purchaseOrdersRes.data) : [];
      let pedidosPagina = purchaseOrdersRestRes ? normalizeArrayPayload(purchaseOrdersRestRes.data) : [];
      let financeiro = billsRes ? normalizeArrayPayload(billsRes.data) : [];
      let receber = accountsStatementsRes ? normalizeArrayPayload(accountsStatementsRes.data) : [];
      let empresas = companiesRes ? normalizeArrayPayload(companiesRes.data) : [];
      let unidades = unitsRes ? normalizeArrayPayload(unitsRes.data) : [];
      let usuarios = usersRes ? normalizeArrayPayload(usersRes.data) : [];

      // -----------------------------------------------------
      // 2) FALLBACKS QUANDO ENDPOINT NÃO VIER LIBERADO
      // -----------------------------------------------------
      if (!Array.isArray(pedidos) || pedidos.length === 0) {
        pedidos = pedidosPagina;
      }

      if (!Array.isArray(obras) || obras.length === 0) {
        const map = new Map<string, any>();
        for (const p of pedidos) {
          const id = p.enterpriseId || p.idEnterprise || p.idObra || p.codigoVisivelObra;
          const nome = p.enterpriseName || p.nomeObra || p.obra || (id ? `Obra ${id}` : null);
          if (id) {
            map.set(String(id), {
              id,
              name: nome || `Obra ${id}`,
              nome: nome || `Obra ${id}`
            });
          }
        }
        obras = Array.from(map.values());
      }

      if (!Array.isArray(usuarios) || usuarios.length === 0) {
        const map = new Map<string, any>();
        for (const p of pedidos) {
          const id = p.buyerId || p.idComprador || p.codigoComprador || p.createdBy;
          const nome = p.buyerName || p.nomeComprador || p.comprador || p.createdBy;
          if (id) {
            map.set(String(id), {
              id: String(id),
              name: nome || String(id),
              nome: nome || String(id)
            });
          }
        }
        usuarios = Array.from(map.values());
      }

      let credoresRaw = creditorsRes ? normalizeArrayPayload(creditorsRes.data) : [];
      let credoresMapData = [];

      if (Array.isArray(credoresRaw) && credoresRaw.length > 0) {
        credoresMapData = credoresRaw.map(c => ({
          id: c.id,
          name: c.name || c.nomeFantasia || c.nome || `Credor ${c.id}`,
          nome: c.name || c.nomeFantasia || c.nome || `Credor ${c.id}`
        }));
      } else {
        const credoresMap = new Map<string, any>();
        for (const p of pedidos) {
          const id = p.supplierId || p.creditorId || p.idCredor || p.codigoFornecedor;
          const nome = p.supplierName || p.nomeFornecedor || p.nomeCredor || p.creditorName;
          if (id) {
            credoresMap.set(String(id), {
              id,
              name: nome || `Credor ${id}`,
              nome: nome || `Credor ${id}`
            });
          }
        }
        credoresMapData = Array.from(credoresMap.values());
      }
      const credoresById = new Map(credoresMapData.map(c => [String(c.id), c]));

      // -----------------------------------------------------
      // 3) ITENS DOS PEDIDOS
      // -----------------------------------------------------
      const itemsMap: Record<string, any[]> = (await readFromFile("itens_pedidos.json")) || {};
      const topOrders = Array.isArray(pedidos) ? pedidos.slice(0, 80) : [];

      for (const order of topOrders) {
        const id = order.id || order.purchaseOrderId || order.number || order.numero;
        if (!id) continue;

        if (!itemsMap[id]) {
          try {
            const itemsRes = await siengeAPI.get(`/purchase-orders/${id}/items`);
            itemsMap[id] = normalizeArrayPayload(itemsRes.data);
          } catch (e: any) {
            itemsMap[id] = [];
          }
        }
      }

      // -----------------------------------------------------
      // 4) ENRIQUECIMENTO / NORMALIZAÇÕES
      // -----------------------------------------------------
      const obrasById = new Map<string, any>();
      for (const o of obras) {
        const oid = o.id || o.enterpriseId || o.code || o.codigoVisivel;
        if (oid) obrasById.set(String(oid), o);
      }

      const empresasById = new Map<string, any>();
      for (const e of empresas) {
        const eid = e.id || e.companyId;
        if (eid) empresasById.set(String(eid), e);
      }

      const usuariosById = new Map<string, any>();
      for (const u of usuarios) {
        const uid = u.id || u.userId || u.code;
        if (uid) usuariosById.set(String(uid), u);
      }

      // Resolver solicitante/comprador/fornecedor
      for (const p of pedidos) {
        const buyerId = p.buyerId || p.idComprador || p.codigoComprador || p.createdBy;
        const buyerName =
          p.buyerName ||
          p.nomeComprador ||
          usuariosById.get(String(buyerId))?.name ||
          usuariosById.get(String(buyerId))?.nome ||
          p.createdBy ||
          "";

        const supplierId = p.supplierId || p.creditorId || p.idCredor || p.codigoFornecedor;
        const supplierName =
          p.supplierName ||
          p.nomeFornecedor ||
          credoresById.get(String(supplierId))?.name ||
          credoresById.get(String(supplierId))?.nome ||
          "";

        p.comprador = buyerName;
        p.nomeComprador = buyerName;
        p.solicitante = p.requesterUser || p.createdBy || buyerName;
        p.requesterId = p.requesterId || p.createdBy || buyerId || "";
        p.nomeFornecedor = supplierName;
      }

      // -----------------------------------------------------
      // 4b) RESOLVE OBRA NAMES BY VISIBLE CODE
      // The Sienge /enterprises list uses internal IDs (1,2,3...)
      // but purchase-orders reference obras by visible code (codigoVisivel).
      // We fetch /enterprises/{visibleCode} per unique buildingId.
      // -----------------------------------------------------
      const obrasByCode: Record<string, { id: number; name: string; address: string }> = {};

      // Try to load existing cache first to avoid re-fetching all
      const existingObrasByCode = (await readFromFile("obras_by_code.json")) || {};
      Object.assign(obrasByCode, existingObrasByCode);

      const uniqueBuildingIds = [...new Set(
        pedidos.map((p: any) => p.buildingId).filter(Boolean).map(String)
      )];

      for (const bid of uniqueBuildingIds) {
        if (obrasByCode[bid]) continue; // already cached
        try {
          const res = await safeGet(`/enterprises/${bid}`);
          if (res?.data) {
            const d = res.data;
            obrasByCode[bid] = {
              id: d.id || Number(bid),
              name: d.name || d.nome || d.tradeName || d.description || `Obra ${bid}`,
              address: d.adress || d.address || d.endereco || ""
            };
          }
        } catch {
          // leave undefined — will fall back to code
        }
        await sleep(120); // light rate limiting
      }

      // Inject buildingName into each pedido
      for (const p of pedidos) {
        const bid = String(p.buildingId || '');
        if (bid && obrasByCode[bid]) {
          p.buildingName = obrasByCode[bid].name;
        }
      }

      // -----------------------------------------------------
      // 5) SALVAR CACHE
      // -----------------------------------------------------
      await saveToFile("obras.json", obras);
      await saveToFile("obras_by_code.json", obrasByCode);
      await saveToFile("credores.json", credoresMapData);
      await saveToFile("usuarios.json", usuarios);
      await saveToFile("empresas.json", empresas);
      await saveToFile("units-of-measure.json", unidades);
      await saveToFile("pedidos.json", { results: pedidos });
      await saveToFile("financeiro.json", { results: financeiro });
      await saveToFile("receber.json", { results: receber });
      await saveToFile("itens_pedidos.json", itemsMap);

      // -----------------------------------------------------
      // 6) CSV CONSOLIDADO
      // -----------------------------------------------------
      const headers = [
        "Tipo",
        "ID",
        "Obra",
        "Empresa",
        "Fornecedor/Cliente/Descricao",
        "Comprador",
        "Solicitante",
        "Data",
        "Valor",
        "Status",
        "Condicao/Prazo",
        "Item/Insumo",
        "Qtd",
        "Un",
        "Vlr Unit"
      ];

      const csvRows: string[] = [];
      csvRows.push(headers.join(","));

      for (const o of pedidos) {
        const id = o.id || o.purchaseOrderId || o.number || o.numero || "";
        const obraId = o.enterpriseId || o.idEnterprise || o.idObra || o.codigoVisivelObra || "";
        const obraObj = obrasById.get(String(obraId));
        const obraNome =
          o.enterpriseName ||
          o.nomeObra ||
          obraObj?.name ||
          obraObj?.nome ||
          (obraId ? `Obra ${obraId}` : "Não Informado");

        const companyId = obraObj?.companyId || obraObj?.idCompany || o.companyId || "";
        const empresaNome =
          empresasById.get(String(companyId))?.name ||
          empresasById.get(String(companyId))?.nome ||
          "Dinamica";

        const supplierId = o.supplierId || o.creditorId || o.idCredor || o.codigoFornecedor || "";
        const supplierNome =
          o.supplierName ||
          o.nomeFornecedor ||
          credoresById.get(String(supplierId))?.name ||
          credoresById.get(String(supplierId))?.nome ||
          "Não Informado";

        const buyerId = o.buyerId || o.idComprador || o.codigoComprador || o.createdBy || "";
        const buyerNome =
          o.nomeComprador ||
          o.comprador ||
          usuariosById.get(String(buyerId))?.name ||
          usuariosById.get(String(buyerId))?.nome ||
          String(buyerId || "Não Informado");

        const solicitante = o.solicitante || o.requesterUser || o.createdBy || buyerNome || "Não Informado";
        const data = o.issueDate || o.dataEmissao || o.data || "";
        const valor = o.totalAmount || o.valorTotal || o.amount || 0;
        const status = o.status || o.situacao || "N/A";
        const condicao = o.paymentConditionDescription || o.condicaoPagamentoDescricao || "N/A";
        const prazo = o.deliveryDate || o.dataEntrega || o.prazoEntrega || "";

        csvRows.push([
          csvEscape("Pedido"),
          csvEscape(id),
          csvEscape(obraNome),
          csvEscape(empresaNome),
          csvEscape(supplierNome),
          csvEscape(buyerNome),
          csvEscape(solicitante),
          csvEscape(data),
          csvEscape(valor),
          csvEscape(status),
          csvEscape(`${condicao}${prazo ? ` / Prazo: ${prazo}` : ""}`),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---")
        ].join(","));

        const items = itemsMap[String(id)] || [];
        for (const item of items) {
          const desc = item.description || item.descricao || item.itemName || item.itemNome || "Item";
          const qtd = item.quantity || item.quantidade || 0;
          const un = item.unitOfMeasure || item.unidadeMedidaSigla || item.unit || "UN";
          const vlrU = item.unitPrice || item.valorUnitario || 0;
          const vlrT = item.totalAmount || item.valorTotal || 0;

          csvRows.push([
            csvEscape("Item"),
            csvEscape(id),
            csvEscape(obraNome),
            csvEscape(empresaNome),
            csvEscape("---"),
            csvEscape("---"),
            csvEscape("---"),
            csvEscape(data),
            csvEscape(vlrT),
            csvEscape(status),
            csvEscape("---"),
            csvEscape(desc),
            csvEscape(qtd),
            csvEscape(un),
            csvEscape(vlrU)
          ].join(","));
        }
      }

      for (const f of financeiro) {
        const id = f.id || f.billId || f.code || f.codigoTitulo || "";
        const obraId = f.enterpriseId || f.idEnterprise || f.idObra || f.codigoVisivelObra || "";
        const obraObj = obrasById.get(String(obraId));
        const obraNome =
          f.enterpriseName ||
          f.nomeObra ||
          obraObj?.name ||
          obraObj?.nome ||
          (obraId ? `Obra ${obraId}` : "Não Informado");

        const companyId = obraObj?.companyId || obraObj?.idCompany || f.companyId || "";
        const empresaNome =
          empresasById.get(String(companyId))?.name ||
          empresasById.get(String(companyId))?.nome ||
          "Dinamica";

        const desc = f.notes || f.description || f.descricao || f.history || f.historico || "Título a Pagar";
        const valor = f.totalInvoiceAmount || f.amount || f.valor || f.balanceAmount || f.valorSaldo || 0;
        const data = f.dueDate || f.issueDate || f.dataVencimento || f.dataEmissao || "";
        const status = f.status || f.situacao || "ABERTO";

        csvRows.push([
          csvEscape("A Pagar"),
          csvEscape(id),
          csvEscape(obraNome),
          csvEscape(empresaNome),
          csvEscape(desc),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape(data),
          csvEscape(valor),
          csvEscape(status),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---")
        ].join(","));
      }

      for (const r of receber) {
        const id = r.id || r.statementId || r.numero || r.codigoTitulo || "";
        const obraId = r.enterpriseId || r.idEnterprise || r.idObra || r.codigoVisivelObra || "";
        const obraObj = obrasById.get(String(obraId));
        const obraNome =
          r.enterpriseName ||
          r.nomeObra ||
          obraObj?.name ||
          obraObj?.nome ||
          (obraId ? `Obra ${obraId}` : "Não Informado");

        const companyId = obraObj?.companyId || obraObj?.idCompany || r.companyId || "";
        const empresaNome =
          empresasById.get(String(companyId))?.name ||
          empresasById.get(String(companyId))?.nome ||
          "Dinamica";

        const desc = r.description || r.descricao || r.notes || r.history || r.historico || "Título a Receber";
        const valor = r.value || r.amount || r.valor || r.balanceAmount || r.valorSaldo || 0;
        const data = r.date || r.dueDate || r.dataVencimento || r.issueDate || r.dataEmissao || "";
        const status = r.status || r.situacao || "ABERTO";

        csvRows.push([
          csvEscape("A Receber"),
          csvEscape(id),
          csvEscape(obraNome),
          csvEscape(empresaNome),
          csvEscape(desc),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape(data),
          csvEscape(valor),
          csvEscape(status),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---"),
          csvEscape("---")
        ].join(","));
      }

      await saveToFile("consolidado.csv", "\uFEFF" + csvRows.join("\n"));

      console.log("✅ [Sync] Sincronização concluída com sucesso.");
      return true;
    } catch (e: any) {
      console.error("❌ [Sync] Erro crítico na sincronização:", e?.response?.data || e?.message || e);
      return false;
    } finally {
      isSyncing = false;
    }
  }

  // =========================================================
  // AUTO SYNC
  // =========================================================
  setInterval(() => {
    syncAllData().catch((e) => console.error(e));
  }, 30 * 60 * 1000);

  // =========================================================
  // ROTAS DE TESTE / SUPORTE
  // =========================================================
  app.post("/api/sienge/sync", async (_req, res) => {
    const success = await syncAllData();
    if (success) {
      return res.json({
        ok: true,
        message: "Sincronização concluída com sucesso",
        timestamp: new Date().toISOString()
      });
    }
    return res.status(500).json({
      ok: false,
      error: "Falha na sincronização"
    });
  });

  app.get("/api/sienge/download-csv", async (_req, res) => {
    const filePath = path.join(DATA_DIR, "consolidado.csv");
    if (!existsSync(filePath)) {
      return res.status(404).json({
        error: "Arquivo CSV ainda não foi gerado."
      });
    }
    return res.download(filePath, "sienge_consolidado.csv");
  });

  app.get("/api/sienge/test", async (_req, res) => {
    const endpoints = [
      { key: "units-of-measure", path: "/units-of-measure", params: { limit: 1, offset: 0 } },
      { key: "enterprises", path: "/enterprises", params: { limit: 1, offset: 0 } },
      { key: "purchase-orders", path: "/purchase-orders", params: { limit: 1, offset: 0 } },
      { key: "bills", path: "/bills", params: { startDate: "2019-01-01", endDate: "2030-12-31", limit: 1, offset: 0 } },
      { key: "accounts-statements", path: "/accounts-statements", params: { startDate: "2019-01-01", endDate: "2030-12-31", limit: 1, offset: 0 } },
      { key: "users", path: "/users", params: { limit: 1, offset: 0 } },
      { key: "companies", path: "/companies", params: { limit: 1, offset: 0 } }
    ];

    const results = [];

    for (const item of endpoints) {
      try {
        const response = await siengeAPI.get(item.path, { params: item.params });
        results.push({
          endpoint: item.key,
          path: item.path,
          status: response.status,
          ok: true,
          sampleCount: normalizeArrayPayload(response.data).length
        });
      } catch (e: any) {
        results.push({
          endpoint: item.key,
          path: item.path,
          status: e?.response?.status || 500,
          ok: false,
          error: e?.response?.data || e?.message
        });
      }
    }

    return res.json({
      baseURL: SIENGE_BASE_URL,
      instance: SIENGE_INSTANCE,
      results
    });
  });

  // =========================================================
  // ROTAS DE LEITURA DO CACHE / API
  // =========================================================
  app.get("/api/sienge/itens-pedidos", async (_req, res) => {
    try {
      const cached = await readFromFile("itens_pedidos.json");
      return res.json(cached || {});
    } catch (e: any) {
      return res.status(500).json({ error: e?.message || "Failed to read itens cache" });
    }
  });

  app.post("/api/sienge/fetch-items", async (req, res) => {
    try {
      const { ids } = req.body;

      if (!Array.isArray(ids)) {
        return res.status(400).json({ error: "Body deve conter ids: []" });
      }

      const itemsMap: Record<string, any[]> = (await readFromFile("itens_pedidos.json")) || {};
      let changed = false;

      for (const id of ids) {
        if (!itemsMap[id]) {
          try {
            const result = await siengeAPI.get(`/purchase-orders/${id}/items`);
            itemsMap[id] = normalizeArrayPayload(result.data);
            changed = true;
          } catch {
            itemsMap[id] = [];
            changed = true;
          }
        }
      }

      if (changed) {
        await saveToFile("itens_pedidos.json", itemsMap);
      }

      return res.json(itemsMap);
    } catch (error: any) {
      return res.status(500).json({ status: "error", error: error.message });
    }
  });

  app.get("/api/sienge/financeiro", async (req, res) => {
    try {
      const cached = await readFromFile("financeiro.json");
      if (cached && !req.query.force) return res.json(cached);

      const response = await siengeAPI.get("/bills", { params: req.query });
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/financeiro/receber", async (req, res) => {
    try {
      const cached = await readFromFile("receber.json");
      if (cached && !req.query.force) return res.json(cached);

      const response = await siengeAPI.get("/accounts-statements", { params: req.query });
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/obras", async (_req, res) => {
    try {
      let cached = await readFromFile("obras.json");
      if (!cached) {
        try {
          const response = await siengeAPI.get("/enterprises");
          cached = response.data;
        } catch {
          // keep cached as undefined if failed
        }
      }
      
      const meta = (await readFromFile("obras_meta.json")) || {};
      
      if (cached) {
        const results = normalizeArrayPayload(cached).map((obra: any) => {
          return {
            ...obra,
            engineer: meta[obra.id]?.engineer || obra.engineer || ""
          }
        });
        return res.json({ results });
      }
      
      return res.json([]);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.post("/api/sienge/obras/meta", async (req, res) => {
    try {
      const { id, engineer } = req.body;
      const meta = (await readFromFile("obras_meta.json")) || {};
      meta[id] = { ...meta[id], engineer };
      await saveToFile("obras_meta.json", meta);
      res.json({ success: true, meta: meta[id] });
    } catch (error: any) {
      return res.status(500).json(error.message);
    }
  });

  app.get("/api/sienge/obras-by-code", async (_req, res) => {
    try {
      const cached = await readFromFile("obras_by_code.json");
      return res.json(cached || {});
    } catch (error: any) {
      return res.status(500).json(error.message);
    }
  });

  app.get("/api/sienge/usuarios", async (_req, res) => {
    try {
      const cached = await readFromFile("usuarios.json");
      if (cached) return res.json(cached);

      const response = await siengeAPI.get("/users");
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/credores", async (_req, res) => {
    try {
      const cached = await readFromFile("credores.json");
      if (cached) return res.json(cached);

      return res.json([]);
    } catch (error: any) {
      return res.status(500).json(error.message);
    }
  });

  app.get("/api/sienge/companies", async (_req, res) => {
    try {
      const cached = await readFromFile("empresas.json");
      if (cached) return res.json(cached);

      const response = await siengeAPI.get("/companies");
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/unidades-medida", async (_req, res) => {
    try {
      const cached = await readFromFile("units-of-measure.json");
      if (cached) return res.json(cached);

      const response = await siengeAPI.get("/units-of-measure");
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/pedidos-compra", async (req, res) => {
    try {
      const cached = await readFromFile("pedidos.json");
      if (cached && !req.query.force) return res.json(cached);

      const response = await siengeAPI.get("/purchase-orders", { params: req.query });
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/pedidos-compra/:id/itens", async (req, res) => {
    try {
      const { id } = req.params;

      const cachedItems = await readFromFile("itens_pedidos.json");
      if (cachedItems && cachedItems[id]) {
        return res.json(cachedItems[id]);
      }

      const response = await siengeAPI.get(`/purchase-orders/${id}/items`);
      return res.json(response.data);
    } catch (error: any) {
      return res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  // =========================================================
  // VITE / FRONTEND
  // =========================================================
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa"
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (_req, res) => res.sendFile(path.join(distPath, "index.html")));
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`
✅ Servidor rodando!
🚀 Local: http://localhost:${PORT}
🔗 API Sienge: ${SIENGE_BASE_URL}
⚙️ Atualização Automática: ON (a cada 30 min)
    `);

    setTimeout(() => {
      syncAllData().catch((e) => console.error(e));
    }, 4000);
  });
}

startServer().catch((e) => {
  console.error("❌ Erro fatal ao iniciar servidor:", e);
});