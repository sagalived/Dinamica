import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import axios from "axios";
import dotenv from "dotenv";
import fs from "fs/promises";
import { existsSync, mkdirSync } from "fs";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";

dotenv.config();

async function startServer() {
  const app = express();
  const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

  app.use(express.json());

  // --- CONFIGURAÇÃO SIENGE ---
  const SIENGE_USERNAME = process.env.SIENGE_USERNAME || 'dinamicaempreendimentos-jrmorais';
  const SIENGE_PASSWORD = process.env.SIENGE_PASSWORD || '5jT2uxIW6YYAPL2epk9QUUvCEGM2eX9z';
  const SIENGE_INSTANCE = (process.env.SIENGE_INSTANCE || 'dinamicaempreendimentos').split('.')[0];

  // URL Padrão do Sienge para integrações de backend
  const SIENGE_BASE_URL = `https://api.sienge.com.br/${SIENGE_INSTANCE}`;

  // Autenticação Basic em Base64
  const siengeAuth = Buffer.from(`${SIENGE_USERNAME}:${SIENGE_PASSWORD}`).toString("base64");

  // Helper centralizado para chamadas à API
  const siengeAPI = axios.create({
    baseURL: SIENGE_BASE_URL,
    headers: {
      'Authorization': `Basic ${siengeAuth}`,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    },
    timeout: 60000 // 60 segundos
  });

  // --- DESCOBERTA AUTOMÁTICA DE CAMINHO ---
  const detectedPrefix = '/public/api/v1';

  // Usa diretamente o prefixo oficial da Public API do Sienge.


  // --- PERSISTÊNCIA DE DADOS (CACHE) ---
  const DATA_DIR = path.join(process.cwd(), "data");
  if (!existsSync(DATA_DIR)) {
    mkdirSync(DATA_DIR);
  }
  const DB_PATH = path.join(DATA_DIR, "dinamica.db");
  const db = new DatabaseSync(DB_PATH);

  db.exec(`
    CREATE TABLE IF NOT EXISTS dataset_cache (
      key TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS building_meta (
      building_id TEXT PRIMARY KEY,
      engineer TEXT DEFAULT '',
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS app_locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT UNIQUE,
      name TEXT NOT NULL,
      address TEXT DEFAULT '',
      latitude REAL,
      longitude REAL,
      type TEXT NOT NULL DEFAULT 'custom',
      source TEXT NOT NULL DEFAULT 'manual',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS app_users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE,
      name TEXT NOT NULL,
      role TEXT DEFAULT '',
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sync_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      status TEXT NOT NULL,
      notes TEXT DEFAULT ''
    );
  `);

  function columnExists(table: string, column: string) {
    const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name?: string }>;
    return rows.some((row) => row.name === column);
  }

  if (!columnExists("app_users", "email")) {
    db.exec(`ALTER TABLE app_users ADD COLUMN email TEXT DEFAULT ''`);
  }

  if (!columnExists("app_users", "password_hash")) {
    db.exec(`ALTER TABLE app_users ADD COLUMN password_hash TEXT DEFAULT ''`);
  }

  if (!columnExists("app_users", "department")) {
    db.exec(`ALTER TABLE app_users ADD COLUMN department TEXT DEFAULT ''`);
  }

  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_app_users_email ON app_users(email) WHERE email <> ''`);

  function hashPassword(password: string) {
    return createHash("sha256").update(password).digest("hex");
  }

  function seedDefaultAdminUser() {
    const now = new Date().toISOString();
    db.prepare(`
      INSERT INTO app_users (username, email, password_hash, name, role, department, active, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(username) DO UPDATE SET
        email = excluded.email,
        password_hash = excluded.password_hash,
        name = excluded.name,
        role = excluded.role,
        department = excluded.department,
        active = excluded.active,
        updated_at = excluded.updated_at
    `).run(
      "dev@admin.com",
      "dev@admin.com",
      hashPassword("admin"),
      "Administrador Dev",
      "developer",
      "Tecnologia",
      1,
      now,
      now
    );
  }

  seedDefaultAdminUser();

  function saveDatasetCache(key: string, payload: any) {
    const serialized = JSON.stringify(payload ?? null);
    db.prepare(`
      INSERT INTO dataset_cache (key, payload, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET
        payload = excluded.payload,
        updated_at = excluded.updated_at
    `).run(key, serialized, new Date().toISOString());
  }

  function readDatasetCache(key: string) {
    const row = db.prepare("SELECT payload FROM dataset_cache WHERE key = ?").get(key) as { payload?: string } | undefined;
    if (!row?.payload) return null;
    try {
      return JSON.parse(row.payload);
    } catch {
      return null;
    }
  }

  function saveBuildingMetaToDb(buildingId: string, engineer: string) {
    db.prepare(`
      INSERT INTO building_meta (building_id, engineer, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(building_id) DO UPDATE SET
        engineer = excluded.engineer,
        updated_at = excluded.updated_at
    `).run(buildingId, engineer, new Date().toISOString());
  }

  function readBuildingMetaFromDb() {
    const rows = db.prepare("SELECT building_id, engineer FROM building_meta").all() as Array<{ building_id: string; engineer: string }>;
    return rows.reduce<Record<string, { engineer: string }>>((acc, row) => {
      acc[String(row.building_id)] = { engineer: row.engineer || "" };
      return acc;
    }, {});
  }

  function normalizeBuildingName(raw: any, fallbackId?: string | number) {
    const resolved =
      raw?.name ||
      raw?.nome ||
      raw?.tradeName ||
      raw?.description ||
      raw?.enterpriseName ||
      raw?.nomeObra ||
      raw?.fantasyName ||
      "";
    const clean = fixServerText(String(resolved || "").trim());
    if (clean) return clean;
    return fallbackId ? `Obra ${fallbackId}` : "Obra sem nome";
  }

  function normalizeCreditorName(raw: any, fallbackId?: string | number) {
    const resolved =
      raw?.name ||
      raw?.nome ||
      raw?.nomeFantasia ||
      raw?.supplierName ||
      raw?.creditorName ||
      raw?.nomeFornecedor ||
      raw?.fornecedor ||
      "";
    const clean = fixServerText(String(resolved || "").trim());
    if (clean) return clean;
    return fallbackId ? `Credor ${fallbackId}` : "Credor sem nome";
  }

  function fixServerText(value: unknown) {
    if (value == null) return "";
    let text = String(value);

    const applyCommonFixes = (input: string) => input
      .replace(/CONSTRU��O/g, "CONSTRUÇÃO")
      .replace(/MANUTEN��O/g, "MANUTENÇÃO")
      .replace(/ESPA�O/g, "ESPAÇO")
      .replace(/VIV�NCIA/g, "VIVÊNCIA")
      .replace(/TAU�/g, "TAUÁ")
      .replace(/TIANGU�/g, "TIANGUÁ")
      .replace(/QUIXAD�/g, "QUIXADÁ")
      .replace(/CANIND�/g, "CANINDÉ")
      .replace(/MARACANA�/g, "MARACANAÚ")
      .replace(/EDUCA��O/g, "EDUCAÇÃO")
      .replace(/CI�NCIA/g, "CIÊNCIA")
      .replace(/CEAR�/g, "CEARÁ")
      .replace(/PAVIMENTA��O/g, "PAVIMENTAÇÃO")
      .replace(/REGULARIZA��O/g, "REGULARIZAÇÃO")
      .replace(/REQUALIFICA��O/g, "REQUALIFICAÇÃO")
      .replace(/DUPLICA��O/g, "DUPLICAÇÃO")
      .replace(/AMPLIA��O/g, "AMPLIAÇÃO")
      .replace(/SERVI�OS/g, "SERVIÇOS")
      .replace(/GEST�O/g, "GESTÃO")
      .replace(/SUBESTA��O/g, "SUBESTAÇÃO")
      .replace(/A�UDE/g, "AÇUDE")
      .replace(/S�O/g, "SÃO")
      .replace(/JO�O/g, "JOÃO");

    if (!/Ã.|Â.|â.|ï¿½|�|�/.test(text)) return applyCommonFixes(text);
    try {
      text = Buffer.from(text, "latin1").toString("utf8");
      return applyCommonFixes(text);
    } catch {
      return applyCommonFixes(text);
    }
  }

  function normalizePersonName(value: any) {
    const raw = typeof value === "string"
      ? value
      : value?.name || value?.nome || value?.username || value?.userName || "";
    return fixServerText(String(raw || ""))
      .replace(/^comprador\s+/i, "")
      .replace(/^usu[aá]rio\s+/i, "")
      .trim();
  }

  function resolveBuildingNameFromCaches(buildingId: string, fallback?: string) {
    const pedidosCache = readDatasetCache("pedidos") || {};
    const pedidosList = Array.isArray(pedidosCache?.results) ? pedidosCache.results : Array.isArray(pedidosCache) ? pedidosCache : [];
    const matchedOrder = pedidosList.find((item: any) => String(item.codigoVisivelObra || item.idObra || item.buildingId || "") === buildingId);
    const name = normalizeBuildingName(matchedOrder, buildingId);
    if (name && name !== `Obra ${buildingId}` && name !== "Obra sem nome") {
      return name;
    }
    return fallback || `Obra ${buildingId}`;
  }

  function resolveCreditorNameFromCaches(creditorId: string, fallback?: string) {
    const pedidosCache = readDatasetCache("pedidos") || {};
    const financeiroCache = readDatasetCache("financeiro") || {};
    const pedidosList = Array.isArray(pedidosCache?.results) ? pedidosCache.results : Array.isArray(pedidosCache) ? pedidosCache : [];
    const financeiroList = Array.isArray(financeiroCache?.results) ? financeiroCache.results : Array.isArray(financeiroCache) ? financeiroCache : [];

    const pedido = pedidosList.find((item: any) => String(item.codigoFornecedor || item.idCredor || item.supplierId || "") === creditorId);
    const financeiro = financeiroList.find((item: any) => String(item.creditorId || item.idCredor || item.codigoFornecedor || item.debtorId || "") === creditorId);

    const candidate = normalizeCreditorName(pedido, creditorId);
    if (candidate && candidate !== `Credor ${creditorId}` && candidate !== "Credor sem nome") {
      return candidate;
    }

    const financialCandidate = normalizeCreditorName(financeiro, creditorId);
    if (financialCandidate && financialCandidate !== `Credor ${creditorId}` && financialCandidate !== "Credor sem nome") {
      return financialCandidate;
    }

    return fallback || `Credor ${creditorId}`;
  }

  function toArray(payload: any) {
    if (Array.isArray(payload?.results)) return payload.results;
    if (Array.isArray(payload)) return payload;
    return [];
  }

  function toCoordinate(value: unknown) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  async function geocodeAddress(address: string) {
    const response = await axios.get("https://nominatim.openstreetmap.org/search", {
      params: {
        format: "jsonv2",
        limit: 1,
        q: address,
      },
      headers: {
        "User-Agent": "DinamicaDashboard/1.0",
      },
      timeout: 20000,
    });

    const firstResult = Array.isArray(response.data) ? response.data[0] : null;
    if (!firstResult) return null;

    const latitude = toCoordinate(firstResult.lat);
    const longitude = toCoordinate(firstResult.lon);
    if (latitude === null || longitude === null) return null;
    return { latitude, longitude };
  }

  async function resolveRoutePoint(point: any) {
    const latitude = toCoordinate(point?.latitude);
    const longitude = toCoordinate(point?.longitude);

    if (latitude !== null && longitude !== null) {
      return { latitude, longitude };
    }

    const address = String(point?.address || "").trim();
    if (!address) return null;
    return geocodeAddress(address);
  }

  function extractDistanceKmFromGoogleHtml(html: string) {
    const patterns = [
      /"distance":"\s*([0-9]+(?:[.,][0-9]+)?)\s*km"/i,
      /"distanceText":"\s*([0-9]+(?:[.,][0-9]+)?)\s*km"/i,
      /aria-label="([0-9]+(?:[.,][0-9]+)?)\s*km"/i,
      />([0-9]+(?:[.,][0-9]+)?)\s*km</i,
    ];

    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (!match?.[1]) continue;
      const numeric = Number(match[1].replace(",", "."));
      if (Number.isFinite(numeric) && numeric > 0) {
        return numeric;
      }
    }

    return null;
  }

  async function getGoogleMapsPublicDistance(origin: any, destination: any) {
    try {
      const response = await axios.get("https://www.google.com/maps/dir/", {
        params: {
          api: 1,
          origin: origin?.address || `${origin?.latitude},${origin?.longitude}`,
          destination: destination?.address || `${destination?.latitude},${destination?.longitude}`,
          travelmode: "driving",
        },
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
          "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        },
        timeout: 20000,
      });

      const html = String(response.data || "");
      const distanceKm = extractDistanceKmFromGoogleHtml(html);
      if (distanceKm !== null) {
        console.log("[Route] Google Maps público retornou distância", distanceKm);
        return {
          distanceKm,
          provider: "Google Maps",
        };
      }
    } catch (error) {
      console.error("[Route] Falha ao extrair distância da página pública do Google Maps:", (error as any)?.message || error);
    }

    return null;
  }

  async function calculateRouteDistance(origin: any, destination: any) {
    console.log("[Route] Calculando rota", { origin, destination });
    const googleMapsApiKey = String(process.env.GOOGLE_MAPS_API_KEY || "").trim();

    if (googleMapsApiKey) {
      try {
        const response = await axios.get("https://maps.googleapis.com/maps/api/directions/json", {
          params: {
            origin: origin?.address || `${origin?.latitude},${origin?.longitude}`,
            destination: destination?.address || `${destination?.latitude},${destination?.longitude}`,
            mode: "driving",
            language: "pt-BR",
            key: googleMapsApiKey,
          },
          timeout: 20000,
        });

        const googleDistanceMeters = response.data?.routes?.[0]?.legs?.[0]?.distance?.value;
        if (typeof googleDistanceMeters === "number") {
          console.log("[Route] Google Maps API retornou distância", googleDistanceMeters / 1000);
          return {
            distanceKm: googleDistanceMeters / 1000,
            provider: "Google Maps",
          };
        }
      } catch (error) {
        console.error("[Route] Falha ao consultar Google Maps:", (error as any)?.message || error);
      }
    }

    const googlePublicDistance = await getGoogleMapsPublicDistance(origin, destination);
    if (googlePublicDistance) {
      return googlePublicDistance;
    }

    const originCoords = await resolveRoutePoint(origin);
    const destinationCoords = await resolveRoutePoint(destination);
    console.log("[Route] Coordenadas resolvidas", { originCoords, destinationCoords });
    if (!originCoords || !destinationCoords) {
      console.log("[Route] Coordenadas insuficientes para calcular.");
      return { distanceKm: null, provider: "" };
    }

    try {
      const osrmResponse = await axios.get(
        `https://router.project-osrm.org/route/v1/driving/${originCoords.longitude},${originCoords.latitude};${destinationCoords.longitude},${destinationCoords.latitude}`,
        {
          params: { overview: "false" },
          timeout: 20000,
        }
      );

      const osrmDistanceMeters = osrmResponse.data?.routes?.[0]?.distance;
      if (typeof osrmDistanceMeters === "number") {
        console.log("[Route] OSRM retornou distância", osrmDistanceMeters / 1000);
        return {
          distanceKm: osrmDistanceMeters / 1000,
          provider: "OSRM",
        };
      }
    } catch (error) {
      console.error("[Route] Falha ao consultar OSRM:", (error as any)?.message || error);
    }

    console.log("[Route] Nenhum provedor retornou distância.");
    return {
      distanceKm: null,
      provider: "",
    };
  }

  function getLatestSyncInfo() {
    const row = db.prepare(`
      SELECT started_at, finished_at, status, notes
      FROM sync_runs
      ORDER BY id DESC
      LIMIT 1
    `).get() as { started_at?: string; finished_at?: string; status?: string; notes?: string } | undefined;
    return row || null;
  }

  async function saveToFile(filename: string, data: any) {
    try {
      await fs.writeFile(path.join(DATA_DIR, filename), JSON.stringify(data, null, 2));
    } catch (e) {
      console.error(`[Cache] Erro ao salvar ${filename}:`, e);
    }
  }

  async function readFromFile(filename: string) {
    const filePath = path.join(DATA_DIR, filename);
    if (existsSync(filePath)) {
      try {
        const content = await fs.readFile(filePath, "utf-8");
        return JSON.parse(content);
      } catch (e) {
        console.error(`[Cache] Erro ao ler ${filename}:`, e);
        return null;
      }
    }
    return null;
  }

  async function readObrasMeta() {
    const fileMeta = await readFromFile("obras_meta.json") || {};
    const dbMeta = readBuildingMetaFromDb();
    return { ...fileMeta, ...dbMeta };
  }

  async function saveObrasMeta(meta: Record<string, any>) {
    await saveToFile("obras_meta.json", meta);
    Object.entries(meta).forEach(([buildingId, item]) => {
      saveBuildingMetaToDb(String(buildingId), String((item as any)?.engineer || ""));
    });
  }

  async function fetchAll(endpoint: string, baseParams: any = {}) {
    let allResults: any[] = [];
    let offset = 0;
    const limit = 200;
    while (true) {
      try {
        const res = await siengeAPI.get(endpoint, { params: { ...baseParams, limit, offset } });
        const results = res.data.results || (Array.isArray(res.data) ? res.data : []);
        if (!Array.isArray(results) || results.length === 0) break;
        allResults = allResults.concat(results);
        offset += results.length;
        if (results.length < limit && (!res.data.resultSetMetadata || offset >= res.data.resultSetMetadata.count)) break;
      } catch (err: any) {
        if (offset === 0) throw err; // Falha real se quebrar na primeira pág
        break;
      }
    }
    return { data: { results: allResults } };
  }

  let isSyncing = false;
  async function syncAllData() {
    if (isSyncing) {
      console.log("⏳ [Sync] Sincronização com Sienge já está em andamento. Cache mantido.");
      return;
    }
    isSyncing = true;
    const syncStart = new Date().toISOString();
    const syncRun = db.prepare("INSERT INTO sync_runs (started_at, status, notes) VALUES (?, ?, ?)").run(
      syncStart,
      "running",
      "Sincronizacao Sienge completa"
    );
    console.log("🔄 [Sync] Iniciando sincronização completa com Sienge...");
    try {
      const startDateStr = "1900-01-01";
      const endDateStr = "2030-12-31";

      const [obrasRes, usuariosRes, credoresRes, pedidosRes, poRestRes, financeiroRes, receberRes, empresasRes, clientesRes] = await Promise.allSettled([
        fetchAll(`${detectedPrefix}/enterprises`).catch(() => null),
        fetchAll(`${detectedPrefix}/users`).catch(() => null),
        fetchAll(`${detectedPrefix}/creditors`).catch(() => null),
        fetchAll(`${detectedPrefix}/purchase-orders`),
        fetchAll(`/public/api/v1/purchase-orders`),
        fetchAll(`${detectedPrefix}/bills`, { startDate: startDateStr, endDate: endDateStr }),
        fetchAll(`${detectedPrefix}/accounts-statements`, { startDate: startDateStr, endDate: endDateStr }),
        fetchAll(`${detectedPrefix}/companies`).catch(() => null),
        siengeAPI.get(`${detectedPrefix}/clientes`).catch(() => null)
      ]);

      let pedidos = pedidosRes.status === 'fulfilled' && pedidosRes.value ? (pedidosRes.value.data.results || pedidosRes.value.data) : [];
      let poRest = poRestRes.status === 'fulfilled' && poRestRes.value ? (poRestRes.value.data.results || poRestRes.value.data) : [];
      
      // Carregar cache de solicitantes real
      let solicitantesCache: Record<string, string> = {};
      try {
        if (existsSync(path.join(DATA_DIR, 'solicitantes-cache.json'))) {
          solicitantesCache = JSON.parse(await fs.readFile(path.join(DATA_DIR, 'solicitantes-cache.json'), 'utf-8'));
        }
      } catch(e) {}

      // Resolver nomes dos solicitantes (Diferenciando Comprador vs Solicitante)
      if (Array.isArray(pedidos) && Array.isArray(poRest)) {
        const poMap = new Map();
        poRest.forEach(po => {
          if (po.id) poMap.set(po.id, po);
        });
        
        const missingReqs = new Set<string>();

        // 1ª Passagem - Identifica Requests ausentes
        pedidos.forEach(p => {
          const id = p.numero || p.id;
          const poObj = poMap.get(id);
          if (poObj) {
            p.createdBy =
              normalizePersonName(poObj.createdBy) ||
              normalizePersonName(poObj.buyerName) ||
              normalizePersonName(p.nomeComprador) ||
              String(p.codigoComprador || '').trim();
            const note = fixServerText(poObj.internalNotes || poObj.notes || "");
            const reqMatch = note.match(/SOLICITA[CÇ][AÃ]O\s+(\d+)/i) || note.match(/REQ(?:UISION)?\s+(\d+)/i);
            
            if (reqMatch) {
              const reqId = reqMatch[1];
              p.reqIdOrigin = reqId; 
              if (!solicitantesCache[reqId]) missingReqs.add(reqId);
            }
          }
        });

        // 2ª Passagem - Busca os faltantes na Sienge com concorrência batelada (Lot of 15)
        if (missingReqs.size > 0) {
          console.log(`[Sync] Identificados ${missingReqs.size} tickets de Solicitação novos para extrair o Solicitante Real. Baixando...`);
          const reqArray = Array.from(missingReqs);
          for (let i = 0; i < reqArray.length; i += 15) {
            const batch = reqArray.slice(i, i + 15);
            await Promise.all(batch.map(async (reqId) => {
              try {
                const res = await siengeAPI.get(`/public/api/v1/purchase-requests/${reqId}`);
                if (res && res.data && res.data.requesterUser) {
                  const requesterName = normalizePersonName(res.data.requesterUser);
                  if (requesterName) {
                    solicitantesCache[reqId] = requesterName;
                  }
                }
              } catch(e) {
                // Ignore errors like 404
              }
            }));
          }
          await fs.writeFile(path.join(DATA_DIR, 'solicitantes-cache.json'), JSON.stringify(solicitantesCache, null, 2));
        }

        // 3ª Passagem - Injeta definitvamente no catálogo de dados
        pedidos.forEach(p => {
          if (p.reqIdOrigin && solicitantesCache[p.reqIdOrigin]) {
            p.solicitante = solicitantesCache[p.reqIdOrigin];
            p.requesterId = solicitantesCache[p.reqIdOrigin];
          } else {
            // Se não originou de Solicitação (compras diretas), mantém o comprador
            const fallbackRequester = normalizePersonName(p.createdBy);
            p.solicitante = fallbackRequester;
            p.requesterId = fallbackRequester;
          }
        });
      }
      let financeiro = financeiroRes.status === 'fulfilled' && financeiroRes.value ? (financeiroRes.value.data.results || financeiroRes.value.data) : [];
      let receber = receberRes.status === 'fulfilled' && receberRes.value ? (receberRes.value.data.results || receberRes.value.data) : [];
      
      let obras = obrasRes.status === 'fulfilled' && obrasRes.value?.data ? (obrasRes.value.data.results || obrasRes.value.data) : [];
      let usuarios = usuariosRes.status === 'fulfilled' && usuariosRes.value?.data ? (usuariosRes.value.data.results || usuariosRes.value.data) : [];
      let credores = credoresRes.status === 'fulfilled' && credoresRes.value?.data ? (credoresRes.value.data.results || credoresRes.value.data) : [];
      let empresas = empresasRes.status === 'fulfilled' && empresasRes.value?.data ? (empresasRes.value.data.results || empresasRes.value.data) : [];
      let clientes = clientesRes.status === 'fulfilled' && clientesRes.value?.data ? (clientesRes.value.data.results || clientesRes.value.data) : [];

      // Extrapolação de dados (Fallback p/ Endpoints bloqueados por código 400 da Sienge)
      if (obras.length === 0 && Array.isArray(pedidos)) {
        const map = new Map();
        pedidos.forEach((p: any) => { if (p.codigoVisivelObra || p.idObra) { const id = p.codigoVisivelObra || p.idObra; map.set(id, { id, code: String(id), nome: normalizeBuildingName(p, id), name: normalizeBuildingName(p, id) }); } });
        obras = Array.from(map.values());
      }
      if (usuarios.length === 0 && Array.isArray(pedidos)) {
        const map = new Map();
        pedidos.forEach((p: any) => { if (p.codigoComprador || p.idComprador) { const id = p.codigoComprador || p.idComprador; map.set(id, { id: String(id), nome: p.nomeComprador || String(id) }); } });
        usuarios = Array.from(map.values());
      }
      if (credores.length === 0 && Array.isArray(pedidos)) {
        const map = new Map();
        pedidos.forEach((p: any) => { if (p.codigoFornecedor || p.idCredor) { const id = p.codigoFornecedor || p.idCredor; map.set(id, { id, nome: normalizeCreditorName(p, id), name: normalizeCreditorName(p, id) }); } });
        credores = Array.from(map.values());
      }

      const obrasByCode: Record<string, any> = (await readFromFile("obras_by_code.json")) || {};
      const uniqueBuildingIds = [...new Set((pedidos as any[]).map((p: any) => p.codigoVisivelObra || p.idObra || p.buildingId).filter(Boolean).map(String))];

      for (const buildingId of uniqueBuildingIds) {
        if (obrasByCode[buildingId]?.name || obrasByCode[buildingId]?.nome) continue;
        try {
          const response = await siengeAPI.get(`${detectedPrefix}/enterprises/${buildingId}`);
          const data = response.data || {};
          obrasByCode[buildingId] = {
            id: data.id || Number(buildingId),
            code: buildingId,
            name: data.name || data.nome || data.tradeName || data.description || `Obra ${buildingId}`,
            nome: data.name || data.nome || data.tradeName || data.description || `Obra ${buildingId}`,
            address: data.address || data.adress || data.endereco || "",
            endereco: data.address || data.adress || data.endereco || "",
            companyId: data.companyId || data.idCompany || null,
            latitude: data.latitude,
            longitude: data.longitude
          };
        } catch (_error) {
          const existing = (pedidos as any[]).find((p: any) => String(p.codigoVisivelObra || p.idObra || p.buildingId) === buildingId);
          obrasByCode[buildingId] = {
            id: Number(buildingId),
            code: buildingId,
            name: existing?.nomeObra || `Obra ${buildingId}`,
            nome: existing?.nomeObra || `Obra ${buildingId}`,
            address: existing?.enderecoObra || "",
            endereco: existing?.enderecoObra || ""
          };
        }
      }

      if (Object.keys(obrasByCode).length > 0) {
        await saveToFile("obras_by_code.json", obrasByCode);
        saveDatasetCache("obras_by_code", obrasByCode);
      }

      await saveToFile("obras.json", obras);
      await saveToFile("usuarios.json", usuarios);
      await saveToFile("credores.json", credores);
      await saveToFile("empresas.json", empresas);
      await saveToFile("clientes.json", clientes);
      saveDatasetCache("obras", obras);
      saveDatasetCache("usuarios", usuarios);
      saveDatasetCache("credores", credores);
      saveDatasetCache("empresas", empresas);
      saveDatasetCache("clientes", clientes);

      db.prepare(`
        INSERT INTO app_locations (code, name, address, latitude, longitude, type, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
          name = excluded.name,
          address = excluded.address,
          latitude = excluded.latitude,
          longitude = excluded.longitude,
          type = excluded.type,
          source = excluded.source,
          updated_at = excluded.updated_at
      `).run(
        "hq",
        "Sede",
        "Dinamica Empreendimentos e Solucoes LTDA, Fortaleza, CE, Brasil",
        -3.7319,
        -38.5267,
        "hq",
        "system",
        syncStart,
        syncStart
      );

      const upsertLocation = db.prepare(`
        INSERT INTO app_locations (code, name, address, latitude, longitude, type, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
          name = excluded.name,
          address = excluded.address,
          latitude = excluded.latitude,
          longitude = excluded.longitude,
          type = excluded.type,
          source = excluded.source,
          updated_at = excluded.updated_at
      `);

      [...obras, ...Object.values(obrasByCode)].forEach((obra: any) => {
        const code = String(obra.code || obra.codigoVisivel || obra.id || "").trim();
        if (!code) return;
        const name = normalizeBuildingName(obra, code);
        upsertLocation.run(
          `building:${code}`,
          name,
          String(obra.address || obra.endereco || name),
          obra.latitude ?? null,
          obra.longitude ?? null,
          "building",
          "sienge",
          syncStart,
          new Date().toISOString()
        );
      });
      
      const itemsMap: Record<number, any> = {};
      try {
        const existingItems = await readFromFile("itens_pedidos.json") || {};
        Object.assign(itemsMap, existingItems);
      } catch (e) {}

      if (pedidosRes.status === 'fulfilled') {
        await saveToFile("pedidos.json", pedidosRes.value.data);
        saveDatasetCache("pedidos", pedidosRes.value.data);
        
        // Sincronizar itens dos pedidos mais recentes no cache
        if (Array.isArray(pedidos)) {
          const topOrders = pedidos.slice(0, 50);
          for (const order of topOrders) {
            const id = order.id || order.numero;
            if (!itemsMap[id]) {
              try {
                const items = await siengeAPI.get(`/public/api/v1/purchase-orders/${id}/items`);
                itemsMap[id] = items.data.results || items.data || [];
              } catch (e) {}
            }
          }
          await saveToFile("itens_pedidos.json", itemsMap);
          saveDatasetCache("itens_pedidos", itemsMap);
        }
      }
      if (financeiroRes.status === 'fulfilled') {
        await saveToFile("financeiro.json", financeiroRes.value.data);
        saveDatasetCache("financeiro", financeiroRes.value.data);
      }
      if (receberRes.status === 'fulfilled') {
        await saveToFile("receber.json", receberRes.value.data);
        saveDatasetCache("receber", receberRes.value.data);
      }

      // GERAR CSV CONSOLIDADO
      const csvHeaders = "Tipo,ID,Obra,Empresa,Fornecedor/Cliente/Descricao,Comprador,Data,Valor,Status,Condicao Pagamento/Prazos,Item/Insumo,Qtd,Un,Vlr Unit\n";
      const csvRows: string[] = [];

      (pedidos as any[]).forEach((o: any) => {
        const idObra = o.idObra || o.codigoVisivelObra;
        const obraObj = obras.find((b: any) => String(b.id) === String(idObra));
        const obra = obraObj?.nome || idObra || "Não Informado";
        const empresa = empresas.find((e: any) => e.id === obraObj?.idCompany)?.name || "Dinamica";
        const idCredor = o.idCredor || o.codigoFornecedor;
        const supplier = credores.find((c: any) => String(c.id) === String(idCredor))?.nome || idCredor || "Não Informado";
        const idUser = o.idComprador || o.codigoComprador;
        const user = usuarios.find((u: any) => String(u.id) === String(idUser))?.nome || idUser || "Não Informado";
        const date = o.dataEmissao || o.data || "---";
        const valor = o.valorTotal || 0;
        const status = o.situacao || "N/A";
        const condicao = o.condicaoPagamentoDescricao || "N/A";
        const prazo = o.dataEntrega || o.prazoEntrega || "---";

        csvRows.push(`Pedido,${o.id || o.numero},"${obra}","${empresa}","${supplier}","${user}",${date},${valor},${status},"${condicao} / Prazo: ${prazo}","---","---","---","---"`);

        const items = itemsMap[o.id];
        if (Array.isArray(items)) {
          items.forEach((item: any) => {
            const desc = item.descricao || item.itemNome || "Item";
            const qtd = item.quantidade || 0;
            const un = item.unidadeMedidaSigla || "UN";
            const vlrU = item.valorUnitario || 0;
            const vlrT = item.valorTotal || 0;
            csvRows.push(`Item,${o.id},"${obra}","${empresa}","---","---",${date},${vlrT},"${status}","---","${desc}",${qtd},"${un}",${vlrU}`);
          });
        }
      });

      (financeiro as any[]).forEach((f: any) => {
        const idObra = f.idObra || f.codigoVisivelObra;
        const obraObj = obras.find((b: any) => String(b.id) === String(idObra));
        const obra = obraObj?.nome || idObra || "Não Informado";
        const empresa = empresas.find((e: any) => e.id === obraObj?.idCompany)?.name || "Dinamica";
        const desc = f.descricao || f.historico || f.tipoDocumento || "Título a Pagar";
        csvRows.push(`A Pagar,${f.id || f.codigoTitulo},"${obra}","${empresa}","${desc}","---",${f.dataVencimento || f.dataEmissao || f.issueDate},${f.valor || f.valorSaldo},${f.situacao || "ABERTO"},"---","---","---","---","---"`);
      });

      (receber as any[]).forEach((r: any) => {
        const obraObj = obras.find((b: any) => b.id === r.idObra);
        const obra = obraObj?.nome || r.idObra || "Não Informado";
        const empresa = empresas.find((e: any) => e.id === obraObj?.idCompany)?.name || "Dinamica";
        const desc = r.descricao || r.historico || "Título a Receber";
        csvRows.push(`A Receber,${r.id || r.numero || r.codigoTitulo},"${obra}","${empresa}","${desc}","---",${r.dataVencimento || r.dataEmissao},${r.valor || r.valorSaldo},${r.situacao || "ABERTO"},"---","---","---","---","---"`);
      });

      await saveToFile("consolidado.csv", "\ufeff" + csvHeaders + csvRows.join("\n"));
      saveDatasetCache("consolidado_csv", "\ufeff" + csvHeaders + csvRows.join("\n"));
      db.prepare("UPDATE sync_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?").run(
        new Date().toISOString(),
        "success",
        `Sincronizacao completa desde ${startDateStr}`,
        Number(syncRun.lastInsertRowid)
      );
      console.log("✅ [Sync] Sincronização Sienge concluída! CSV atualizado.");
      return true;
    } catch (e) {
      db.prepare("UPDATE sync_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?").run(
        new Date().toISOString(),
        "error",
        String((e as any)?.message || e),
        Number(syncRun.lastInsertRowid)
      );
      console.error("❌ [Sync] Erro crítico na sincronização: ", e);
      return false;
    } finally {
      isSyncing = false;
    }
  }

  // --- ROTAS DA API ---

  // Rota para forçar sincronização manual
  app.post("/api/sienge/sync", async (req, res) => {
    const success = await syncAllData();
    if (success) {
      res.json({ message: "Sincronização concluída com sucesso", timestamp: new Date() });
    } else {
      res.status(500).json({ error: "Falha na sincronização" });
    }
  });

  // Rota para baixar o CSV consolidado
  app.get("/api/sienge/download-csv", async (req, res) => {
    const filePath = path.join(DATA_DIR, "consolidado.csv");
    if (existsSync(filePath)) {
      res.download(filePath, "sienge_consolidado.csv");
    } else {
      res.status(404).json({ error: "Arquivo CSV ainda não gerado. Aguarde a sincronização." });
    }
  });

  app.get("/api/sienge/test", async (_req, res) => {
    const pedidos = toArray(readDatasetCache("pedidos") || await readFromFile("pedidos.json"));
    const financeiro = toArray(readDatasetCache("financeiro") || await readFromFile("financeiro.json"));
    const receber = toArray(readDatasetCache("receber") || await readFromFile("receber.json"));
    const obras = toArray(readDatasetCache("obras") || await readFromFile("obras.json"));
    const credores = toArray(readDatasetCache("credores") || await readFromFile("credores.json"));
    const usuarios = toArray(readDatasetCache("usuarios") || await readFromFile("usuarios.json"));
    const latestSync = getLatestSyncInfo();

    res.json({
      ok: pedidos.length > 0 || financeiro.length > 0 || receber.length > 0,
      baseURL: `${SIENGE_BASE_URL}${detectedPrefix}`,
      cache: {
        pedidos: pedidos.length,
        financeiro: financeiro.length,
        receber: receber.length,
        obras: obras.length,
        credores: credores.length,
        usuarios: usuarios.length,
      },
      latestSync,
    });
  });

  app.get("/api/sienge/bootstrap", async (_req, res) => {
    try {
      const obrasPayload = await readFromFile("obras.json") || readDatasetCache("obras");
      const obrasByCodePayload = await readFromFile("obras_by_code.json") || readDatasetCache("obras_by_code");
      const usuariosPayload = await readFromFile("usuarios.json") || readDatasetCache("usuarios");
      const credoresPayload = await readFromFile("credores.json") || readDatasetCache("credores");
      const companiesPayload = await readFromFile("empresas.json") || readDatasetCache("empresas");
      const pedidosPayload = await readFromFile("pedidos.json") || readDatasetCache("pedidos");
      const financeiroPayload = await readFromFile("financeiro.json") || readDatasetCache("financeiro");
      const receberPayload = await readFromFile("receber.json") || readDatasetCache("receber");
      const itensPayload = await readFromFile("itens_pedidos.json") || readDatasetCache("itens_pedidos") || {};
      const solicitantesCache = await readFromFile("solicitantes-cache.json") || {};
      const meta = await readObrasMeta();

      const obrasBase = toArray(obrasPayload);
      const obrasByCode = obrasByCodePayload ? Object.values(obrasByCodePayload) : [];
      const usuarios = toArray(usuariosPayload);
      const credores = toArray(credoresPayload);
      const companies = toArray(companiesPayload);
      const pedidos = toArray(pedidosPayload);
      const financeiro = toArray(financeiroPayload);
      const receber = toArray(receberPayload);

      const buildingNameHints = new Map<string, string>();
      const creditorNameHints = new Map<string, string>();

      pedidos.forEach((pedido: any) => {
        const buildingId = String(pedido.codigoVisivelObra || pedido.idObra || pedido.buildingId || "");
        const creditorId = String(pedido.codigoFornecedor || pedido.idCredor || pedido.supplierId || "");
        const buildingName = normalizeBuildingName(pedido, buildingId);
        const creditorName = normalizeCreditorName(pedido, creditorId);

        if (buildingId && buildingName && buildingName !== `Obra ${buildingId}` && buildingName !== "Obra sem nome") {
          buildingNameHints.set(buildingId, buildingName);
        }
        if (creditorId && creditorName && creditorName !== `Credor ${creditorId}` && creditorName !== "Credor sem nome") {
          creditorNameHints.set(creditorId, creditorName);
        }
      });

      financeiro.forEach((item: any) => {
        const creditorId = String(item.creditorId || item.idCredor || item.codigoFornecedor || item.debtorId || "");
        const creditorName = normalizeCreditorName(item, creditorId);
        if (creditorId && creditorName && creditorName !== `Credor ${creditorId}` && creditorName !== "Credor sem nome") {
          creditorNameHints.set(creditorId, creditorName);
        }
      });

      const buildingMap = new Map<string, any>();
      [...obrasBase, ...obrasByCode].forEach((obra: any) => {
        const id = String(obra.id || obra.code || obra.codigoVisivel || "");
        if (!id) return;
        const normalizedName = normalizeBuildingName(obra, id);
        const fallbackName =
          (normalizedName !== `Obra ${id}` && normalizedName !== "Obra sem nome" ? normalizedName : "") ||
          buildingNameHints.get(id) ||
          `Obra ${id}`;
        buildingMap.set(id, {
          id: Number(obra.id || id),
          code: String(obra.code || obra.codigoVisivel || id),
          name: fallbackName,
          nome: fallbackName,
          address: obra.address || obra.endereco || obra.adress || "",
          endereco: obra.endereco || obra.address || obra.adress || "",
          latitude: obra.latitude,
          longitude: obra.longitude,
          companyId: obra.companyId || obra.idCompany || null,
          engineer: meta[id]?.engineer || obra.engineer || obra.responsavelTecnico || "",
        });
      });

      pedidos.forEach((pedido: any) => {
        const id = String(pedido.codigoVisivelObra || pedido.idObra || pedido.buildingId || "");
        if (!id || buildingMap.has(id)) return;
        const hintedName = buildingNameHints.get(id) || normalizeBuildingName(pedido, id);
        buildingMap.set(id, {
          id: Number(id),
          code: id,
          name: hintedName,
          nome: hintedName,
          address: pedido.enderecoObra || "",
          endereco: pedido.enderecoObra || "",
          latitude: undefined,
          longitude: undefined,
          companyId: null,
          engineer: meta[id]?.engineer || "",
        });
      });

      const userMap = new Map<string, string>();
      usuarios.forEach((user: any) => {
        const id = String(user.id || user.userId || user.username || "");
        if (!id) return;
        userMap.set(id, normalizePersonName(user));
      });

      const creditorMap = new Map<string, string>();
      credores.forEach((credor: any) => {
        const id = String(credor.id || credor.creditorId || "");
        if (!id) return;
        const normalizedName = normalizeCreditorName(credor, id);
        creditorMap.set(
          id,
          (normalizedName !== `Credor ${id}` && normalizedName !== "Credor sem nome" ? normalizedName : "") ||
            creditorNameHints.get(id) ||
            `Credor ${id}`
        );
      });

      const normalizedOrders = pedidos.map((pedido: any) => {
        const buildingId = String(pedido.codigoVisivelObra || pedido.idObra || pedido.buildingId || "");
        const supplierId = String(pedido.codigoFornecedor || pedido.idCredor || pedido.supplierId || "");
        const buyerId = String(pedido.idComprador || pedido.codigoComprador || pedido.buyerId || "");
        const note = fixServerText(pedido.internalNotes || pedido.notes || "");
        const requestMatch = note.match(/SOLICITA[CÇ][AÃ]O\s+(\d+)/i) || note.match(/REQ(?:UISION)?\s+(\d+)/i);
        const requesterFromCache = requestMatch ? solicitantesCache?.[requestMatch[1]] : "";
        const rawRequester = String(requesterFromCache || pedido.solicitante || pedido.requesterId || pedido.requesterUser || pedido.createdBy || "").trim();
        const requesterName = normalizePersonName(userMap.get(rawRequester) || rawRequester);
        const buyerName = normalizePersonName(pedido.nomeComprador || pedido.buyerName || userMap.get(buyerId) || buyerId);
        const building = buildingMap.get(buildingId);

        return {
          id: pedido.id || pedido.numero || 0,
          buildingId: Number(buildingId || 0),
          buyerId,
          supplierId: supplierId ? Number(supplierId) : 0,
          date: pedido.data || pedido.dataEmissao || pedido.date || "",
          totalAmount: Number(pedido.totalAmount || pedido.valorTotal || 0),
          status: pedido.status || pedido.situacao || "N/A",
          paymentCondition: pedido.condicaoPagamento || pedido.paymentMethod || "A Prazo",
          deliveryDate: pedido.dataEntrega || pedido.prazoEntrega || "",
          internalNotes: pedido.internalNotes || pedido.observacao || "",
          nomeObra: building?.name || buildingNameHints.get(buildingId) || normalizeBuildingName(pedido, buildingId),
          nomeFornecedor: creditorMap.get(supplierId) || creditorNameHints.get(supplierId) || normalizeCreditorName(pedido, supplierId),
          nomeComprador: buyerName,
          solicitante: requesterName || buyerName,
          requesterId: requesterName || buyerName,
          createdBy: buyerName,
        };
      });

      const normalizedFinancial = financeiro.map((item: any) => {
        const creditorId = String(item.creditorId || item.idCredor || item.codigoFornecedor || item.debtorId || "");
        const buildingId = String(item.idObra || item.codigoObra || item.enterpriseId || "");
        return {
          id: item.id || item.numero || item.codigoTitulo || item.documentNumber || 0,
          creditorId,
          buildingId: Number(buildingId || 0),
          dataVencimento: item.dataVencimento || item.issueDate || item.dueDate || item.dataVencimentoProjetado || item.dataEmissao || item.dataContabil || "",
          descricao: item.descricao || item.historico || item.tipoDocumento || item.notes || item.observacao || "Titulo a Pagar",
          valor: Number(item.totalInvoiceAmount || item.valor || item.amount || item.valorTotal || item.valorLiquido || item.valorBruto || 0),
          situacao: item.situacao || item.status || "Pendente",
          creditorName: creditorMap.get(creditorId) || creditorNameHints.get(creditorId) || normalizeCreditorName(item, creditorId),
          nomeCredor: creditorMap.get(creditorId) || creditorNameHints.get(creditorId) || normalizeCreditorName(item, creditorId),
          nomeObra: buildingMap.get(buildingId)?.name || buildingNameHints.get(buildingId) || normalizeBuildingName(item, buildingId),
        };
      });

      const normalizedReceivable = receber.map((item: any) => {
        const buildingId = String(item.idObra || item.codigoObra || item.enterpriseId || "");
        return {
          id: item.id || item.numero || item.numeroTitulo || item.codigoTitulo || item.documentNumber || 0,
          buildingId: Number(buildingId || 0),
          dataVencimento: item.data || item.date || item.dataVencimento || item.dataEmissao || item.issueDate || item?.dataVencimentoProjetado || "",
          descricao: item.descricao || item.historico || item.observacao || item.notes || item.description || "Titulo a Receber",
          valor: Number(item.value || item.valor || item.valorSaldo || item.totalInvoiceAmount || item.valorTotal || item.amount || 0),
          situacao: String(item.situacao || item.status || "ABERTO").toUpperCase(),
          nomeCliente: item.nomeCliente || item.nomeFantasiaCliente || item.cliente || item.clientName || "Extrato/Cliente",
          nomeObra: buildingMap.get(buildingId)?.name || buildingNameHints.get(buildingId) || normalizeBuildingName(item, buildingId),
        };
      });

      res.json({
        latestSync: getLatestSyncInfo(),
        obras: Array.from(buildingMap.values()).map((obra: any) => ({
          id: obra.id,
          code: obra.code,
          name: obra.name,
          nome: obra.nome,
          address: obra.address,
          endereco: obra.endereco,
          latitude: obra.latitude,
          longitude: obra.longitude,
          companyId: obra.companyId,
          engineer: obra.engineer,
        })),
        usuarios: usuarios.map((user: any) => ({
          id: String(user.id || user.userId || user.username || ""),
          name: normalizePersonName(user),
          nome: normalizePersonName(user),
        })),
        credores: credores.map((credor: any) => ({
          id: credor.id,
          name: creditorMap.get(String(credor.id)) || normalizeCreditorName(credor, credor.id),
          nome: creditorMap.get(String(credor.id)) || normalizeCreditorName(credor, credor.id),
          cnpj: credor.cnpj || credor.cpfCnpj || "",
        })),
        companies: companies.map((company: any) => ({
          id: company.id,
          name: company.name || company.nome || company.companyName || `Empresa ${company.id}`,
          cnpj: company.cnpj || company.cpfCnpj || "",
        })),
        pedidos: normalizedOrders,
        financeiro: normalizedFinancial,
        receber: normalizedReceivable,
        itensPedidos: itensPayload,
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/sienge/itens-pedidos", async (_req, res) => {
    try {
      const cached = readDatasetCache("itens_pedidos") || await readFromFile("itens_pedidos.json");
      return res.json(cached || {});
    } catch (_error) {
      return res.status(500).json({ error: "Failed to read itens cache" });
    }
  });

  app.post("/api/sienge/fetch-items", async (req, res) => {
    try {
      const { ids } = req.body;
      if (!Array.isArray(ids)) return res.json({});
      
      let itemsMap: Record<number, any> = await readFromFile("itens_pedidos.json") || {};
      let changed = false;

      for (const id of ids) {
        if (!itemsMap[id]) {
          try {
            const result = await siengeAPI.get(`/public/api/v1/purchase-orders/${id}/items`);
            itemsMap[id] = result.data?.results || result.data || [];
            changed = true;
          } catch (e) {}
        }
      }
      
      if (changed) {
        await saveToFile("itens_pedidos.json", itemsMap);
      }
      return res.json(itemsMap);
    } catch (error: any) {
      res.status(500).json({ status: "error", error: error.message });
    }
  });

  app.get("/api/sienge/financeiro", async (req, res) => {
    try {
      const cached = readDatasetCache("financeiro") || await readFromFile("financeiro.json");
      if (cached && !req.query.force) return res.json(cached);
      
      const response = await siengeAPI.get(`${detectedPrefix}/bills`, { params: req.query });
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/financeiro/receber", async (req, res) => {
    try {
      const cached = readDatasetCache("receber") || await readFromFile("receber.json");
      if (cached && !req.query.force) return res.json(cached);

      const response = await siengeAPI.get(`${detectedPrefix}/accounts-statements`, { params: req.query });
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/notas-entrada", async (req, res) => {
    try {
      const response = await siengeAPI.get(`${detectedPrefix}/notas-fiscais-entrada`, { params: req.query });
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/itens-nota/:id", async (req, res) => {
    try {
      const { id } = req.params;
      const response = await siengeAPI.get(`${detectedPrefix}/notas-fiscais-entrada/${id}/itens`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/obras", async (req, res) => {
    try {
      const cached = await readFromFile("obras.json") || readDatasetCache("obras");
      const cachedByCode = await readFromFile("obras_by_code.json") || readDatasetCache("obras_by_code");
      const pedidosCache = await readFromFile("pedidos.json") || readDatasetCache("pedidos");
      const meta = await readObrasMeta();

      if (cached || cachedByCode) {
        const baseList = Array.isArray(cached?.results) ? cached.results : Array.isArray(cached) ? cached : [];
        const byCodeList = cachedByCode ? Object.values(cachedByCode) : [];
        const pedidosList = Array.isArray(pedidosCache?.results) ? pedidosCache.results : Array.isArray(pedidosCache) ? pedidosCache : [];
        const merged = new Map<string, any>();

        [...baseList, ...byCodeList].forEach((obra: any) => {
          const id = String(obra.id || obra.code || obra.codigoVisivel || '');
          if (!id) return;
          const current = merged.get(id) || {};
          const pedidoFallback = pedidosList.find((item: any) => String(item.codigoVisivelObra || item.idObra || item.buildingId || "") === id);
          const normalizedName = normalizeBuildingName(obra, id);
          const fallbackName = normalizeBuildingName(pedidoFallback, id);
          merged.set(id, {
            ...current,
            ...obra,
            id: obra.id || current.id || Number(id),
            code: obra.code || obra.codigoVisivel || current.code || String(id),
            name: normalizedName !== `Obra ${id}` && normalizedName !== "Obra sem nome" ? normalizedName : (fallbackName || current.name || current.nome || `Obra ${id}`),
            nome: normalizedName !== `Obra ${id}` && normalizedName !== "Obra sem nome" ? normalizedName : (fallbackName || current.nome || current.name || `Obra ${id}`),
            address: obra.address || obra.endereco || obra.adress || current.address || current.endereco || "",
            endereco: obra.endereco || obra.address || obra.adress || current.endereco || current.address || "",
            engineer: meta[id]?.engineer || obra.engineer || obra.responsavelTecnico || obra.engenheiro || current.engineer || ""
          });
        });

        return res.json({ results: Array.from(merged.values()) });
      }

      const response = await siengeAPI.get(`${detectedPrefix}/enterprises`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.post("/api/sienge/obras/meta", async (req, res) => {
    try {
      const { id, engineer } = req.body || {};
      if (!id) {
        return res.status(400).json({ error: "Id da obra é obrigatório." });
      }

      const meta = await readObrasMeta();
      meta[String(id)] = {
        ...(meta[String(id)] || {}),
        engineer: String(engineer || "").trim()
      };

      await saveObrasMeta(meta);
      saveBuildingMetaToDb(String(id), String(engineer || "").trim());
      return res.json({ success: true, meta: meta[String(id)] });
    } catch (error: any) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.get("/api/sienge/usuarios", async (req, res) => {
    try {
      const cached = readDatasetCache("usuarios") || await readFromFile("usuarios.json");
      if (cached) return res.json(cached);

      const response = await siengeAPI.get(`${detectedPrefix}/users`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/credores", async (req, res) => {
    try {
      const cached = await readFromFile("credores.json") || readDatasetCache("credores");
      if (cached) {
        const list = Array.isArray(cached?.results) ? cached.results : Array.isArray(cached) ? cached : [];
        return res.json({
          results: list.map((credor: any) => ({
            ...credor,
            id: credor.id,
            code: String(credor.code || credor.id || ""),
            name: resolveCreditorNameFromCaches(String(credor.id), normalizeCreditorName(credor, credor.id)),
            nome: resolveCreditorNameFromCaches(String(credor.id), normalizeCreditorName(credor, credor.id))
          }))
        });
      }

      const response = await siengeAPI.get(`${detectedPrefix}/creditors`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/companies", async (req, res) => {
    try {
      const cached = readDatasetCache("empresas") || await readFromFile("empresas.json");
      if (cached) return res.json(cached);

      const response = await siengeAPI.get(`${detectedPrefix}/companies`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/clientes", async (req, res) => {
    try {
      const cached = readDatasetCache("clientes") || await readFromFile("clientes.json");
      if (cached) return res.json(cached);

      const response = await siengeAPI.get(`${detectedPrefix}/clientes`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/pedidos-compra", async (req, res) => {
    try {
      const cached = readDatasetCache("pedidos") || await readFromFile("pedidos.json");
      if (cached && !req.query.force) return res.json(cached);

      const response = await siengeAPI.get(`${detectedPrefix}/purchase-orders`, { params: req.query });
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/pedidos-compra/:id/itens", async (req, res) => {
    try {
      const { id } = req.params;
      
      // Tentar buscar do cache de itens consolidado
      const cachedItems = readDatasetCache("itens_pedidos") || await readFromFile("itens_pedidos.json");
      if (cachedItems && cachedItems[id]) {
        return res.json(cachedItems[id]);
      }

      const response = await siengeAPI.get(`${detectedPrefix}/purchase-orders/${id}/items`);
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/extrato", async (req, res) => {
    try {
      const response = await siengeAPI.get(`${detectedPrefix}/extratos-bancarios`, { params: req.query });
      res.json(response.data);
    } catch (error: any) {
      res.status(error.response?.status || 500).json(error.response?.data || error.message);
    }
  });

  app.get("/api/sienge/logistics/locations", (_req, res) => {
    const rows = db.prepare(`
      SELECT id, code, name, address, latitude, longitude, type, source
      FROM app_locations
      ORDER BY
        CASE WHEN type = 'hq' THEN 0 WHEN type = 'building' THEN 1 ELSE 2 END,
        name COLLATE NOCASE ASC
    `).all();
    res.json({ results: rows });
  });

  app.post("/api/sienge/logistics/route-distance", async (req, res) => {
    try {
      const origin = req.body?.origin || {};
      const destination = req.body?.destination || {};
      console.log("[Route] Request recebida", { origin, destination });

      if (!origin?.address && (origin?.latitude == null || origin?.longitude == null)) {
        return res.status(400).json({ error: "Origem da rota é obrigatória." });
      }

      if (!destination?.address && (destination?.latitude == null || destination?.longitude == null)) {
        return res.status(400).json({ error: "Destino da rota é obrigatório." });
      }

      const result = await calculateRouteDistance(origin, destination);
      console.log("[Route] Resultado final", result);
      return res.json(result);
    } catch (error: any) {
      return res.status(500).json({ error: error.message || "Falha ao calcular rota." });
    }
  });

  app.post("/api/sienge/logistics/locations", (req, res) => {
    try {
      const body = req.body || {};
      const name = String(body.name || "").trim();
      if (!name) {
        return res.status(400).json({ error: "Nome do local é obrigatório." });
      }

      const now = new Date().toISOString();
      const code = String(body.code || `custom-${Date.now()}`).trim();
      db.prepare(`
        INSERT INTO app_locations (code, name, address, latitude, longitude, type, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
          name = excluded.name,
          address = excluded.address,
          latitude = excluded.latitude,
          longitude = excluded.longitude,
          type = excluded.type,
          source = excluded.source,
          updated_at = excluded.updated_at
      `).run(
        code,
        name,
        String(body.address || "").trim(),
        body.latitude ?? null,
        body.longitude ?? null,
        String(body.type || "custom"),
        String(body.source || "manual"),
        now,
        now
      );

      const inserted = db.prepare(`
        SELECT id, code, name, address, latitude, longitude, type, source
        FROM app_locations
        WHERE code = ?
      `).get(code);
      return res.json({ success: true, location: inserted });
    } catch (error: any) {
      return res.status(500).json({ error: error.message });
    }
  });

  app.post("/api/auth/login", (req, res) => {
    try {
      const email = String(req.body?.email || "").trim().toLowerCase();
      const password = String(req.body?.password || "");

      if (!email || !password) {
        return res.status(400).json({ error: "Email e senha são obrigatórios." });
      }

      const user = db.prepare(`
        SELECT id, username, email, password_hash, name, role, department, active
        FROM app_users
        WHERE lower(email) = ? OR lower(username) = ?
        LIMIT 1
      `).get(email, email) as
        | { id: number; username: string; email: string; password_hash: string; name: string; role: string; department: string; active: number }
        | undefined;

      if (!user || !user.active || user.password_hash !== hashPassword(password)) {
        return res.status(401).json({ error: "Credenciais inválidas." });
      }

      return res.json({
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          name: user.name,
          role: user.role,
          department: user.department || "",
        },
      });
    } catch (error: any) {
      return res.status(500).json({ error: error.message || "Falha ao autenticar." });
    }
  });

  app.post("/api/auth/register", (req, res) => {
    try {
      const name = String(req.body?.name || "").trim();
      const email = String(req.body?.email || "").trim().toLowerCase();
      const department = String(req.body?.department || "").trim();
      const roleInput = String(req.body?.role || "").trim().toLowerCase();
      const role = roleInput === "developer" || roleInput === "admin" || roleInput === "user" ? roleInput : "";

      if (!name || !email || !department || !role) {
        return res.status(400).json({ error: "Nome, email, setor e perfil são obrigatórios." });
      }

      const exists = db.prepare(`
        SELECT id FROM app_users WHERE lower(email) = ? OR lower(username) = ? LIMIT 1
      `).get(email, email) as { id: number } | undefined;

      if (exists) {
        return res.status(409).json({ error: "Já existe um usuário cadastrado com este email." });
      }

      const now = new Date().toISOString();
      const tempPassword = "123456";
      const result = db.prepare(`
        INSERT INTO app_users (username, email, password_hash, name, role, department, active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        email,
        email,
        hashPassword(tempPassword),
        name,
        role,
        department,
        1,
        now,
        now
      );

      return res.json({
        success: true,
        tempPassword,
        user: {
          id: Number(result.lastInsertRowid),
          username: email,
          email,
          name,
          role,
          department,
        },
      });
    } catch (error: any) {
      return res.status(500).json({ error: error.message || "Falha ao cadastrar usuário." });
    }
  });

  // --- CONFIGURAÇÃO VITE / FRONTEND ---
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => res.sendFile(path.join(distPath, "index.html")));
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`
    ✅ Servidor rodando!
    🚀 Local: http://localhost:${PORT}
    🔗 API Sienge: ${SIENGE_BASE_URL}${detectedPrefix}
    ⚙️ Atualização Automática: ON (A cada 20 min)
    `);

    const autoSyncOnBoot = process.env.AUTO_SYNC_ON_BOOT === "true";
    const autoSyncInterval = process.env.AUTO_SYNC_INTERVAL !== "false";
    const isProduction = process.env.NODE_ENV === "production";

    if (autoSyncOnBoot || isProduction) {
      setTimeout(() => {
        syncAllData().catch(e => console.log(e));
      }, 5000);
    } else {
      console.log("[Sync] Auto-sync inicial desativado no ambiente atual. Use o botão 'Sincronizar' para iniciar quando quiser.");
    }

    if (autoSyncInterval || isProduction) {
      setInterval(() => {
        syncAllData().catch(e => console.log(e));
      }, 20 * 60 * 1000);
    }
  });
}

startServer();
