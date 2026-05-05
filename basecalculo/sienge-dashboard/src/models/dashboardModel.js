export const MONTHS = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];

export function formatCurrency(value) {
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  }).format(value || 0);
}

export function formatPercent(value) {
  return `${(value || 0).toFixed(1)}%`;
}

export function buildMonthlyBase() {
  return MONTHS.map((month, monthIndex) => ({
    month,
    monthIndex,
    receita: 0,
    custoVariavel: 0,
    margem: 0,
    mcPercent: 0,
  }));
}

function normalizeText(value) {
  return String(value || "").toLowerCase();
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function getRowDate(row) {
  return row.dataVencimento ?? row.date ?? row.operationDate ?? row.paymentDate ?? row.dueDate ?? row.issueDate;
}

function getRowAmount(row) {
  return Math.abs(Number(row.valor ?? row.rawValue ?? row.amount ?? row.value ?? row.netAmount ?? 0));
}

function getRowBuildingId(row) {
  return toNumber(row.idObra ?? row.buildingId);
}

function matchesCompany(row, companyId) {
  if (!companyId || companyId === "Todos") return true;
  const rowCompany = toNumber(row.companyId);
  const selectedCompany = toNumber(companyId);
  return rowCompany !== null && selectedCompany !== null && rowCompany === selectedCompany;
}

function matchesBuilding(row, buildingId) {
  if (!buildingId || buildingId === "Todos") return true;
  const rowBuilding = getRowBuildingId(row);
  const selectedBuilding = toNumber(buildingId);
  return rowBuilding !== null && selectedBuilding !== null && rowBuilding === selectedBuilding;
}

export function classifyOperationalFinance(row) {
  const type = normalizeText(row.type ?? row.tipoLancamento);
  const statementType = normalizeText(row.statementType ?? row.operationType);
  const origin = normalizeText(row.statementOrigin ?? row.origin);
  const nature = normalizeText(row.nature ?? row.natureza ?? row.accountNature);

  if (
    statementType.includes("transf") ||
    statementType.includes("transfer") ||
    statementType.includes("saque") ||
    origin === "bc"
  ) {
    return "ignore";
  }

  if (
    type.includes("income") ||
    type.includes("entrada") ||
    statementType.includes("receb") ||
    nature.includes("receita")
  ) {
    return "receita";
  }

  if (
    type.includes("expense") ||
    type.includes("saida") ||
    statementType.includes("pagamento") ||
    statementType.includes("despesa") ||
    nature.includes("custo")
  ) {
    return "custo";
  }

  return "ignore";
}

export function aggregateFinanceByMonth(financeiro) {
  const monthly = buildMonthlyBase();

  (Array.isArray(financeiro) ? financeiro : []).forEach((row) => {
    const dateStr = row.date ?? row.operationDate ?? row.paymentDate ?? row.dueDate ?? row.issueDate;
    if (!dateStr) return;

    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) return;

    const monthIndex = date.getMonth();
    const amount = Math.abs(Number(row.amount ?? row.value ?? row.netAmount ?? 0));
    if (amount <= 0) return;

    const classification = classifyOperationalFinance(row);
    if (classification === "receita") monthly[monthIndex].receita += amount;
    if (classification === "custo") monthly[monthIndex].custoVariavel += amount;
  });

  return monthly;
}

function isRevenueRow(row, defaultRevenue) {
  const classification = classifyOperationalFinance(row);
  if (classification === "receita") return true;
  if (classification === "custo") return false;
  return defaultRevenue;
}

export function aggregateOperationalByMonth({ financeiro, receber, companyId, buildingId }) {
  const monthly = buildMonthlyBase();

  (Array.isArray(receber) ? receber : []).forEach((row) => {
    if (!matchesCompany(row, companyId) || !matchesBuilding(row, buildingId)) return;
    const dateStr = getRowDate(row);
    if (!dateStr) return;
    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) return;

    const amount = getRowAmount(row);
    if (amount <= 0) return;

    const monthIndex = date.getMonth();
    if (isRevenueRow(row, true)) {
      monthly[monthIndex].receita += amount;
    } else {
      monthly[monthIndex].custoVariavel += amount;
    }
  });

  (Array.isArray(financeiro) ? financeiro : []).forEach((row) => {
    if (!matchesCompany(row, companyId) || !matchesBuilding(row, buildingId)) return;
    const dateStr = getRowDate(row);
    if (!dateStr) return;
    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) return;

    const amount = getRowAmount(row);
    if (amount <= 0) return;

    const monthIndex = date.getMonth();
    if (isRevenueRow(row, false)) {
      monthly[monthIndex].receita += amount;
    } else {
      monthly[monthIndex].custoVariavel += amount;
    }
  });

  return monthly;
}

export function computeBuildingMargins({ financeiro, receber, companyId, buildingId }) {
  const byBuilding = new Map();

  function ensureBuilding(row) {
    const buildingKey = String(getRowBuildingId(row) ?? 0);
    if (!byBuilding.has(buildingKey)) {
      byBuilding.set(buildingKey, {
        buildingId: buildingKey,
        buildingName: row.nomeObra || "Sem obra",
        receita: 0,
        custoVariavel: 0,
        margem: 0,
        mcPercent: 0,
      });
    }
    return byBuilding.get(buildingKey);
  }

  (Array.isArray(receber) ? receber : []).forEach((row) => {
    if (!matchesCompany(row, companyId) || !matchesBuilding(row, buildingId)) return;
    const amount = getRowAmount(row);
    if (amount <= 0) return;
    const target = ensureBuilding(row);
    if (isRevenueRow(row, true)) target.receita += amount;
    else target.custoVariavel += amount;
  });

  (Array.isArray(financeiro) ? financeiro : []).forEach((row) => {
    if (!matchesCompany(row, companyId) || !matchesBuilding(row, buildingId)) return;
    const amount = getRowAmount(row);
    if (amount <= 0) return;
    const target = ensureBuilding(row);
    if (isRevenueRow(row, false)) target.receita += amount;
    else target.custoVariavel += amount;
  });

  return Array.from(byBuilding.values())
    .map((row) => {
      const margem = row.receita - row.custoVariavel;
      const mcPercent = row.receita > 0 ? (margem / row.receita) * 100 : 0;
      return { ...row, margem, mcPercent };
    })
    .sort((a, b) => b.receita - a.receita);
}

export function computeMonthlyMetrics(monthly) {
  return monthly.map((row) => {
    const margem = row.receita - row.custoVariavel;
    const mcPercent = row.receita > 0 ? (margem / row.receita) * 100 : 0;
    return { ...row, margem, mcPercent };
  });
}

export function computeTotals(data) {
  const receita = data.reduce((sum, row) => sum + row.receita, 0);
  const margem = data.reduce((sum, row) => sum + row.margem, 0);
  const mcPercent = receita > 0 ? (margem / receita) * 100 : 0;
  return { receita, margem, mcPercent };
}

export function bestMonth(data, key) {
  return data.reduce((best, row) => (row[key] > best[key] ? row : best), data[0] || {});
}
