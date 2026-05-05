import {
  buildMonthlyBase,
  computeBuildingMargins,
  computeMonthlyMetrics,
  computeTotals,
  aggregateOperationalByMonth,
} from "../models/dashboardModel";
import { fetchDashboardBootstrap, fetchDashboardFiltered } from "../services/dashboardService";

export async function loadDashboardBootstrap() {
  return fetchDashboardBootstrap();
}

export async function loadDashboardData(filters) {
  const filtered = await fetchDashboardFiltered(filters);
  const financeiro = Array.isArray(filtered?.financeiro) ? filtered.financeiro : [];
  const receber = Array.isArray(filtered?.receber) ? filtered.receber : [];

  const monthlyBase = aggregateOperationalByMonth({
    financeiro,
    receber,
    companyId: filters.companyId,
    buildingId: filters.buildingId,
  });

  const buildingMargins = computeBuildingMargins({
    financeiro,
    receber,
    companyId: filters.companyId,
    buildingId: filters.buildingId,
  });

  return {
    monthly: computeMonthlyMetrics(monthlyBase),
    buildingMargins,
  };
}

export function createEmptyDashboardData() {
  return buildMonthlyBase();
}

export function calculateDashboardTotals(data) {
  return computeTotals(data);
}
