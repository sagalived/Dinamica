import { apiGet, apiPost } from "./apiService";

export async function fetchDashboardBootstrap() {
  const bootstrap = await apiGet("/sienge/bootstrap");
  const obras = bootstrap?.obras ?? bootstrap?.buildings ?? bootstrap?.enterprises ?? [];
  const empresas = bootstrap?.empresas ?? bootstrap?.companies ?? [];

  return {
    buildings: Array.isArray(obras) ? obras : Object.values(obras),
    companies: Array.isArray(empresas) ? empresas : Object.values(empresas),
  };
}

export async function fetchDashboardFiltered({ year, companyId, buildingId }) {
  const startDate = `${year}-01-01`;
  const endDate = `${year}-12-31`;

  const params = { start_date: startDate, end_date: endDate };
  if (companyId && companyId !== "Todos") params.company_id = companyId;
  if (buildingId && buildingId !== "Todos") params.building_id = buildingId;

  return apiGet("/sienge/filtered", params);
}

export async function requestDashboardSync() {
  return apiPost("/sienge/sync");
}
