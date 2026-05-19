import { api as baseApi } from '../../lib/api';

export type CatalogBuilding = {
  id: number;
  name: string;
  company_id?: number | null;
  company_name?: string | null;
  cnpj?: string | null;
  address?: string | null;
  building_type?: string | null;
  active: boolean;
};

export type CatalogCompany = {
  id: number;
  name: string;
  trade_name?: string | null;
  cnpj?: string | null;
};

export async function listCatalogBuildings(params?: { active?: 'all' | 'true' | 'false' }): Promise<CatalogBuilding[]> {
  const res = await baseApi.get('/buildings', { params: { active: params?.active || 'true' } });
  return Array.isArray(res.data) ? res.data : [];
}

export async function listCatalogCompanies(): Promise<CatalogCompany[]> {
  const res = await baseApi.get('/companies');
  return Array.isArray(res.data) ? res.data : [];
}

export async function createCatalogBuilding(payload: {
  name: string;
  companyId?: number | null;
  address?: string | null;
  buildingType?: string | null;
  active?: boolean;
}): Promise<CatalogBuilding> {
  const res = await baseApi.post('/buildings', {
    name: payload.name,
    companyId: payload.companyId ?? null,
    address: payload.address ?? null,
    buildingType: payload.buildingType ?? null,
    active: payload.active ?? true,
  });
  return res.data?.building;
}
