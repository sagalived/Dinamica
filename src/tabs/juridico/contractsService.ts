import { api as baseApi } from '../../lib/api';

export type ContractPayload = {
  buildingId: number;
  contractNumber?: string | null;
  title: string;

  kind?: 'suprimentos' | 'medicoes' | 'privado' | 'licitacao' | string;

  supplierLegalName?: string | null;
  supplierCnpj?: string | null;
  supplierAddress?: string | null;
  supplierRepresentatives?: string | null;

  ownerLegalName?: string | null;
  ownerCnpj?: string | null;
  ownerAddress?: string | null;
  ownerRepresentatives?: string | null;

  objectText?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  totalValue?: number | null;
  retentionPercent?: number | null;
  guaranteeText?: string | null;
  insuranceText?: string | null;
  status?: string;
  notes?: string | null;

  costCenterCode?: string | null;
  stageLabel?: string | null;
  enterpriseLabel?: string | null;
};

export async function listContracts(params?: { buildingId?: number; q?: string; kind?: string }): Promise<any[]> {
  const res = await baseApi.get('/juridico/contratos', {
    params: {
      building_id: params?.buildingId,
      q: params?.q,
      kind: params?.kind,
    },
  });
  return res.data?.contracts || [];
}

export async function syncContractsFromSiengeLive(): Promise<any> {
  const res = await baseApi.post('/juridico/contratos/sync-sienge-live');
  return res.data;
}

export async function createContract(payload: ContractPayload): Promise<any> {
  const res = await baseApi.post('/juridico/contratos', payload);
  return res.data?.contract;
}

export async function updateContract(contractId: number, payload: ContractPayload): Promise<any> {
  const res = await baseApi.patch(`/juridico/contratos/${contractId}`, payload);
  return res.data?.contract;
}

export async function deleteContract(contractId: number): Promise<void> {
  await baseApi.delete(`/juridico/contratos/${contractId}`);
}

export async function listContractDocuments(contractId: number): Promise<any[]> {
  const res = await baseApi.get(`/juridico/contratos/${contractId}/documents`);
  return res.data?.documents || [];
}

export async function uploadContractDocument(contractId: number, file: File, params: { docType: string; title?: string }): Promise<any> {
  const form = new FormData();
  form.append('file', file);
  const res = await baseApi.post(`/juridico/contratos/${contractId}/upload`, form, {
    params: { doc_type: params.docType, title: params.title || undefined },
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return res.data?.document;
}

export async function deleteContractDocument(contractId: number, docId: number): Promise<void> {
  await baseApi.delete(`/juridico/contratos/${contractId}/documents/${docId}`);
}

export async function getPrintableContractHtml(contractId: number): Promise<string> {
  const res = await baseApi.get(`/juridico/contratos/${contractId}/print`);
  return String(res.data?.html || '');
}
