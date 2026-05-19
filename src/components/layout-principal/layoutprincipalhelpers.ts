export const MAPA_ROTAS_POR_ABA: Record<string, string> = {
  dashboard: '/',
  finance: '/financeiro',
  map: '/obras/mapa',
  'obras-alerta': '/obras/alerta',
  logistics: '/logistica',
  'juridico-contratos': '/juridico/contratos',
  access: '/acessos',
  'obras-diario': '/obras/diario',
  'financeiro-fluxo': '/financeiro/fluxo',
  'financeiro-centro-custo': '/financeiro/centro-custo',
};

export function obterAbaAtivaPorRota(path: string): string {
  if (path.startsWith('/financeiro')) return 'finance';
  if (path.startsWith('/logistica')) return 'logistics';
  if (path.startsWith('/juridico')) return 'juridico-contratos';
  if (path.startsWith('/acessos')) return 'access';
  if (path.startsWith('/obras/alerta')) return 'obras-alerta';
  if (path.startsWith('/obras')) return 'map';
  return 'dashboard';
}

export function obterOpcoesObraPorEmpresa(buildings: any[], selectedCompany: string): any[] {
  if (selectedCompany === 'all') return buildings || [];
  return (buildings || []).filter((b: any) => {
    const companyId = b?.companyId ?? b?.company_id;
    return String(companyId) === String(selectedCompany);
  });
}

export function filtrarListaPorTexto(items: any[], search: string, campos: string[]): any[] {
  const term = String(search || '').trim().toLowerCase();
  if (!term) return items || [];
  return (items || []).filter((item: any) => {
    return campos.some((campo) => String(item?.[campo] || '').toLowerCase().includes(term));
  });
}
