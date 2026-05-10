export const MAPA_ROTAS_POR_ABA: Record<string, string> = {
  dashboard: '/',
  'dashboard-financeiro': '/dashboard/financeiro',
  finance: '/financeiro',
  alerts: '/financeiro/alerta',
  map: '/obras/mapa',
  logistics: '/logistica',
  access: '/acessos',
  'obras-diario': '/obras/diario',
  'financeiro-fluxo': '/financeiro/fluxo',
  'financeiro-leandro': '/financeiro/leandro',
  'financeiro-centro-custo': '/financeiro/centro-custo',
};

export function obterAbaAtivaPorRota(path: string): string {
  if (path.startsWith('/dashboard/financeiro')) return 'dashboard-financeiro';
  if (path.startsWith('/financeiro')) return 'finance';
  if (path.startsWith('/logistica')) return 'logistics';
  if (path.startsWith('/acessos')) return 'access';
  if (path.startsWith('/obras')) return 'map';
  return 'dashboard';
}

export function obterOpcoesObraPorEmpresa(buildings: any[], selectedCompany: string): any[] {
  if (selectedCompany === 'all') return buildings || [];
  return (buildings || []).filter((b: any) => String(b?.companyId) === String(selectedCompany));
}

export function filtrarListaPorTexto(items: any[], search: string, campos: string[]): any[] {
  const term = String(search || '').trim().toLowerCase();
  if (!term) return items || [];
  return (items || []).filter((item: any) => {
    return campos.some((campo) => String(item?.[campo] || '').toLowerCase().includes(term));
  });
}
