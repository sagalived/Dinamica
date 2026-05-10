export interface LinhaCentroDeCusto {
  id: string;
  companyId?: string;
  buildingId?: string;
  codigoObra: string;
  nomeObra: string;
  empresa: string;
  custoDireto: number;
  custoIndireto: number;
  custoTotal: number;
  receita: number;
  margem: number;
  margemPercentual: number;
  dataAtualizacao: string;
}

function paraNumero(valor: unknown): number {
  if (typeof valor === 'number') return Number.isFinite(valor) ? valor : 0;
  if (typeof valor === 'string') {
    const normalizado = valor.replace('.', '').replace(',', '.');
    const convertido = Number(normalizado);
    return Number.isFinite(convertido) ? convertido : 0;
  }
  return 0;
}

function paraLista(payload: unknown): any[] {
  if (Array.isArray(payload)) return payload;
  if (payload && typeof payload === 'object' && Array.isArray((payload as any).financeiro)) {
    return (payload as any).financeiro;
  }
  return [];
}

export function normalizarCentroDeCusto(payload: unknown): LinhaCentroDeCusto[] {
  return paraLista(payload)
    .filter((item) => item && typeof item === 'object')
    .map((item: any) => ({
      id: String(item.id ?? ''),
      companyId: item.companyId != null ? String(item.companyId) : undefined,
      buildingId: item.buildingId != null ? String(item.buildingId) : undefined,
      codigoObra: String(item.codigoObra ?? item.buildingId ?? ''),
      nomeObra: String(item.nomeObra ?? item.descricao ?? ''),
      empresa: String(item.empresa ?? ''),
      custoDireto: paraNumero(item.custoDireto),
      custoIndireto: paraNumero(item.custoIndireto),
      custoTotal: paraNumero(item.custoTotal ?? item.valor),
      receita: paraNumero(item.receita),
      margem: paraNumero(item.margem),
      margemPercentual: paraNumero(item.margemPercentual),
      dataAtualizacao: String(item.dataAtualizacao ?? item.dataVencimento ?? ''),
    }))
    .filter((linha) => linha.id !== '' || linha.codigoObra !== '' || linha.nomeObra !== '');
}
