export interface CentroDeCustoRow {
  id?: string | number;
  data: string;
  baixa?: string;
  vencto?: string;
  companyId?: string;
  historico: string;
  clienteFornecedor?: string;
  tituloParcela?: string;
  documento?: string;
  obra: string;
  obraId?: string;
  entrada: number;
  saida: number;
  saldo: number;
}

export interface CentroDeCustoPeriod {
  mes: string;
  entradas: number;
  saidas: number;
  saldo: number;
}
