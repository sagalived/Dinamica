import type { GruposTitulosValores } from './valoresfiltro';

export interface ResumoFinanceiroValores {
  total: number;
  avg: number;
  fTotal: number;
  rTotal: number;
  balance: number;
}

export function calcularResumoFinanceiroValores({
  orders,
  financialTitles,
  receivableTitles,
  toMoney,
}: {
  orders: any[];
  financialTitles: any[];
  receivableTitles: any[];
  toMoney: (value: unknown) => number;
}): ResumoFinanceiroValores {
  const total = orders.reduce((acc, curr) => acc + toMoney(curr.totalAmount), 0);
  const avg = orders.length > 0 ? total / orders.length : 0;

  const fTotal = financialTitles.reduce((acc, curr) => acc + toMoney(curr.amount), 0);
  const rTotal = receivableTitles.reduce((acc, curr) => acc + toMoney(curr.amount), 0);
  const balance = rTotal - fTotal;

  return { total, avg, fTotal, rTotal, balance };
}

export function calcularTotaisDosGruposValores(grupos: GruposTitulosValores) {
  const totalPayable = grupos.openPayables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
  const totalReceivable = grupos.openReceivables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
  const overdueReceivableTotal = grupos.overdueReceivables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
  const overduePayableTotal = grupos.overduePayables.reduce((acc, curr) => acc + (curr.amount || 0), 0);

  return {
    totalPayable,
    totalReceivable,
    overdueReceivableTotal,
    overduePayableTotal,
  };
}

export function calcularResumoDreValores(stats: ResumoFinanceiroValores) {
  const rol = stats.rTotal;
  const receitaBruta = rol / 0.8836;
  const deducoes = receitaBruta - rol;
  const despesasTotais = stats.fTotal;

  const maoDeObra = despesasTotais * 0.215;
  const materiais = despesasTotais * 0.557;
  const servicos = despesasTotais * 0.105;
  const cspTotal = maoDeObra + materiais + servicos;

  const despGerais = despesasTotais * 0.051;
  const despTributarias = despesasTotais * 0.003;
  const preLabore = despesasTotais * 0.046;
  const despOperacionaisTotal = despGerais + despTributarias + preLabore;

  const despFinanceiras = despesasTotais * 0.022;
  const irCsll = despesasTotais * 0.001;

  const resultadoBruto = rol - cspTotal;
  const resultadoOperacional = resultadoBruto - despOperacionaisTotal - despFinanceiras;
  const resultadoLiquido = resultadoOperacional - irCsll;

  return {
    receitaBruta,
    deducoes,
    rol,
    custos: { maoDeObra, materiais, servicos, total: cspTotal },
    resultadoBruto,
    despesas: { gerais: despGerais, tributarias: despTributarias, preLabore, total: despOperacionaisTotal },
    despFinanceiras,
    irCsll,
    resultadoLiquido,
  };
}
