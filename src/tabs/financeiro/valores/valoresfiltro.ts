type FuncaoAtraso = (titulo: any) => boolean;
type FuncaoLiquidado = (status: unknown) => boolean;

export interface GruposTitulosValores {
  openPayables: any[];
  openReceivables: any[];
  paidPayables: any[];
  overdueReceivables: any[];
  overduePayables: any[];
}

export function construirGruposTitulosValores({
  financialTitles,
  receivableTitles,
  isOverdue,
  isSettledFinancialStatus,
}: {
  financialTitles: any[];
  receivableTitles: any[];
  isOverdue: FuncaoAtraso;
  isSettledFinancialStatus: FuncaoLiquidado;
}): GruposTitulosValores {
  const openPayables = financialTitles
    .filter((t) => !isSettledFinancialStatus(t.status))
    .sort((a, b) => (a.dueDateNumeric || 0) - (b.dueDateNumeric || 0));

  const openReceivables = receivableTitles
    .filter((t) => !isSettledFinancialStatus(t.status))
    .sort((a, b) => (a.dueDateNumeric || 0) - (b.dueDateNumeric || 0));

  const paidPayables = financialTitles
    .filter((t) => isSettledFinancialStatus(t.status))
    .sort((a, b) => (b.paymentDateNumeric || b.dueDateNumeric || 0) - (a.paymentDateNumeric || a.dueDateNumeric || 0));

  const overdueReceivables = openReceivables.filter(isOverdue);
  const overduePayables = openPayables.filter(isOverdue);

  return {
    openPayables,
    openReceivables,
    paidPayables,
    overdueReceivables,
    overduePayables,
  };
}
