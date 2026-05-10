export interface ResumoFluxoCaixa {
  totalEntradas: number;
  totalSaidas: number;
  totalRegistros: number;
  saldoFinal: number;
}

export function calcularResumoFluxoCaixa({
  rows,
  saldoAnterior,
}: {
  rows: any[];
  saldoAnterior: number;
}): ResumoFluxoCaixa {
  const totalEntradas = rows.reduce((acc, row) => acc + Number(row?.entrada || 0), 0);
  const totalSaidas = rows.reduce((acc, row) => acc + Number(row?.saida || 0), 0);
  const totalRegistros = rows.length;
  const saldoFinal = rows.length > 0 ? Number(rows[rows.length - 1]?.saldo || 0) : saldoAnterior;

  return {
    totalEntradas,
    totalSaidas,
    totalRegistros,
    saldoFinal,
  };
}
