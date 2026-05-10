export function formatarMoedaBR(valor: number): string {
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number.isFinite(valor) ? valor : 0);
}

export function formatarNumeroBR(valor: number, casasDecimais = 2): string {
  return Number(valor || 0).toLocaleString('pt-BR', {
    minimumFractionDigits: casasDecimais,
    maximumFractionDigits: casasDecimais,
  });
}

export function formatarQuantidadeBR(valor: number): string {
  return Number(valor || 0).toLocaleString('pt-BR');
}
