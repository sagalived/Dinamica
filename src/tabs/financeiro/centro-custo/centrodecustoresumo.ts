import { LinhaCentroDeCusto } from './centrodecustonormalizador';

export interface ResumoCentroDeCusto {
  totalCustoDireto: number;
  totalCustoIndireto: number;
  totalCusto: number;
  totalReceita: number;
  totalMargem: number;
  margemPercentualGeral: number;
}

export function calcularResumoCentroDeCusto(linhas: LinhaCentroDeCusto[]): ResumoCentroDeCusto {
  const totalCustoDireto = linhas.reduce((soma, linha) => soma + linha.custoDireto, 0);
  const totalCustoIndireto = linhas.reduce((soma, linha) => soma + linha.custoIndireto, 0);
  const totalCusto = linhas.reduce((soma, linha) => soma + linha.custoTotal, 0);
  const totalReceita = linhas.reduce((soma, linha) => soma + linha.receita, 0);
  const totalMargem = linhas.reduce((soma, linha) => soma + linha.margem, 0);
  const margemPercentualGeral = totalReceita > 0 ? (totalMargem / totalReceita) * 100 : 0;

  return {
    totalCustoDireto,
    totalCustoIndireto,
    totalCusto,
    totalReceita,
    totalMargem,
    margemPercentualGeral,
  };
}
