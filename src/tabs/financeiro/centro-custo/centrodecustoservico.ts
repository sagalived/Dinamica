import { api } from '../../../lib/api';

export async function carregarDadosBrutosCentroDeCusto(): Promise<{
  contasPagar: any[];
  contasReceber: any[];
  titulosReceber: any[];
}> {
  const [cp, cr, tr] = await Promise.all([
    api.get('/sienge-raw/contas-pagar-empresas'),
    api.get('/sienge-raw/contas-receber-empresas'),
    api.get('/sienge-raw/titulos-receber-empresas'),
  ]);

  return {
    contasPagar: Array.isArray(cp?.data?.rows) ? cp.data.rows : [],
    contasReceber: Array.isArray(cr?.data?.rows) ? cr.data.rows : [],
    titulosReceber: Array.isArray(tr?.data?.rows) ? tr.data.rows : [],
  };
}
