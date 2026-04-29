/**
 * FLUXO DE CAIXA — Lógica de dados ISOLADA
 *
 * Fórmula base (replicando o relatório SIENGE PDF):
 *   - Fonte: accounts-statements (campo receber/allReceivableTitles)
 *   - Data usada: campo `date` / `dataVencimento` (data da movimentação)
 *   - Entradas: type === 'Income', valor positivo
 *   - Saídas:  type === 'Expense', valor positivo (e negativo se estorno)
 *   - GE (gerencial/transferências internas): incluídos no total mas com valor 0 no cache
 *   - Contas internas: filtradas via bankAccountCode quando fcHideInternal=true
 *   - Referência PDF: Empresa 1 / Mar 2026 → Entradas 2.029.313,12 / Saídas 2.012.460,49
 *
 * ATENÇÃO: não editar a lógica de outras abas aqui. Apenas FluxoCaixa.
 */

import { format } from 'date-fns'

// ─── Tipos ────────────────────────────────────────────────────────────────────

export interface FluxoCaixaRow {
  id: number | string
  data: string
  dataNumeric: number
  titParc: string
  documento: string
  origem: string
  statementType: string
  bankAccount: string
  pessoa: string
  entrada: number   // positivo = entrada real, negativo = estorno de entrada
  saida: number     // sempre positivo (saída real)
  saldo: number     // saldo acumulado calculado em série
}

export interface FluxoCaixaSummary {
  totalEntradas: number
  totalSaidas: number
  saldoAnterior: number   // saldo acumulado ANTES do período selecionado
  saldoFinal: number      // saldo ao final do último lançamento do período
  registros: number
}

export interface FluxoCaixaResult {
  rows: FluxoCaixaRow[]
  saldoAnterior: number   // saldo acumulado antes do início do período
}

// ─── Contas internas (ocultar quando fcHideInternal=true) ─────────────────────
// ATENÇÃO: bankAccountCode deve ser extraído dos links da API (ver normalization).
// Padrão SIENGE: contas de mutuo, repropriação financeira, uso pontual interno.
export const INTERNAL_BANK_ACCOUNTS = new Set([
  'MUTUODINAM',
  'REAPROPFIN',
  'ITAUPJ',
])

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Extrai code do bankAccount a partir de link rel="bank-account" */
export function extractBankAccountCode(links: any[]): string {
  if (!Array.isArray(links)) return ''
  const link = links.find((l: any) => l?.rel === 'bank-account')
  if (!link?.href) return ''
  return link.href.trim().replace(/\/$/, '').split('/').pop() ?? ''
}

/** Formata documento + número de parcela no padrão SIENGE */
function buildTitParc(
  documentId: string,
  documentNumber: string,
  installment: number | string | null,
): string {
  const id = documentId || ''
  const num = documentNumber || ''
  const inst = installment != null ? String(installment) : ''
  if (id && num) return inst ? `${id}.${num}/${inst}` : `${id}.${num}`
  if (id) return inst ? `${id}/${inst}` : id
  if (num) return inst ? `${num}/${inst}` : num
  return ''
}

/** Converte valor BR 'XXXXXXX,XX' em número */
export function toMoney(value: any): number {
  if (typeof value === 'number') return value
  if (!value) return 0
  return parseFloat(String(value).replace(/\./g, '').replace(',', '.')) || 0
}

// ─── Filtros ──────────────────────────────────────────────────────────────────

function dateToNumeric(dateMs: number): number {
  const d = new Date(dateMs)
  return d.getFullYear() * 10000 + (d.getMonth() + 1) * 100 + d.getDate()
}

function filterByDate(
  item: any,
  startNumeric: number | null,
  endNumeric: number | null,
): boolean {
  const dn: number = item.dueDateNumeric || 0
  if (!dn) return false
  const itemN = dateToNumeric(dn)
  if (startNumeric !== null && itemN < startNumeric) return false
  if (endNumeric !== null && itemN > endNumeric) return false
  return true
}

function filterByCompany(
  item: any,
  fcSelectedCompany: string,
  buildings: any[],
): boolean {
  if (fcSelectedCompany === 'all') return true
  const cId = String(item.companyId ?? '')
  if (cId && cId !== 'undefined' && cId !== 'null') {
    return cId === fcSelectedCompany
  }
  // fallback via building
  const building = buildings.find((b: any) =>
    String(b.id) === String(item.buildingId) || String(b.code ?? '') === String(item.buildingId)
  )
  return building ? String(building.companyId) === fcSelectedCompany : false
}

function filterByBuilding(
  item: any,
  fcSelectedBuilding: string,
  buildings: any[],
): boolean {
  if (fcSelectedBuilding === 'all') return true
  const current = String(item.buildingId ?? '')
  if (current === fcSelectedBuilding) return true
  const selected = buildings.find((b: any) => String(b.id) === fcSelectedBuilding)
  if (!selected) return false
  return String(selected.code ?? '') === current
}

function filterByAccount(
  item: any,
  fcHideInternal: boolean,
): boolean {
  if (!fcHideInternal) return true
  const bankCode: string = item.bankAccountCode || ''
  if (INTERNAL_BANK_ACCOUNTS.has(bankCode)) return false
  if ((item.statementOrigin || '') === 'GE') return false
  return true
}

// ─── Tradução de tipos de lançamento ─────────────────────────────────────────

const STATEMENT_TYPE_MAP: Record<string, string> = {
  BC: 'Banco',
  CX: 'Caixa',
  PG: 'Pagamento',
  CP: 'Contas a Pagar',
  CR: 'Recebimento',
  RC: 'Recebimento',
  GE: 'Gerencial',
  DV: 'Devolução',
  RE: 'Reembolso',
  TR: 'Transferência',
  NF: 'Nota Fiscal',
  GL: 'Lançamento Geral',
}

function translateType(raw: string): string {
  return STATEMENT_TYPE_MAP[raw] || raw || 'Lançamento'
}

// ─── Processamento principal ──────────────────────────────────────────────────

export interface FluxoCaixaInput {
  allReceivableTitles: any[]
  allFinancialTitles: any[]
  buildings: any[]
  fcSelectedCompany: string
  fcSelectedBuilding: string
  fcHideInternal: boolean
  startNumeric: number | null   // yyyymmdd ou null
  endNumeric: number | null     // yyyymmdd ou null
}

/**
 * Calcula o extrato do Fluxo de Caixa replicando o relatório SIENGE PDF.
 *
 * FÓRMULA:
 *  1. Calcula saldoAnterior: soma tudo em allReceivableTitles ANTES do startNumeric
 *     (mesmos filtros de empresa/conta) → saldo de abertura do período
 *  2. Filtra allReceivableTitles por data, empresa, conta bancária (período selecionado)
 *  3. Para cada entrada: rawValue positivo = Income (entrada), negativo = estorno
 *  4. Para cada saída: rawValue positivo = Expense (saída), negativo = estorno de saída
 *  5. Ordena por data ascendente
 *  6. Calcula saldo acumulado em série, iniciando de saldoAnterior
 *  7. Fallback: se extrato vazio, usa allFinancialTitles + allReceivableTitles
 */
export function calcularFluxoCaixa(input: FluxoCaixaInput): FluxoCaixaResult {
  const {
    allReceivableTitles,
    allFinancialTitles,
    buildings,
    fcSelectedCompany,
    fcSelectedBuilding,
    fcHideInternal,
    startNumeric,
    endNumeric,
  } = input

  // ── Passo 1: saldoAnterior — soma TUDO antes do startNumeric ───────────────
  // Replica o "Saldo Anterior" do relatório SIENGE: ao primeiro lançamento do
  // período, o saldo já parte do acumulado histórico, não de zero.
  let saldoAnterior = 0
  if (startNumeric !== null) {
    saldoAnterior = allReceivableTitles.reduce((acc: number, t: any) => {
      const dn = t.dueDateNumeric || 0
      if (!dn) return acc
      if (dateToNumeric(dn) >= startNumeric) return acc  // só ANTES do período
      if (!filterByCompany(t, fcSelectedCompany, buildings)) return acc
      if (!filterByBuilding(t, fcSelectedBuilding, buildings)) return acc
      if (!filterByAccount(t, fcHideInternal)) return acc
      const rv: number = t.rawValue ?? ((t.type === 'Income' ? 1 : -1) * Math.abs(toMoney(t.amount)))
      return acc + (t.type === 'Income' ? rv : -Math.abs(rv))
    }, 0)
  }

  // ── Passo 2: filtra dados do extrato bancário ────────────────────────────────
  const extrato = allReceivableTitles.filter((t: any) =>
    filterByDate(t, startNumeric, endNumeric) &&
    filterByCompany(t, fcSelectedCompany, buildings) &&
    filterByBuilding(t, fcSelectedBuilding, buildings) &&
    filterByAccount(t, fcHideInternal),
  )

  // ── Passo 3: mapeia para FluxoCaixaRow ───────────────────────────────────────
  let rows: FluxoCaixaRow[] = extrato.map((t: any): FluxoCaixaRow => {
    // rawValue preserva o sinal original da API SIENGE:
    //   Income positivo  → entrada real de dinheiro
    //   Income negativo  → estorno/devolução de entrada (reduz entradas)
    //   Expense negativo → saída (a API envia negativo para despesas)
    //   Expense positivo → estorno de saída (raro)
    const rv: number = t.rawValue ?? (
      (t.type === 'Income' ? 1 : -1) * Math.abs(toMoney(t.amount))
    )
    const isIncome = t.type === 'Income'
    const entrada = isIncome ? rv : 0           // pode ser negativo (estorno)
    const saida = !isIncome ? Math.abs(rv) : 0  // sempre positivo na coluna Saída

    const titParc = buildTitParc(
      t.documentId || '',
      String(t.documentNumber || ''),
      t.installmentNumber ?? null,
    )

    return {
      id: t.id,
      data: t.dueDate || '',
      dataNumeric: t.dueDateNumeric || 0,
      titParc: titParc || String(t.id || ''),
      documento: t.documentId || String(t.documentNumber || `EXT-${t.id}`),
      origem: t.statementOrigin || '',
      statementType: translateType(t.statementType || t.statementOrigin || (isIncome ? 'RC' : 'PG')),
      bankAccount: t.bankAccountCode || '',
      pessoa: t.clientName || t.description || 'Extrato Diversos',
      entrada,
      saida,
      saldo: 0, // será calculado abaixo
    }
  })

  // ── Passo 3: fallback se extrato vazio ──────────────────────────────────────
  if (rows.length === 0) {
    const receberFallback: FluxoCaixaRow[] = allReceivableTitles
      .filter((t: any) =>
        filterByDate(t, startNumeric, endNumeric) &&
        filterByCompany(t, fcSelectedCompany, buildings) &&
        filterByBuilding(t, fcSelectedBuilding, buildings),
      )
      .map((t: any): FluxoCaixaRow => ({
        id: t.id,
        data: t.dueDate || '',
        dataNumeric: t.dueDateNumeric || 0,
        titParc: String(t.documentNumber || t.id || ''),
        documento: String(t.documentNumber || t.id || ''),
        origem: 'RC',
        statementType: 'Recebimento',
        bankAccount: '',
        pessoa: t.clientName || t.description || 'Cliente',
        entrada: Math.abs(toMoney(t.amount)),
        saida: 0,
        saldo: 0,
      }))

    const pagarFallback: FluxoCaixaRow[] = allFinancialTitles
      .filter((t: any) =>
        filterByDate(t, startNumeric, endNumeric) &&
        filterByCompany(t, fcSelectedCompany, buildings) &&
        filterByBuilding(t, fcSelectedBuilding, buildings),
      )
      .map((t: any): FluxoCaixaRow => ({
        id: t.id,
        data: t.dueDate || '',
        dataNumeric: t.dueDateNumeric || 0,
        titParc: String(t.id || ''),
        documento: String(t.id || ''),
        origem: 'PG',
        statementType: 'Pagamento',
        bankAccount: '',
        pessoa: t.creditorName || t.description || 'Credor',
        entrada: 0,
        saida: Math.abs(toMoney(t.amount)),
        saldo: 0,
      }))

    rows = [...receberFallback, ...pagarFallback]
  }

  // ── Passo 4: ordena por data ASC ────────────────────────────────────────────
  rows.sort((a, b) => (a.dataNumeric || 0) - (b.dataNumeric || 0))

  // ── Passo 5: calcula saldo acumulado partindo do saldoAnterior ───────────────
  // CORRETO: o primeiro lançamento do período já parte do saldo histórico,
  // exatamente como o relatório SIENGE PDF mostra.
  let saldoAtual = saldoAnterior
  const rowsComSaldo = rows.map(row => {
    saldoAtual = saldoAtual + row.entrada - row.saida
    return { ...row, saldo: saldoAtual }
  })

  return { rows: rowsComSaldo, saldoAnterior }
}

/** Sumariza os totais do extrato */
export function summarizeFluxoCaixa(rows: FluxoCaixaRow[], saldoAnterior = 0): FluxoCaixaSummary {
  const totalEntradas = rows.reduce((acc, r) => acc + r.entrada, 0)
  const totalSaidas = rows.reduce((acc, r) => acc + r.saida, 0)
  const saldoFinal = rows.length > 0 ? rows[rows.length - 1].saldo : saldoAnterior
  return {
    totalEntradas,
    totalSaidas,
    saldoAnterior,
    saldoFinal,
    registros: rows.length,
  }
}

/** Converte Date → yyyymmdd numérico para uso nos filtros */
export function dateToFilter(d: Date | undefined | null): number | null {
  if (!d) return null
  return parseInt(format(d, 'yyyyMMdd'))
}
