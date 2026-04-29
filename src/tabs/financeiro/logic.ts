import { fixText } from '../../lib/text';

export function toMoney(value: unknown): number {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

export function normalizeStatus(value: unknown): string {
  return fixText(String(value || ''))
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toUpperCase();
}

export function translateStatusLabel(value: unknown): string {
  const raw = fixText(String(value || 'N/D')).trim();
  const normalized = normalizeStatus(value);
  const map: Record<string, string> = {
    CANCELED: 'CANCELADO',
    CANCELLED: 'CANCELADO',
    FULLY_DELIVERED: 'ENTREGUE TOTAL',
    PARTIALLY_DELIVERED: 'ENTREGUE PARCIAL',
    PENDING: 'PENDENTE',
    APPROVED: 'APROVADO',
    REJECTED: 'REPROVADO',
    OPEN: 'ABERTO',
    CLOSED: 'FECHADO',
    IN_PROGRESS: 'EM ANDAMENTO',
    WAITING: 'AGUARDANDO',
    SUCCESS: 'SUCESSO',
    ERROR: 'ERRO',
    DRAFT: 'RASCUNHO',
    ON_HOLD: 'EM ESPERA',
    N_A: 'N/D',
  };
  return map[normalized] || raw || 'N/D';
}

export function translateStatementType(value: unknown): string {
  const normalized = normalizeStatus(value);
  const map: Record<string, string> = {
    INCOME: 'RECEBIMENTO',
    EXPENSE: 'PAGAMENTO',
    PAYMENT: 'PAGAMENTO',
    RECEIPT: 'RECEBIMENTO',
    TRANSFER: 'TRANSFERENCIA',
    ADJUSTMENT: 'AJUSTE',
  };
  return map[normalized] || fixText(String(value || 'Lancamento'));
}

export function isSettledFinancialStatus(value: unknown): boolean {
  const status = normalizeStatus(value);
  return ['S', 'BAIXADO', 'BAIXADA', 'PAGO', 'PAGA', 'LIQUIDADO', 'LIQUIDADA', 'QUITADO', 'QUITADA'].includes(status);
}
