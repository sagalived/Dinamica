export type CardStatus = 'planned' | 'in_progress' | 'review' | 'done' | 'blocked';
export type CardPriority = 'low' | 'medium' | 'high' | 'critical';

export const COLUMNS: { id: CardStatus; label: string; emoji: string; color: string; bg: string }[] = [
  { id: 'planned', label: 'Planejado', emoji: '📋', color: 'text-blue-400', bg: 'bg-blue-500/10 border-blue-500/20' },
  { id: 'in_progress', label: 'Em Execucao', emoji: '🔧', color: 'text-orange-400', bg: 'bg-orange-500/10 border-orange-500/20' },
  { id: 'review', label: 'Em Revisao', emoji: '🔍', color: 'text-purple-400', bg: 'bg-purple-500/10 border-purple-500/20' },
  { id: 'done', label: 'Concluido', emoji: '✅', color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
  { id: 'blocked', label: 'Bloqueado', emoji: '⛔', color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
];

export const PRIORITIES: { id: CardPriority; label: string; color: string; dot: string }[] = [
  { id: 'low', label: 'Baixa', color: 'text-gray-400', dot: 'bg-gray-400' },
  { id: 'medium', label: 'Media', color: 'text-blue-400', dot: 'bg-blue-400' },
  { id: 'high', label: 'Alta', color: 'text-orange-400', dot: 'bg-orange-400' },
  { id: 'critical', label: 'Critica', color: 'text-red-400', dot: 'bg-red-400' },
];

export function formatDate(iso: string) {
  if (!iso) return '';
  try {
    return new Date(iso).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: '2-digit' });
  } catch {
    return iso;
  }
}

export function isOverdue(dueDate: string) {
  if (!dueDate) return false;
  return new Date(dueDate) < new Date();
}

export function getStatusCol(id: CardStatus) {
  return COLUMNS.find((c) => c.id === id) ?? COLUMNS[0];
}

export function getPriorityMeta(id: CardPriority) {
  return PRIORITIES.find((p) => p.id === id) ?? PRIORITIES[1];
}
