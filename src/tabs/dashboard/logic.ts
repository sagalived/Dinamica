import { format, parseISO } from 'date-fns';

export function safeFormat(dateStr: string | undefined, formatStr: string = 'dd/MM/yyyy'): string {
  if (!dateStr || dateStr === '---') return '---';
  try {
    const d = parseISO(dateStr);
    if (Number.isNaN(d.getTime())) return '---';
    return format(d, formatStr);
  } catch {
    return '---';
  }
}
