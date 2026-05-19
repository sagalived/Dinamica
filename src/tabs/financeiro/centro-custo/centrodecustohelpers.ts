import { format, isValid, parse } from 'date-fns';

import type { CentroDeCustoRow } from '../types';

export function parseDateSafe(raw: string | undefined): Date | null {
  if (!raw) return null;
  const text = String(raw).trim();
  if (!text) return null;

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(text)) {
    const br = parse(text, 'dd/MM/yyyy', new Date());
    return isValid(br) ? br : null;
  }

  if (/^\d{4}-\d{2}-\d{2}(T.*)?$/.test(text)) {
    const isoDate = parse(text.slice(0, 10), 'yyyy-MM-dd', new Date());
    return isValid(isoDate) ? isoDate : null;
  }

  const iso = parse(text, 'yyyy-MM-dd', new Date());
  if (isValid(iso)) return iso;

  const native = new Date(text);
  return Number.isNaN(native.getTime()) ? null : native;
}

export function parseDateFlexible(raw: string | undefined): number {
  const date = parseDateSafe(raw);
  return date ? date.getTime() : 0;
}

export function formatDateSafe(raw: string | undefined, fallback = '—'): string {
  const date = parseDateSafe(raw);
  return date ? format(date, 'dd/MM/yyyy') : fallback;
}

export function normalizeDocument(raw: string | undefined): string {
  const text = String(raw || '').trim();
  if (!text || text === '—') return '';

  const upper = text.toUpperCase();
  const ppc = upper.match(/PPC\D*(\d+)/);
  if (ppc) {
    return `PPC.${ppc[1]}`;
  }

  if (upper.includes('DARM')) {
    const groups = upper.match(/\d+/g) || [];
    if (groups.length >= 3) return `DARM.${groups[0]}.${groups[1]}.${groups[2]}`;
    if (groups.length === 2) return `DARM.${groups[0]}.${groups[1]}`;
    if (groups.length === 1) return `DARM.${groups[0]}`;
    return 'DARM';
  }

  const digits = upper.replace(/\D/g, '');
  if (digits.length >= 8) {
    const last8 = digits.slice(-8);
    if (!/^0+$/.test(last8)) return `${last8.slice(0, 6)}-${last8.slice(6)}`;
  }

  return upper;
}

export function getDocumentParts(raw: string | undefined): { original: string; treated: string } {
  const original = String(raw || '').trim() || '—';
  const treated = normalizeDocument(raw);
  return {
    original,
    treated: treated && treated !== original.toUpperCase() ? treated : '',
  };
}

export function buildEntityLabel(name: string | undefined, doc: string | number | undefined, fallback: string): string {
  const safeName = String(name || '').trim();
  const safeDoc = String(doc || '').trim();
  if (safeName && safeDoc) return `${safeName} • ${safeDoc}`;
  if (safeName) return safeName;
  if (safeDoc) return `${fallback} • ${safeDoc}`;
  return fallback;
}

export function pickEntityName(...values: Array<string | undefined>): string {
  const placeholders = new Set(['Extrato/Cliente', 'Pagamento/Credor', 'Credor sem nome', 'Cliente sem nome']);
  for (const value of values) {
    const safe = String(value || '').trim();
    if (!safe) continue;
    if (placeholders.has(safe)) continue;
    return safe;
  }
  return '';
}

export function parseCsvText(text: string): CentroDeCustoRow[] {
  const lines = text.split('\n').filter((line) => line.trim());
  if (lines.length < 2) return [];

  const header = lines[0].split(/[;,]/).map((h) => h.trim().toLowerCase().replace(/\s+/g, '_'));

  return lines
    .slice(1)
    .map((line) => {
      const cols = line.split(/[;,]/);
      const obj: any = {};
      header.forEach((h, i) => {
        obj[h] = (cols[i] || '').trim().replace(/"/g, '');
      });
      const parseNum = (s: string) => parseFloat(String(s).replace(/\./g, '').replace(',', '.')) || 0;
      return {
        data: obj.data || obj.date || obj.vencimento || '',
        historico: obj.historico || obj.descricao || obj.description || obj.nome || '',
        obra: obj.obra || obj.building || obj.empreendimento || '',
        entrada: parseNum(obj.entrada || obj.receita || obj.credito || '0'),
        saida: parseNum(obj.saida || obj.despesa || obj.debito || '0'),
        saldo: parseNum(obj.saldo || obj.balance || '0'),
      };
    })
    .filter((row) => row.data || row.historico);
}
