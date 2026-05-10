import { format } from 'date-fns';

export function normalizarProgressoSync(value: number): number {
  return Math.max(0, Math.min(100, Math.round(value)));
}

export function deveExecutarAutoSync(lastUpdate?: Date, minAgeMs = 6 * 60 * 60 * 1000): boolean {
  const last = lastUpdate?.getTime?.() ?? 0;
  const ageMs = Date.now() - last;
  if (!Number.isFinite(ageMs)) return false;
  return ageMs >= minAgeMs;
}

export function resolverPeriodoEfetivo({
  hasManualDateFilter,
  startDate,
  endDate,
  globalPeriodMode,
  defaultWindowStart,
  defaultWindowEnd,
}: {
  hasManualDateFilter: boolean;
  startDate?: Date;
  endDate?: Date;
  globalPeriodMode: 'last3m' | 'all';
  defaultWindowStart: Date;
  defaultWindowEnd: Date;
}): { effectiveStart: Date | null; effectiveEnd: Date | null } {
  const effectiveStart = hasManualDateFilter
    ? startDate || null
    : globalPeriodMode === 'all'
    ? null
    : defaultWindowStart;

  const effectiveEnd = hasManualDateFilter
    ? endDate || startDate || null
    : globalPeriodMode === 'all'
    ? null
    : defaultWindowEnd;

  return { effectiveStart, effectiveEnd };
}

export function montarParametrosFilteredSienge({
  selectedCompany,
  fcSelectedBuilding,
  selectedUser,
  selectedRequester,
  effectiveStart,
  effectiveEnd,
}: {
  selectedCompany: string;
  fcSelectedBuilding: string;
  selectedUser: string;
  selectedRequester: string;
  effectiveStart: Date | null;
  effectiveEnd: Date | null;
}): Record<string, string> {
  const params: Record<string, string> = {
    company_id: selectedCompany,
    building_id: fcSelectedBuilding,
    user_id: selectedUser,
    requester_id: selectedRequester,
  };
  if (effectiveStart) params.start_date = format(effectiveStart, 'yyyy-MM-dd');
  if (effectiveEnd) params.end_date = format(effectiveEnd, 'yyyy-MM-dd');
  return params;
}
