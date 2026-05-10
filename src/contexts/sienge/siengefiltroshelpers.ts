import { addDays } from 'date-fns';

export type DateRange = {
  start: number | null;
  endExclusive: number | null;
};

export function calcularDateRangeSienge({
  startDate,
  endDate,
  hasManualDateFilter,
  globalPeriodMode,
  defaultWindowStart,
  defaultWindowEnd,
}: {
  startDate?: Date;
  endDate?: Date;
  hasManualDateFilter: boolean;
  globalPeriodMode: 'last3m' | 'all';
  defaultWindowStart: Date;
  defaultWindowEnd: Date;
}): DateRange {
  const effectiveStartDate = hasManualDateFilter
    ? startDate || null
    : globalPeriodMode === 'all'
    ? null
    : defaultWindowStart;

  const effectiveEndDate = hasManualDateFilter
    ? endDate || startDate || null
    : globalPeriodMode === 'all'
    ? null
    : defaultWindowEnd;

  const start = effectiveStartDate
    ? new Date(
        effectiveStartDate.getFullYear(),
        effectiveStartDate.getMonth(),
        effectiveStartDate.getDate(),
      ).getTime()
    : null;

  const endExclusive = effectiveEndDate
    ? addDays(
        new Date(
          effectiveEndDate.getFullYear(),
          effectiveEndDate.getMonth(),
          effectiveEndDate.getDate(),
        ),
        1,
      ).getTime()
    : null;

  return { start, endExclusive };
}

export function correspondeDateRangeSienge(
  numericValue: number | undefined,
  dateRange: DateRange,
): boolean {
  if (!dateRange.start && !dateRange.endExclusive) return true;
  if (!numericValue || numericValue === 0) return false;
  if (dateRange.start !== null && numericValue < dateRange.start) return false;
  if (dateRange.endExclusive !== null && numericValue >= dateRange.endExclusive) return false;
  return true;
}

export function filtrarBuildingOptionsPorAtividade({
  allOrders,
  allFinancialTitles,
  allReceivableTitles,
  buildings,
  matchesDateRange,
}: {
  allOrders: any[];
  allFinancialTitles: any[];
  allReceivableTitles: any[];
  buildings: any[];
  matchesDateRange: (value?: number) => boolean;
}): any[] {
  if (allOrders.length === 0 && allFinancialTitles.length === 0 && allReceivableTitles.length === 0) {
    return buildings;
  }

  const activeIds = new Set<string>();

  allOrders.forEach((o) => {
    if (matchesDateRange(o.dateNumeric)) activeIds.add(String(o.buildingId));
  });

  allFinancialTitles.forEach((f) => {
    if (matchesDateRange(f.dueDateNumeric)) activeIds.add(String(f.buildingId));
  });

  allReceivableTitles.forEach((r) => {
    if (matchesDateRange(r.dueDateNumeric)) activeIds.add(String(r.buildingId));
  });

  return buildings.filter(
    (b) => activeIds.has(String(b.id)) || Boolean(b.code && activeIds.has(String(b.code))),
  );
}
