export const SYNC_INTERVAL_MS = 20 * 60 * 1000;

export function createInitialFilters(currentYear) {
  return {
    year: String(currentYear),
    companyId: "Todos",
    buildingId: "Todos",
  };
}
