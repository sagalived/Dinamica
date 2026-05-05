import { useCallback, useEffect, useMemo, useState } from "react";
import {
  calculateDashboardTotals,
  createEmptyDashboardData,
  loadDashboardBootstrap,
  loadDashboardData,
} from "./dashboardController";
import { createInitialFilters, SYNC_INTERVAL_MS } from "../models/filterModel";
import { requestDashboardSync } from "../services/dashboardService";

export function useDashboardController() {
  const currentYear = new Date().getFullYear();

  const [draftFilters, setDraftFilters] = useState(() => createInitialFilters(currentYear));
  const [appliedFilters, setAppliedFilters] = useState(() => createInitialFilters(currentYear));

  const [companies, setCompanies] = useState([]);
  const [buildings, setBuildings] = useState([]);
  const [data, setData] = useState(createEmptyDashboardData());
  const [buildingMargins, setBuildingMargins] = useState([]);

  const [loading, setLoading] = useState(true);
  const [isSyncing, setIsSyncing] = useState(false);
  const [error, setError] = useState("");
  const [lastSyncAt, setLastSyncAt] = useState(null);
  const [syncStepLabel, setSyncStepLabel] = useState("Aguardando sincronização automática");
  const [showPendingFilterHint, setShowPendingFilterHint] = useState(false);

  const hasPendingFilterChanges = useMemo(() => {
    return (
      draftFilters.year !== appliedFilters.year ||
      draftFilters.companyId !== appliedFilters.companyId ||
      draftFilters.buildingId !== appliedFilters.buildingId
    );
  }, [draftFilters, appliedFilters]);

  const refreshDashboard = useCallback(async (filters, options = {}) => {
    const { runSync = false } = options;

    setLoading(true);
    setError("");

    try {
      if (runSync) {
        setIsSyncing(true);
        setSyncStepLabel("1/3 Sincronizando com o Sienge...");
        await requestDashboardSync();
        setLastSyncAt(new Date());
      }

      setSyncStepLabel(runSync ? "2/3 Recarregando dados tratados..." : "Atualizando dados...");
      const result = await loadDashboardData(filters);
      setData(result.monthly);
      setBuildingMargins(result.buildingMargins);
      setSyncStepLabel(runSync ? "3/3 Sincronização concluída" : "Dados atualizados");
    } catch (err) {
      setError(err.message || "Falha ao carregar dashboard.");
      setSyncStepLabel("Erro na atualização");
      setData(createEmptyDashboardData());
      setBuildingMargins([]);
    } finally {
      setIsSyncing(false);
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    async function bootstrap() {
      try {
        const options = await loadDashboardBootstrap();
        setCompanies(options.companies);
        setBuildings(options.buildings);
      } catch (err) {
        console.warn("bootstrap:", err.message);
      }
    }

    bootstrap();
  }, []);

  useEffect(() => {
    refreshDashboard(appliedFilters);
  }, [appliedFilters, refreshDashboard]);

  useEffect(() => {
    const intervalId = setInterval(() => {
      refreshDashboard(appliedFilters, { runSync: true });
    }, SYNC_INTERVAL_MS);

    return () => clearInterval(intervalId);
  }, [appliedFilters, refreshDashboard]);

  useEffect(() => {
    if (!hasPendingFilterChanges) {
      setShowPendingFilterHint(false);
      return;
    }

    const timeoutId = setTimeout(() => setShowPendingFilterHint(true), 350);
    return () => clearTimeout(timeoutId);
  }, [hasPendingFilterChanges]);

  const totals = useMemo(() => calculateDashboardTotals(data), [data]);

  function updateDraftFilter(key, value) {
    setDraftFilters((prev) => ({ ...prev, [key]: value }));
  }

  function applyFilters() {
    setAppliedFilters({ ...draftFilters });
  }

  async function syncNow() {
    await refreshDashboard(appliedFilters, { runSync: true });
  }

  return {
    currentYear,
    draftFilters,
    hasPendingFilterChanges,
    showPendingFilterHint,
    companies,
    buildings,
    data,
    totals,
    buildingMargins,
    loading,
    isSyncing,
    error,
    lastSyncAt,
    syncStepLabel,
    updateDraftFilter,
    applyFilters,
    syncNow,
  };
}
