import React from "react";
import { BarChart3, Loader2, Percent, PieChart } from "lucide-react";
import { BuildingMarginCards } from "./components/BuildingMarginCards";
import { DashboardFilters } from "./components/DashboardFilters";
import { KpiCard } from "./components/KpiCard";
import { useDashboardController } from "./controllers/useDashboardController";
import { formatCurrency, formatPercent } from "./models/dashboardModel";
import "./styles.css";

export default function App() {
  const {
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
  } = useDashboardController();

  const syncLabel = lastSyncAt
    ? `${syncStepLabel} • Última sincronização: ${lastSyncAt.toLocaleTimeString("pt-BR")}`
    : "Sincronização automática a cada 20 minutos";

  return (
    <main className="page">
      <header className="header">
        <h1>Análise de Receita e Margem</h1>

        <DashboardFilters
          currentYear={currentYear}
          filters={draftFilters}
          companies={companies}
          buildings={buildings}
          onYearChange={(value) => updateDraftFilter("year", value)}
          onCompanyChange={(value) => updateDraftFilter("companyId", value)}
          onBuildingChange={(value) => updateDraftFilter("buildingId", value)}
          onApply={applyFilters}
          applyDisabled={loading || isSyncing || !hasPendingFilterChanges}
          hasPendingChanges={hasPendingFilterChanges}
          showPendingHint={showPendingFilterHint}
        />
      </header>

      <div className="syncBar">
        <span>{syncLabel}</span>
        <button type="button" className="syncButton" onClick={syncNow} disabled={loading || isSyncing}>
          {isSyncing ? "Sincronizando..." : "Sincronizar agora"}
        </button>
      </div>

      {error && <div className="error">{error}</div>}

      {loading ? (
        <div className="loading"><Loader2 className="spin" /> Carregando dados...</div>
      ) : (
        <>
          <div className="grid">
            <KpiCard
              title="Receita Operacional"
              value={formatCurrency(totals.receita)}
              dataKey="receita"
              data={data}
              icon={BarChart3}
              variant="receita"
            />

            <KpiCard
              title="Margem de Contribuição"
              value={formatCurrency(totals.margem)}
              dataKey="margem"
              data={data}
              icon={PieChart}
              variant="margem"
            />

            <KpiCard
              title="% MC Geral"
              value={formatPercent(totals.mcPercent)}
              dataKey="mcPercent"
              data={data}
              icon={Percent}
              variant="percentual"
            />
          </div>

          <BuildingMarginCards data={buildingMargins} />
        </>
      )}

      <footer>Receita Operacional = entradas operacionais | Margem = Receita − Custos Variáveis | % MC = Margem / Receita × 100</footer>
    </main>
  );
}
