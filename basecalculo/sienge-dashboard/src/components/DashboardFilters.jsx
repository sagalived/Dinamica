export function DashboardFilters({
  currentYear,
  filters,
  companies,
  buildings,
  onYearChange,
  onCompanyChange,
  onBuildingChange,
  onApply,
  applyDisabled,
  hasPendingChanges,
  showPendingHint,
}) {
  return (
    <div className="filtersWrap">
      <div className="filters">
      <label>
        Ano
        <select value={filters.year} onChange={(e) => onYearChange(e.target.value)}>
          {[currentYear, currentYear - 1, currentYear - 2].map((y) => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
      </label>

      <label>
        Empresa
        <select value={filters.companyId} onChange={(e) => onCompanyChange(e.target.value)}>
          <option value="Todos">Todas</option>
          {companies.map((company, index) => (
            <option key={company.id ?? company.code ?? index} value={company.id ?? company.code}>
              {company.name ?? company.tradeName ?? company.companyName ?? company.id ?? company.code}
            </option>
          ))}
        </select>
      </label>

      <label>
        Obra
        <select value={filters.buildingId} onChange={(e) => onBuildingChange(e.target.value)}>
          <option value="Todos">Todas</option>
          {buildings.map((building, index) => (
            <option key={building.id ?? building.code ?? building.buildingId ?? index} value={building.id ?? building.code ?? building.buildingId}>
              {building.name ?? building.commercialName ?? building.buildingName ?? building.id ?? building.code}
            </option>
          ))}
        </select>
      </label>
      </div>

      <button type="button" className="applyButton" onClick={onApply} disabled={applyDisabled}>
        Aplicar filtros
      </button>

      {showPendingHint && hasPendingChanges && (
        <span className="pendingHint">Filtros alterados, clique em aplicar</span>
      )}
    </div>
  );
}
