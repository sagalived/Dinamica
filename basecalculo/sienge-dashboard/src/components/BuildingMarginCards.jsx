import { formatCurrency, formatPercent } from "../models/dashboardModel";

export function BuildingMarginCards({ data }) {
  if (!data.length) {
    return <div className="emptyState">Sem dados de obra para o filtro selecionado.</div>;
  }

  return (
    <section className="buildingSection">
      <h2>Margem por Obra</h2>
      <div className="buildingGrid">
        {data.slice(0, 6).map((row) => (
          <article key={row.buildingId} className="buildingCard">
            <h3>{row.buildingName}</h3>
            <p>Receita: <strong>{formatCurrency(row.receita)}</strong></p>
            <p>Custo variável: <strong>{formatCurrency(row.custoVariavel)}</strong></p>
            <p>MC: <strong>{formatCurrency(row.margem)}</strong></p>
            <p>%MC: <strong>{formatPercent(row.mcPercent)}</strong></p>
          </article>
        ))}
      </div>
    </section>
  );
}
