import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { bestMonth, formatCurrency, formatPercent } from "../models/dashboardModel";

function CustomMonthTooltip({ active, payload, label, dataKey }) {
  if (!active || !payload?.length) return null;
  const rawValue = payload[0].value;
  const value = dataKey === "mcPercent" ? formatPercent(rawValue) : formatCurrency(rawValue);

  return (
    <div className="chartTooltip">
      <strong>{label}</strong>
      <span>{value}</span>
    </div>
  );
}

export function KpiCard({ title, value, dataKey, data, icon: Icon, variant }) {
  const best = bestMonth(data, dataKey);

  return (
    <section className={`card ${variant}`}>
      <Icon className="cardIcon" />
      <div className="watermark">%</div>

      <div className="cardContent">
        <h2>{title}</h2>
        <div className="value">{value}</div>
        <p>Melhor mês: {best?.month || "-"}</p>
        <strong>{dataKey === "mcPercent" ? formatPercent(best?.[dataKey]) : formatCurrency(best?.[dataKey])}</strong>
      </div>

      <div className="chart">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 8, right: 10, left: 10, bottom: 0 }}>
            <defs>
              <linearGradient id={`fill-${variant}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="white" stopOpacity={0.55} />
                <stop offset="95%" stopColor="white" stopOpacity={0.18} />
              </linearGradient>
            </defs>

            <XAxis
              dataKey="month"
              interval={0}
              minTickGap={0}
              padding={{ left: 6, right: 6 }}
              tick={{ fill: "white", fontSize: 11 }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis hide />
            <Tooltip
              cursor={{ stroke: "rgba(255,255,255,0.5)", strokeWidth: 1 }}
              content={<CustomMonthTooltip dataKey={dataKey} />}
            />
            <Area
              type="monotone"
              dataKey={dataKey}
              stroke="white"
              strokeWidth={3}
              fill={`url(#fill-${variant})`}
              activeDot={{ r: 5, fill: "#ffffff", stroke: "rgba(17,24,39,.9)", strokeWidth: 2 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
