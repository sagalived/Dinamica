import { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

type FinanceiroTabProps = {
  summary: any;
  directoryUsers: any[];
};

export function FinanceiroTab({ summary, directoryUsers }: FinanceiroTabProps) {
  const chartData = useMemo(() => {
    return summary?.creditor_states?.slice(0, 8) ?? [];
  }, [summary]);

  const brandColors = ['#3ad64c', '#22c55e', '#84cc16', '#bef264', '#166534', '#4d7c0f'];

  return (
    <div className="space-y-6">
      <section className="rounded-[30px] border border-white/6 bg-[#161618] p-6">
        <div className="mb-6">
          <h2 className="text-2xl font-black text-white">Financeiro</h2>
          <p className="mt-2 text-sm text-gray-400">Análise de títulos a pagar e receber</p>
        </div>

        {chartData.length === 0 ? (
          <div className="rounded-2xl border border-white/10 bg-white/5 p-6 text-center text-gray-400">
            Nenhum dado financeiro disponível. Verifique /api/sienge/bootstrap para dados.
          </div>
        ) : (
          <div className="mt-6">
            <p className="text-sm font-bold text-white mb-4">Credores por Estado</p>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#ffffff20" />
                <XAxis dataKey="state" stroke="#ffffff40" />
                <YAxis stroke="#ffffff40" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#161618', border: '1px solid rgba(255,255,255,0.1)' }}
                  labelStyle={{ color: '#fff' }}
                />
                <Bar dataKey="total" fill="#30d64a" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        <div className="mt-6 rounded-2xl border border-white/6 bg-[#101113] p-4">
          <p className="text-sm font-bold text-white">Endpoints Disponíveis</p>
          <ul className="mt-2 space-y-1 text-xs text-gray-500">
            <li>• GET /api/sienge/bootstrap → títulos em "financeiro" e "receber"</li>
            <li>• POST /api/sienge/sync → sincroniza com ERP (stub)</li>
          </ul>
        </div>
      </section>
    </div>
  );
}
