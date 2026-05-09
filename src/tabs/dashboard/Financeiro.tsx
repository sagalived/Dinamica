import React, { useMemo, useState, useCallback } from 'react';
import { motion } from 'motion/react';
import { Building2, CalendarDays, FileText, TrendingUp, AlertCircle, RefreshCw } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Alert, AlertDescription } from '../../components/ui/alert';
import { useSienge } from '../../contexts/SiengeContext';
import { format } from 'date-fns';
import { sienge as api } from '../../lib/api';

type BuildingRevenueRow = {
  buildingId: string;
  buildingName: string;
  valuesByMonth: Record<string, number>;
  total: number;
  reconciled?: boolean;
  statusTotals?: Record<string, number>;
};

type AnalysisFilterKey = 'realizado' | 'comprometido' | 'aRealizar';
const ANALYSIS_FILTER_OPTIONS: Array<{ key: AnalysisFilterKey; label: string }> = [
  { key: 'realizado', label: 'Realizado' },
  { key: 'comprometido', label: 'Comprometido' },
  { key: 'aRealizar', label: 'A realizar' },
];

type SyncFileRecord = {
  file_name: string;
  dataset: string;
  reason?: string | null;
  start_date?: string | null;
  end_date?: string | null;
  created_at?: string | null;
  mtime?: string | null;
  path?: string | null;
};

type SyncSummary = {
  status?: string;
  started_at?: string;
  finished_at?: string;
  message?: string;
  source?: string;
  updated_files_count?: number;
  updated_files?: SyncFileRecord[];
  counts?: Record<string, number>;
};

const toMoney = (value: any): number => {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
};

const parseDate = (row: any): Date | null => {
  const numeric = Number(row?.dueDateNumeric ?? 0);
  if (numeric > 0) {
    const d = new Date(numeric);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  const raw = String(row?.dueDate ?? row?.paymentDate ?? row?.date ?? '').trim();
  if (!raw || raw === '---') return null;
  const candidate = new Date(raw);
  return Number.isNaN(candidate.getTime()) ? null : candidate;
};

const parseLooseDate = (value: any): Date | null => {
  const raw = String(value ?? '').trim();
  if (!raw || raw === '---') return null;
  const candidate = new Date(raw);
  return Number.isNaN(candidate.getTime()) ? null : candidate;
};

const monthKey = (date: Date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

const monthLabel = (key: string) => {
  const [yearRaw, monthRaw] = key.split('-');
  const y = Number(yearRaw);
  const m = Number(monthRaw);
  if (!y || !m) return key;
  const date = new Date(y, m - 1, 1);
  const monthName = date.toLocaleDateString('pt-BR', { month: 'long' });
  return monthName.charAt(0).toUpperCase() + monthName.slice(1);
};

const fmtBRL = (n: number) => new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
}).format(Number.isFinite(n) ? n : 0);

export function DashboardFinanceiro() {
  const {
    receivableTitles,
    buildings,
    startDate,
    endDate,
    selectedCompany,
    fcSelectedBuilding,
    companies,
    globalPeriodMode,
    refreshData,
  } = useSienge();

  const [syncing, setSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);
  const [cacheStats, setCacheStats] = useState<any>(null);
  const [syncSummary, setSyncSummary] = useState<SyncSummary | null>(null);
  const [syncDetailsOpen, setSyncDetailsOpen] = useState(false);
  const [analysisFilter, setAnalysisFilter] = useState<AnalysisFilterKey>('comprometido');

  const loadCacheStats = useCallback(async () => {
    try {
      const response = await api.get('/sienge/cache-stats');
      setCacheStats(response.data.cache_stats);
      setSyncSummary(response.data.cache_stats?.latest_sync || null);
    } catch (err) {
      console.error('Erro ao carregar cache stats:', err);
    }
  }, []);

  // Carrega stats ao montar
  React.useEffect(() => {
    loadCacheStats();
    // Recarrega stats a cada 30 segundos
    const interval = setInterval(loadCacheStats, 30000);
    return () => clearInterval(interval);
  }, [loadCacheStats]);

  const handleSync = useCallback(async () => {
    setSyncing(true);
    setSyncMessage(null);
    try {
      const response = await api.post('/sienge/sync-receivable-titles');
      const latestSync = response.data.latest_sync || response.data.latestSync || response.data.cache_stats?.latest_sync || null;
      if (latestSync) {
        setSyncSummary(latestSync);
      }

      const message = response.data.message || latestSync?.message ||
        `✓ Sincronizado ${response.data.synced_count || 0} registros. Total em cache: ${response.data.total_cached || 0}`;
      setSyncMessage(message);

      await refreshData();
      await loadCacheStats();
    } catch (err: any) {
      setSyncMessage(`⚠ Erro ao sincronizar: ${err.response?.data?.detail || err.message}`);
    } finally {
      setSyncing(false);
    }
  }, [loadCacheStats, refreshData]);

  const revenueData = useMemo(() => {
    const monthSet = new Set<string>();
    const grouped = new Map<string, BuildingRevenueRow>();
    let totalTitles = 0;
    let totalRevenue = 0;

    const activeAnalysis = new Set<AnalysisFilterKey>([analysisFilter]);

    const classifyAnalysis = (row: any): AnalysisFilterKey => {
      const status = String(row?.status ?? row?.situacao ?? row?.state ?? '').trim().toLowerCase();
      const paymentDate = parseLooseDate(row?.paymentDate ?? row?.dataPagamento ?? row?.paidAt ?? row?.baixa);
      if (paymentDate) return 'realizado';
      if (['baix', 'pago', 'liquid', 'receb', 'realiz'].some((token) => status.includes(token))) return 'realizado';

      const dueDate = parseDate(row);
      if (dueDate && endDate && dueDate.getTime() > endDate.getTime()) return 'aRealizar';
      return 'comprometido';
    };

    (Array.isArray(receivableTitles) ? receivableTitles : []).forEach((row: any) => {
      const amount = Math.abs(toMoney(row?.amount ?? row?.rawValue ?? row?.value ?? row?.valor));
      if (amount <= 0) return;

      const analysisBucket = classifyAnalysis(row);
      if (!activeAnalysis.has(analysisBucket)) return;

      const d = parseDate(row);
      if (!d) return;
      const mKey = monthKey(d);
      monthSet.add(mKey);

      const rawBuildingId = String(row?.buildingId ?? row?.buildingCode ?? row?.codigoObra ?? row?.idObra ?? '').trim();
      const matchedBuilding = (Array.isArray(buildings) ? buildings : []).find((b: any) => (
        String(b?.id) === rawBuildingId || String(b?.code ?? '') === rawBuildingId
      ));

      const buildingId = matchedBuilding ? String(matchedBuilding.id) : (rawBuildingId || 'sem-obra');
      const buildingName = matchedBuilding?.name || row?.buildingName || row?.enterpriseName || row?.nomeObra || 'Sem obra';

      if (!grouped.has(buildingId)) {
        grouped.set(buildingId, {
          buildingId,
          buildingName,
          valuesByMonth: {},
          total: 0,
          statusTotals: {},
        });
      }

      const target = grouped.get(buildingId)!;
      target.valuesByMonth[mKey] = (target.valuesByMonth[mKey] || 0) + amount;
      target.total += amount;
      target.statusTotals = target.statusTotals || {};
      target.statusTotals[analysisBucket] = (target.statusTotals[analysisBucket] || 0) + amount;

      totalTitles += 1;
      totalRevenue += amount;
    });

    const months = Array.from(monthSet).sort();
    const rows = Array.from(grouped.values())
      .sort((a, b) => b.total - a.total);

    const totalsByMonth: Record<string, number> = {};
    months.forEach((m) => {
      totalsByMonth[m] = rows.reduce((acc, row) => acc + (row.valuesByMonth[m] || 0), 0);
    });

    return {
      months,
      rows,
      totalsByMonth,
      totalRevenue,
      totalTitles,
    };
  }, [analysisFilter, buildings, endDate, receivableTitles]);

  const periodText = useMemo(() => {
    if (startDate && endDate) {
      return `${format(startDate, 'dd/MM/yyyy')} até ${format(endDate, 'dd/MM/yyyy')}`;
    }
    return globalPeriodMode === 'all' ? 'Período total' : 'Últimos 3 meses';
  }, [endDate, globalPeriodMode, startDate]);

  const selectedCompanyLabel = selectedCompany === 'all'
    ? 'Todas as Empresas'
    : (companies.find((c: any) => String(c.id) === String(selectedCompany))?.name || 'Empresa');

  const selectedBuildingLabel = fcSelectedBuilding === 'all'
    ? 'Todas as Obras'
    : (buildings.find((b: any) => String(b.id) === String(fcSelectedBuilding))?.name || `Obra ${fcSelectedBuilding}`);

  const lastSyncLabel = syncSummary?.finished_at
    ? format(new Date(syncSummary.finished_at), 'dd/MM/yyyy HH:mm:ss')
    : cacheStats?.receber?.last_updated
      ? format(new Date(cacheStats.receber.last_updated), 'dd/MM/yyyy HH:mm:ss')
      : 'Sem atualização registrada';

  const updatedFiles = syncSummary?.updated_files || [];

  return (
    <motion.div
      key="db-fin"
      initial={{ opacity: 0, scale: 0.98 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.98 }}
      className="space-y-6"
    >
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3">
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader className="pb-2"><CardTitle className="text-[11px] uppercase tracking-widest text-gray-400">Faturamento Geral</CardTitle></CardHeader>
          <CardContent className="text-2xl font-black text-emerald-400">{fmtBRL(revenueData.totalRevenue)}</CardContent>
        </Card>
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader className="pb-2"><CardTitle className="text-[11px] uppercase tracking-widest text-gray-400">Obras com Receita</CardTitle></CardHeader>
          <CardContent className="text-2xl font-black text-white">{revenueData.rows.length}</CardContent>
        </Card>
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader className="pb-2"><CardTitle className="text-[11px] uppercase tracking-widest text-gray-400">Títulos a Receber</CardTitle></CardHeader>
          <CardContent className="text-2xl font-black text-white">{revenueData.totalTitles.toLocaleString('pt-BR')}</CardContent>
        </Card>
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader className="pb-2"><CardTitle className="text-[11px] uppercase tracking-widest text-gray-400">Período Ativo</CardTitle></CardHeader>
          <CardContent className="text-sm font-black text-orange-400">{periodText}</CardContent>
        </Card>
      </div>

      <Card className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden">
        <CardHeader className="border-b border-white/5">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <CardTitle className="text-white text-lg font-black uppercase tracking-tight flex items-center gap-2">
                <TrendingUp size={16} className="text-emerald-500" />
                Faturamento Geral por Obra
              </CardTitle>
              <div className="text-xs text-gray-400 flex flex-wrap gap-4 pt-1">
                <span className="inline-flex items-center gap-1"><CalendarDays size={12} /> {periodText}</span>
                <span className="inline-flex items-center gap-1"><Building2 size={12} /> {selectedCompanyLabel} / {selectedBuildingLabel}</span>
                <span className="inline-flex items-center gap-1"><FileText size={12} /> Base: fluxo do relatório financeiro do SIENGE (print 4)</span>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              {ANALYSIS_FILTER_OPTIONS.map((option) => (
                <button
                  key={option.key}
                  type="button"
                  onClick={() => setAnalysisFilter(option.key)}
                  className={[
                    'inline-flex items-center gap-2 rounded-full border px-3 py-2 text-sm font-semibold transition',
                    analysisFilter === option.key
                      ? 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300'
                      : 'border-white/10 bg-white/[0.03] text-gray-200 hover:bg-white/[0.06]',
                  ].join(' ')}
                >
                  <input
                    type="checkbox"
                    checked={analysisFilter === option.key}
                    readOnly
                    className="h-4 w-4 rounded border-white/20 bg-transparent text-emerald-500 focus:ring-emerald-500"
                  />
                  <span>{option.label}</span>
                </button>
              ))}
            </div>
          </div>
        </CardHeader>

        <CardContent className="p-0">
          <div className="overflow-auto max-h-[70vh] custom-scrollbar">
            <table className="w-full min-w-[900px] table-fixed border-separate border-spacing-0">
              <thead className="sticky top-0 z-10 bg-[#1a1a1d]">
                <tr>
                  <th className="text-left px-3 py-2 text-[11px] font-black uppercase tracking-wider text-gray-300 border-b border-white/10 w-[340px]">Obra</th>
                  {revenueData.months.map((m) => (
                    <th key={m} className="text-right px-3 py-2 text-[11px] font-black uppercase tracking-wider text-gray-300 border-b border-white/10 min-w-[140px]">
                      {monthLabel(m)}
                    </th>
                  ))}
                  <th className="text-right px-3 py-2 text-[11px] font-black uppercase tracking-wider text-emerald-300 border-b border-white/10 min-w-[150px]">Total</th>
                </tr>
              </thead>
              <tbody>
                {revenueData.rows.length === 0 ? (
                  <tr>
                    <td colSpan={revenueData.months.length + 2} className="px-4 py-10 text-center text-gray-400 text-sm">
                      Sem dados de faturamento para os filtros selecionados.
                    </td>
                  </tr>
                ) : revenueData.rows.map((row) => (
                  <tr key={row.buildingId} className="border-b border-white/5 hover:bg-white/[0.03]">
                    <td className="px-3 py-2 text-sm font-semibold text-white truncate" title={row.buildingName}>{row.buildingName}</td>
                    {revenueData.months.map((m) => (
                      <td key={`${row.buildingId}-${m}`} className="px-3 py-2 text-right text-sm text-gray-200 font-mono">
                        {row.valuesByMonth[m] ? fmtBRL(row.valuesByMonth[m]) : '-'}
                      </td>
                    ))}
                    <td className="px-3 py-2 text-right text-sm font-black text-emerald-300 font-mono">{fmtBRL(row.total)}</td>
                  </tr>
                ))}
              </tbody>
              {revenueData.rows.length > 0 && (
                <tfoot>
                  <tr className="bg-black/30">
                    <td className="px-3 py-3 text-sm font-black text-gray-100 uppercase">Total Faturamento</td>
                    {revenueData.months.map((m) => (
                      <td key={`total-${m}`} className="px-3 py-3 text-right text-sm font-black text-gray-100 font-mono">
                        {fmtBRL(revenueData.totalsByMonth[m] || 0)}
                      </td>
                    ))}
                    <td className="px-3 py-3 text-right text-sm font-black text-emerald-300 font-mono">{fmtBRL(revenueData.totalRevenue)}</td>
                  </tr>
                </tfoot>
              )}
            </table>
          </div>
        </CardContent>
      </Card>

      {syncMessage && (
        <Alert className={`bg-${syncMessage.startsWith('✓') ? 'emerald' : 'amber'}-950 border-${syncMessage.startsWith('✓') ? 'emerald' : 'amber'}-800`}>
          <AlertCircle size={16} className={`text-${syncMessage.startsWith('✓') ? 'emerald' : 'amber'}-500`} />
          <AlertDescription className={`text-${syncMessage.startsWith('✓') ? 'emerald' : 'amber'}-200`}>{syncMessage}</AlertDescription>
        </Alert>
      )}

      {syncDetailsOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
          <Card className="w-full max-w-4xl border-white/10 bg-[#111113] shadow-2xl">
            <CardHeader className="border-b border-white/5">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <CardTitle className="text-white text-lg font-black uppercase tracking-tight">Detalhamento da última atualização</CardTitle>
                  <div className="text-xs text-gray-400 mt-1">{syncSummary?.message || 'Sincronização concluída'}</div>
                </div>
                <Button type="button" variant="ghost" className="text-gray-300 hover:bg-white/10" onClick={() => setSyncDetailsOpen(false)}>
                  Fechar
                </Button>
              </div>
            </CardHeader>
            <CardContent className="p-4 space-y-4 max-h-[70vh] overflow-auto custom-scrollbar">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <div className="rounded-xl border border-white/5 bg-white/[0.03] p-3">
                  <div className="text-[10px] uppercase tracking-wider text-gray-400">Início</div>
                  <div className="mt-1 text-sm font-semibold text-white">
                    {syncSummary?.started_at ? format(new Date(syncSummary.started_at), 'dd/MM/yyyy HH:mm:ss') : '—'}
                  </div>
                </div>
                <div className="rounded-xl border border-white/5 bg-white/[0.03] p-3">
                  <div className="text-[10px] uppercase tracking-wider text-gray-400">Fim</div>
                  <div className="mt-1 text-sm font-semibold text-white">
                    {syncSummary?.finished_at ? format(new Date(syncSummary.finished_at), 'dd/MM/yyyy HH:mm:ss') : '—'}
                  </div>
                </div>
                <div className="rounded-xl border border-white/5 bg-white/[0.03] p-3">
                  <div className="text-[10px] uppercase tracking-wider text-gray-400">Arquivos atualizados</div>
                  <div className="mt-1 text-sm font-semibold text-emerald-300">{updatedFiles.length}</div>
                </div>
              </div>

              {updatedFiles.length === 0 ? (
                <div className="rounded-xl border border-white/5 bg-white/[0.02] p-4 text-sm text-gray-400">
                  Nenhum arquivo novo foi gerado nesta atualização.
                </div>
              ) : (
                <div className="overflow-hidden rounded-2xl border border-white/5">
                  <table className="w-full border-separate border-spacing-0 text-sm">
                    <thead className="bg-[#17171b] text-gray-300">
                      <tr>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Arquivo</th>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Dataset</th>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Motivo</th>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Período</th>
                      </tr>
                    </thead>
                    <tbody>
                      {updatedFiles.map((file) => (
                        <tr key={file.file_name} className="border-t border-white/5 odd:bg-white/[0.02]">
                          <td className="px-3 py-2 text-gray-100 font-mono text-xs break-all">{file.file_name}</td>
                          <td className="px-3 py-2 text-gray-300">{file.dataset}</td>
                          <td className="px-3 py-2 text-gray-300">{file.reason || '—'}</td>
                          <td className="px-3 py-2 text-gray-400 text-xs">{file.start_date || '—'} {file.end_date ? `até ${file.end_date}` : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </motion.div>
  );
}
