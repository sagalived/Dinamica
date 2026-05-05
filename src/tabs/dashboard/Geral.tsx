import React, { useEffect, useMemo, useState } from 'react';
import { motion } from 'framer-motion';
import { TrendingUp, DollarSign, Building2, Percent } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../components/ui/card';
import { Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { cn } from '../../lib/utils';
import { sienge as siengeApi } from '../../lib/api';
import {
  ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, Legend,
  PieChart, Pie, Cell
} from 'recharts';
import { useSienge } from '../../contexts/SiengeContext';
import { addMonths, format, parseISO } from 'date-fns';
import { toMoney, translateStatusLabel } from '../financeiro/logic';

import type { McByBuildingResponse } from './types';

export function DashboardGeral() {
  const ROWS_PER_PAGE = 10;
  const {
    orders,
    financialTitles,
    receivableTitles,
    nfeDocuments,
    fcSelectedBuilding,
    selectedCompany,
    selectedUser,
    selectedRequester,
    companies,
    buildings,
    dataRevision,
    activeBuildingCount,
    startDate,
    endDate,
  } = useSienge();

  const toNumberSafe = (value: any): number => {
    if (typeof value === 'number') return Number.isFinite(value) ? value : NaN;
    if (typeof value !== 'string') return Number(value);
    const raw = value.trim();
    if (!raw) return NaN;
    // aceita: "1234.56", "1.234,56", "1,234.56", "1234,56"
    const cleaned = raw
      .replace(/\s/g, '')
      .replace(/R\$/gi, '')
      .replace(/%/g, '');

    const hasComma = cleaned.includes(',');
    const hasDot = cleaned.includes('.');
    if (hasComma && hasDot) {
      // assume o separador decimal é o ÚLTIMO entre '.' e ','
      const lastComma = cleaned.lastIndexOf(',');
      const lastDot = cleaned.lastIndexOf('.');
      const decimalSep = lastComma > lastDot ? ',' : '.';
      const thousandsSep = decimalSep === ',' ? '.' : ',';
      const normalized = cleaned
        .split(thousandsSep).join('')
        .replace(decimalSep, '.');
      return Number(normalized);
    }
    if (hasComma && !hasDot) {
      return Number(cleaned.replace(',', '.'));
    }
    // só ponto, ou nenhum
    return Number(cleaned);
  };

  const normalizeText = (value: any): string => String(value || '').toLowerCase();

  const classifyOperationalFinance = (row: any): 'receita' | 'custo' | 'ignore' => {
    const type = normalizeText(row?.type ?? row?.tipoLancamento);
    const statementType = normalizeText(row?.statementType ?? row?.operationType);
    const origin = normalizeText(row?.statementOrigin ?? row?.origin);
    const nature = normalizeText(row?.nature ?? row?.natureza ?? row?.accountNature);

    if (
      statementType.includes('transf') ||
      statementType.includes('transfer') ||
      statementType.includes('saque') ||
      origin === 'bc'
    ) {
      return 'ignore';
    }

    if (
      type.includes('income') ||
      type.includes('entrada') ||
      statementType.includes('receb') ||
      nature.includes('receita')
    ) {
      return 'receita';
    }

    if (
      type.includes('expense') ||
      type.includes('saida') ||
      statementType.includes('pagamento') ||
      statementType.includes('despesa') ||
      nature.includes('custo')
    ) {
      return 'custo';
    }

    return 'ignore';
  };

  const isRevenueRow = (row: any, defaultRevenue: boolean): boolean => {
    const classification = classifyOperationalFinance(row);
    if (classification === 'receita') return true;
    if (classification === 'custo') return false;
    return defaultRevenue;
  };

  const getRowDate = (row: any): Date | null => {
    const numeric = Number(row?.dueDateNumeric) || 0;
    if (numeric) {
      const d = new Date(numeric);
      return Number.isNaN(d.getTime()) ? null : d;
    }
    const raw = String(
      row?.dataVencimento ?? row?.date ?? row?.operationDate ?? row?.paymentDate ?? row?.dueDate ?? row?.issueDate ?? ''
    ).trim();
    if (!raw || raw === '---') return null;
    try {
      const d = parseISO(raw);
      return Number.isNaN(d.getTime()) ? null : d;
    } catch {
      return null;
    }
  };

  const getRowAmount = (row: any): number => Math.abs(toMoney(row?.valor ?? row?.rawValue ?? row?.amount ?? row?.value ?? row?.netAmount ?? 0));

  const getEffectiveRange = () => {
    const now = new Date();
    const defaultEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const defaultStart = addMonths(defaultEnd, -12);
    return {
      start: startDate || defaultStart,
      end: endDate || startDate || defaultEnd,
    };
  };

  const formatMonthLabel = (d: Date) => {
    const month = d.toLocaleDateString('pt-BR', { month: 'short' }).replace('.', '');
    const year = String(d.getFullYear()).slice(-2);
    return `${month}/${year}`;
  };

  const [mcByBuildingResp, setMcByBuildingResp] = useState<McByBuildingResponse>({
    rows: [],
    total: { receita_operacional: 0, mc: 0, mc_percent: 0 },
  });
  const [mcByBuildingLoading, setMcByBuildingLoading] = useState(false);
  const [obraPage, setObraPage] = useState(1);

  // Dados históricos completos (sem filtro de data) para a tabela MC por Obra.
  const [historicalReceivables, setHistoricalReceivables] = useState<any[]>([]);
  const [historicalFinancials, setHistoricalFinancials] = useState<any[]>([]);
  const [historicalLoading, setHistoricalLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setHistoricalLoading(true);
      try {
        const params: any = {};
        if (selectedCompany !== 'all') params.company_id = selectedCompany;
        const resp = await siengeApi.get('/filtered', { params });
        if (cancelled) return;
        const data = resp.data ?? {};
        setHistoricalReceivables(Array.isArray(data.receber) ? data.receber : []);
        setHistoricalFinancials(Array.isArray(data.financeiro) ? data.financeiro : []);
      } catch {
        if (!cancelled) {
          setHistoricalReceivables([]);
          setHistoricalFinancials([]);
        }
      } finally {
        if (!cancelled) setHistoricalLoading(false);
      }
    };
    run();
    return () => { cancelled = true; };
  }, [selectedCompany, dataRevision]);

  useEffect(() => {
    let cancelled = false;

    const normalizeFilter = (value: any): string => {
      const str = String(value ?? '').trim();
      if (!str) return 'all';
      const lowered = str.toLowerCase();
      if (lowered === 'undefined' || lowered === 'null') return 'all';
      return str;
    };

    const run = async () => {
      setMcByBuildingLoading(true);
      try {
        const now = new Date();
        const defaultEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const defaultStart = addMonths(defaultEnd, -12);
        const effectiveStart = startDate || defaultStart;
        const effectiveEnd = endDate || startDate || defaultEnd;

        // Rows (Top 5) não sofrem influência de filtro.
        const rowsParams: any = {
          company_id: 'all',
          building_id: 'all',
          user_id: 'all',
          requester_id: 'all',
          start_date: format(effectiveStart, 'yyyy-MM-dd'),
          end_date: format(effectiveEnd, 'yyyy-MM-dd'),
          top: 5,
          time_budget_seconds: 15,
          max_concurrency: 4,
        };
        // Total respeita filtros (empresa/obra/usuário/solicitante), mas sem data.
        const totalParams: any = {
          company_id: normalizeFilter(selectedCompany),
          building_id: normalizeFilter(fcSelectedBuilding),
          user_id: normalizeFilter(selectedUser),
          requester_id: normalizeFilter(selectedRequester),
          start_date: format(effectiveStart, 'yyyy-MM-dd'),
          end_date: format(effectiveEnd, 'yyyy-MM-dd'),
          top: 5,
          time_budget_seconds: 15,
          max_concurrency: 4,
        };

        const [{ data: rowsData }, { data: totalData }] = await Promise.all([
          siengeApi.get('/mc-by-building', { params: rowsParams }),
          siengeApi.get('/mc-by-building', { params: totalParams }),
        ]);
        if (cancelled) return;
        setMcByBuildingResp({
          rows: Array.isArray(rowsData?.rows) ? rowsData.rows : [],
          total: totalData?.total || { receita_operacional: 0, mc: 0, mc_percent: 0 },
          diagnostic: {
            rows: rowsData?.diagnostic,
            total: totalData?.diagnostic,
          },
        });
      } catch {
        if (cancelled) return;
        setMcByBuildingResp({ rows: [], total: { receita_operacional: 0, mc: 0, mc_percent: 0 } });
      } finally {
        if (!cancelled) setMcByBuildingLoading(false);
      }
    };

    run();
    return () => {
      cancelled = true;
    };
  }, [dataRevision, endDate, fcSelectedBuilding, selectedCompany, selectedRequester, selectedUser, startDate]);

  const stats = useMemo(() => {
    const ordersArray = Array.isArray(orders) ? orders : [];
    const total = ordersArray.reduce((acc: number, curr: any) => acc + toMoney(curr.totalAmount), 0);
    const avg = ordersArray.length > 0 ? total / ordersArray.length : 0;

    let receitaOperacional = 0;
    let cpv = 0;

    (Array.isArray(receivableTitles) ? receivableTitles : []).forEach((row: any) => {
      const amount = getRowAmount(row);
      if (amount <= 0) return;
      if (isRevenueRow(row, true)) receitaOperacional += amount;
      else cpv += amount;
    });

    (Array.isArray(financialTitles) ? financialTitles : []).forEach((row: any) => {
      const amount = getRowAmount(row);
      if (amount <= 0) return;
      if (isRevenueRow(row, false)) receitaOperacional += amount;
      else cpv += amount;
    });
    
    const fTotal = financialTitles.reduce((acc: number, curr: any) => acc + toMoney(curr.amount), 0);
    const rTotal = receivableTitles.reduce((acc: number, curr: any) => acc + toMoney(curr.amount), 0);
    const balance = rTotal - fTotal;

    const margemContribuicao = receitaOperacional - cpv;

    return { total, avg, receitaOperacional, fTotal, rTotal, balance, cpv, margemContribuicao };
  }, [orders, financialTitles, receivableTitles]);

  const hasUsableMcByBuildingTotals = useMemo(() => {
    const rows = Array.isArray(mcByBuildingResp.rows) ? mcByBuildingResp.rows : [];
    if (rows.length > 0) return true;
    const receita = toNumberSafe(mcByBuildingResp.total?.receita_operacional);
    const mc = toNumberSafe(mcByBuildingResp.total?.mc);
    return Math.abs(receita || 0) > 0 || Math.abs(mc || 0) > 0;
  }, [mcByBuildingResp.rows, mcByBuildingResp.total]);

  const receitaMargemSeries = useMemo(() => {
    const monthKey = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    const { start: effectiveStart, end: effectiveEnd } = getEffectiveRange();

    const startMonth = new Date(effectiveStart.getFullYear(), effectiveStart.getMonth(), 1);
    const endMonth = new Date(effectiveEnd.getFullYear(), effectiveEnd.getMonth(), 1);
    const months: { key: string; label: string }[] = [];
    for (let d = new Date(startMonth); d.getTime() <= endMonth.getTime(); d = new Date(d.getFullYear(), d.getMonth() + 1, 1)) {
      months.push({ key: monthKey(d), label: formatMonthLabel(d) });
      // segurança: evita loop infinito em caso de data inválida
      if (months.length > 48) break;
    }

    const receitaByMonth: Record<string, number> = {};
    const cpvByMonth: Record<string, number> = {};

    (Array.isArray(receivableTitles) ? receivableTitles : []).forEach((t: any) => {
      const d = getRowDate(t);
      if (!d) return;
      const key = monthKey(d);
      const amount = getRowAmount(t);
      if (amount <= 0) return;
      if (isRevenueRow(t, true)) receitaByMonth[key] = (receitaByMonth[key] || 0) + amount;
      else cpvByMonth[key] = (cpvByMonth[key] || 0) + amount;
    });

    (Array.isArray(financialTitles) ? financialTitles : []).forEach((t: any) => {
      const d = getRowDate(t);
      if (!d) return;
      const key = monthKey(d);
      const amount = getRowAmount(t);
      if (amount <= 0) return;
      if (isRevenueRow(t, false)) receitaByMonth[key] = (receitaByMonth[key] || 0) + amount;
      else cpvByMonth[key] = (cpvByMonth[key] || 0) + amount;
    });

    const receitaChart = months.map((m) => ({ name: m.label, valor: receitaByMonth[m.key] || 0 }));
    const margemChart = months.map((m) => ({
      name: m.label,
      valor: (receitaByMonth[m.key] || 0) - (cpvByMonth[m.key] || 0),
    }));

    const mcPercentChart = months.map((m) => {
      const receita = receitaByMonth[m.key] || 0;
      const margem = receita - (cpvByMonth[m.key] || 0);
      const pct = receita > 0 ? (margem / receita) * 100 : 0;
      return { name: m.label, valor: pct };
    });

    const bestMonth = (series: { name: string; valor: number }[]) => {
      if (!series.length) return { name: '-', valor: 0 };
      return series.reduce((best, row) => (row.valor > best.valor ? row : best), series[0]);
    };

    return {
      receitaChart,
      margemChart,
      mcPercentChart,
      bestReceita: bestMonth(receitaChart),
      bestMargem: bestMonth(margemChart),
      bestMcPercent: bestMonth(mcPercentChart),
    };
  }, [endDate, financialTitles, receivableTitles, startDate]);

  const seriesTotals = useMemo(() => {
    const receita = (receitaMargemSeries.receitaChart || []).reduce((acc, row) => acc + Number(row?.valor || 0), 0);
    const margem = (receitaMargemSeries.margemChart || []).reduce((acc, row) => acc + Number(row?.valor || 0), 0);
    const mcPercent = receita > 0 ? (margem / receita) * 100 : 0;
    return { receita, margem, mcPercent };
  }, [receitaMargemSeries]);

  const mcGeralPercent = useMemo(() => {
    const receita = Number(seriesTotals.receita || 0);
    const mc = Number(seriesTotals.margem || 0);
    return receita > 0 ? (mc / receita) * 100 : 0;
  }, [seriesTotals]);

  const resumoPorObra = useMemo(() => {
    const buildingNameMap: Record<string, string> = {};
    const scopedBuildings = (Array.isArray(buildings) ? buildings : []).filter((b: any) => {
      if (selectedCompany === 'all') return true;
      return String(b?.companyId ?? '') === String(selectedCompany);
    });

    scopedBuildings.forEach((b: any) => {
      const id = String(b?.id ?? '');
      if (id) buildingNameMap[id] = String(b?.name || `Obra ${id}`);
      const code = String(b?.code ?? '');
      if (code) buildingNameMap[code] = String(b?.name || `Obra ${code}`);
    });

    const byBuilding = new Map<string, { id: string; name: string; receita: number; custo: number }>();

    // Inicializa com todas as obras do escopo para sempre listar valores discriminados.
    scopedBuildings.forEach((b: any) => {
      const id = String(b?.id ?? '');
      if (!id) return;
      byBuilding.set(id, {
        id,
        name: String(b?.name || `Obra ${id}`),
        receita: 0,
        custo: 0,
      });
    });

    const passesCompany = (row: any): boolean => {
      if (selectedCompany === 'all') return true;
      const rowCompany = String(row?.companyId ?? '').trim();
      if (rowCompany) return rowCompany === String(selectedCompany);
      const rawBuilding = String(row?.idObra ?? row?.buildingId ?? row?.codigoObra ?? row?.buildingCode ?? '').trim();
      if (!rawBuilding) return false;
      const building = (Array.isArray(buildings) ? buildings : []).find(
        (b: any) => String(b?.id) === rawBuilding || String(b?.code ?? '') === rawBuilding
      );
      return String(building?.companyId ?? '') === String(selectedCompany);
    };

    const consumeRow = (row: any, defaultRevenue: boolean) => {
      if (!passesCompany(row)) return;

      const amount = getRowAmount(row);
      if (amount <= 0) return;

      const rawId = String(row?.idObra ?? row?.buildingId ?? row?.codigoObra ?? row?.buildingCode ?? '').trim();
      const id = rawId || '0';
      const name = String(row?.nomeObra || buildingNameMap[id] || (id === '0' ? 'Sem obra' : `Obra ${id}`));

      const current = byBuilding.get(id) || { id, name, receita: 0, custo: 0 };
      if (isRevenueRow(row, defaultRevenue)) current.receita += amount;
      else current.custo += amount;
      byBuilding.set(id, current);
    };

    // Usa dados históricos completos (sem filtro de data) para exibir acumulado total por obra.
    (Array.isArray(historicalReceivables) ? historicalReceivables : []).forEach((row: any) => consumeRow(row, true));
    (Array.isArray(historicalFinancials) ? historicalFinancials : []).forEach((row: any) => consumeRow(row, false));

    const normalizedRows = Array.from(byBuilding.values())
      .map((r) => {
        const mc = r.receita - r.custo;
        const pct = r.receita > 0 ? (mc / r.receita) * 100 : 0;
        return { id: r.id, name: r.name, receita: r.receita, mc, pct };
      })
      .sort((a, b) => {
        if (b.receita !== a.receita) return b.receita - a.receita;
        return a.name.localeCompare(b.name);
      });

    const totalReceita = normalizedRows.reduce((acc, r) => acc + r.receita, 0);
    const totalMc = normalizedRows.reduce((acc, r) => acc + r.mc, 0);

    return {
      rows: normalizedRows,
      total: {
        receita: totalReceita,
        mc: totalMc,
        pct: totalReceita > 0 ? (totalMc / totalReceita) * 100 : 0,
      },
      maxReceita: Math.max(1, ...normalizedRows.map((r) => r.receita)),
      maxMcAbs: Math.max(1, ...normalizedRows.map((r) => Math.abs(r.mc))),
    };
  }, [historicalReceivables, historicalFinancials, buildings, selectedCompany]);

  const orderStatusData = useMemo(() => {
    const map: Record<string, number> = {};
    const ordersArray = Array.isArray(orders) ? orders : [];
    ordersArray.forEach((o: any) => {
      const status = translateStatusLabel(o.status) || 'N/D';
      map[status] = (map[status] || 0) + 1;
    });
    return Object.entries(map).map(([name, value]) => ({ name, value })).sort((a,b) => b.value - a.value);
  }, [orders]);

  const kpiReceitaOperacional = useMemo(() => {
    return Number(seriesTotals.receita || 0);
  }, [seriesTotals.receita]);

  const kpiMargemContribuicao = useMemo(() => {
    return Number(seriesTotals.margem || 0);
  }, [seriesTotals.margem]);

  const obraPagination = useMemo(() => {
    const totalRows = resumoPorObra.rows.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / ROWS_PER_PAGE));
    const safePage = Math.min(Math.max(1, obraPage), totalPages);
    const start = (safePage - 1) * ROWS_PER_PAGE;
    const end = start + ROWS_PER_PAGE;
    return {
      safePage,
      totalPages,
      pageRows: resumoPorObra.rows.slice(start, end),
      start,
      end: Math.min(end, totalRows),
      totalRows,
    };
  }, [obraPage, resumoPorObra.rows]);

  useEffect(() => {
    setObraPage(1);
  }, [selectedCompany, resumoPorObra.rows.length]);

  return (
    <motion.div key="db-geral" initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -20 }} className="space-y-8">
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 sm:gap-6">
        {[
          {
            label: selectedCompany !== 'all'
              ? `RECEITA — ${companies.find((c: any) => String(c.id) === selectedCompany)?.name || 'Empresa'}`
              : 'RECEITA OPERACIONAL',
            value: `R$ ${kpiReceitaOperacional.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`,
            icon: TrendingUp,
            color: 'orange',
            tooltip: 'Receita Operacional calculada pelos lançamentos filtrados (empresa, obra e período).',
          },
          {
            label: selectedCompany !== 'all'
              ? `MARGEM — ${companies.find((c: any) => String(c.id) === selectedCompany)?.name || 'Empresa'}`
              : 'Margem de Contribuição',
            value: `R$ ${kpiMargemContribuicao.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`,
            icon: DollarSign,
            color: kpiMargemContribuicao >= 0 ? 'green' : 'red',
            tooltip: 'Margem de Contribuição (MC): Receita Operacional − Custos, respeitando os filtros aplicados.',
          },
          { label: 'Obras Ativas', value: activeBuildingCount, icon: Building2, color: 'orange' }
        ].map((kpi, i) => (
          <Card key={i} className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative group" title={(kpi as any).tooltip || ''}>
            <div className="absolute top-0 right-0 p-3 opacity-10 group-hover:opacity-20 transition-opacity"><kpi.icon size={40} className="text-orange-500" /></div>
            <CardHeader className="pb-2 p-4 sm:p-6">
              <CardDescription className="text-[9px] sm:text-[10px] font-black uppercase tracking-widest text-orange-500/70 leading-tight">{kpi.label}</CardDescription>
              <CardTitle className={cn("text-xl sm:text-3xl font-black tracking-tighter mt-1", kpi.color === 'red' ? 'text-red-500' : kpi.color === 'green' ? 'text-green-500' : 'text-white')}>{kpi.value}</CardTitle>
            </CardHeader>
            <div className="h-1 w-full bg-orange-600/20"><div className="h-full bg-orange-600 w-1/3" /></div>
          </Card>
        ))}
      </div>

      {/* Mini gráficos (estilo referência) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-8">
        <Card
          className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative"
          title="Receita Operacional (base NF). Fórmula: Receita = Σ(NF) no período filtrado. Melhor mês = maior valor mensal de receita."
        >
          <div className="absolute top-0 right-0 p-4 opacity-10"><TrendingUp size={52} className="text-orange-500" /></div>
          <CardHeader className="pb-2 p-4 sm:p-6">
            <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Receita Operacional</CardTitle>
            <div className="flex items-center justify-between text-[11px] text-gray-400">
              <span>Melhor mês: {receitaMargemSeries.bestReceita.name}</span>
              <span className="font-black text-orange-500">R$ {receitaMargemSeries.bestReceita.valor.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</span>
            </div>
            <div className="text-2xl sm:text-4xl font-black tracking-tighter text-white">R$ {kpiReceitaOperacional.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</div>
          </CardHeader>
          <CardContent className="h-[150px] sm:h-[180px] pt-2 pb-4">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={receitaMargemSeries.receitaChart} margin={{ top: 8, right: 8, left: 10, bottom: 0 }}>
                <defs>
                  <linearGradient id="miniReceita" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f97316" stopOpacity={0.35} />
                    <stop offset="95%" stopColor="#f97316" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="name" axisLine={false} tickLine={false} interval={0} tickMargin={8} padding={{ left: 8, right: 8 }} tick={{ fill: '#666', fontSize: 11 }} />
                <YAxis hide />
                <Tooltip
                  labelFormatter={(label: string) => `Mes: ${label}`}
                  formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)}
                  contentStyle={{ backgroundColor: '#161618', border: '1px solid rgba(255,255,255,0.1)' }}
                />
                <Area type="monotone" dataKey="valor" stroke="#f97316" strokeWidth={3} fill="url(#miniReceita)" dot={{ r: 2 }} activeDot={{ r: 4 }} />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
          <div className="h-1 w-full bg-orange-600/20"><div className="h-full bg-orange-600 w-1/3" /></div>
        </Card>

        <Card
          className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative"
          title="Margem de Contribuição (MC). Fórmula: MC = Receita Líquida − Custos. Aqui, custos ≈ Σ(despesas do extrato: type=Expense) no período filtrado. Melhor mês = maior MC mensal."
        >
          <div className="absolute top-0 right-0 p-4 opacity-10"><DollarSign size={52} className="text-emerald-400" /></div>
          <CardHeader className="pb-2 p-4 sm:p-6">
            <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Margem de Contribuição</CardTitle>
            <div className="flex items-center justify-between text-[11px] text-gray-400">
              <span>Melhor mês: {receitaMargemSeries.bestMargem.name}</span>
              <span className={cn('font-black', receitaMargemSeries.bestMargem.valor >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                R$ {receitaMargemSeries.bestMargem.valor.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}
              </span>
            </div>
            <div className={cn(
              'text-2xl sm:text-4xl font-black tracking-tighter',
              kpiMargemContribuicao >= 0 ? 'text-emerald-400' : 'text-red-500'
            )}>R$ {kpiMargemContribuicao.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</div>
          </CardHeader>
          <CardContent className="h-[150px] sm:h-[180px] pt-2 pb-4">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={receitaMargemSeries.margemChart} margin={{ top: 8, right: 8, left: 10, bottom: 0 }}>
                <defs>
                  <linearGradient id="miniMargem" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#10b981" stopOpacity={0.35} />
                    <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="name" axisLine={false} tickLine={false} interval={0} tickMargin={8} padding={{ left: 8, right: 8 }} tick={{ fill: '#666', fontSize: 11 }} />
                <YAxis hide />
                <Tooltip
                  labelFormatter={(label: string) => `Mes: ${label}`}
                  formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)}
                  contentStyle={{ backgroundColor: '#161618', border: '1px solid rgba(255,255,255,0.1)' }}
                />
                <Area type="monotone" dataKey="valor" stroke="#10b981" strokeWidth={3} fill="url(#miniMargem)" dot={{ r: 2 }} activeDot={{ r: 4 }} />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
          <div className="h-1 w-full bg-emerald-600/20"><div className="h-full bg-emerald-500 w-1/3" /></div>
        </Card>

        <Card
          className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative"
          title="MC Geral (%). Fórmula: MC% = (MC / Receita Líquida) × 100, onde MC = Receita Líquida − (Custos Variáveis + Custos Diretos). Melhor mês = maior MC% mensal."
        >
          <div className="absolute top-0 right-0 p-4 opacity-10"><Percent size={52} className="text-violet-400" /></div>
          <CardHeader className="pb-2 p-4 sm:p-6">
            <CardTitle className="text-lg font-black uppercase tracking-tight text-white">MC Geral (%)</CardTitle>
            <div className="flex items-center justify-between text-[11px] text-gray-400">
              <span>Melhor mês: {receitaMargemSeries.bestMcPercent.name}</span>
              <span className="font-black text-violet-400">{receitaMargemSeries.bestMcPercent.valor.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%</span>
            </div>
            <div className={cn(
              'text-2xl sm:text-4xl font-black tracking-tighter',
              mcGeralPercent >= 0 ? 'text-white' : 'text-red-300'
            )}>{mcGeralPercent.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%</div>
          </CardHeader>
          <CardContent className="h-[150px] sm:h-[180px] pt-2 pb-4">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={receitaMargemSeries.mcPercentChart} margin={{ top: 8, right: 8, left: 10, bottom: 0 }}>
                <defs>
                  <linearGradient id="miniMcPct" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.35} />
                    <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="name" axisLine={false} tickLine={false} interval={0} tickMargin={8} padding={{ left: 8, right: 8 }} tick={{ fill: '#666', fontSize: 11 }} />
                <YAxis hide />
                <Tooltip
                  labelFormatter={(label: string) => `Mes: ${label}`}
                  formatter={(value: number) => `${Number(value || 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%`}
                  contentStyle={{ backgroundColor: '#161618', border: '1px solid rgba(255,255,255,0.1)' }}
                />
                <Area type="monotone" dataKey="valor" stroke="#8b5cf6" strokeWidth={3} fill="url(#miniMcPct)" dot={{ r: 2 }} activeDot={{ r: 4 }} />
              </AreaChart>
            </ResponsiveContainer>
          </CardContent>
          <div className="h-1 w-full bg-violet-600/20"><div className="h-full bg-violet-500 w-1/3" /></div>
        </Card>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-8">
        <Card className="lg:col-span-2 bg-[#161618] border-white/5 shadow-2xl" title="Resumo por Obra (todo o período). Receita Operacional = soma de NF (títulos a receber). MC = Receita Líquida − (Custos Variáveis + Custos Diretos) (aqui aproximado pelos títulos a pagar/CPV). %MC = MC / Receita Líquida × 100.">
          <CardHeader>
            <CardTitle className="text-lg font-black uppercase tracking-tight text-white">MC por Obra</CardTitle>
          </CardHeader>
          <CardContent className="pt-2">
            <Table className="text-xs sm:text-sm">
              <TableHeader className="border-white/10">
                <TableRow className="border-white/10 hover:bg-transparent">
                  <TableHead className="text-gray-300">Obra</TableHead>
                  <TableHead className="text-gray-300">Receita Operacional</TableHead>
                  <TableHead className="text-gray-300">Margem Contribuição</TableHead>
                  <TableHead className="text-gray-300 text-right">% MC</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {historicalLoading ? (
                  <TableRow className="border-white/10 hover:bg-white/5">
                    <TableCell colSpan={4} className="text-gray-300">
                      Carregando dados históricos por obra...
                    </TableCell>
                  </TableRow>
                ) : resumoPorObra.rows.length === 0 ? (
                  <TableRow className="border-white/10 hover:bg-white/5">
                    <TableCell colSpan={4} className="text-gray-300">
                      Sem dados por obra.
                    </TableCell>
                  </TableRow>
                ) : (
                  obraPagination.pageRows.map((r) => (
                  <TableRow key={r.id} className="border-white/10 hover:bg-white/5">
                    <TableCell className="text-white font-semibold max-w-[220px] truncate">{r.name}</TableCell>

                    <TableCell>
                      <div className="relative h-7 rounded bg-white/5 overflow-hidden">
                        <div className="absolute inset-y-0 left-0 bg-orange-500/30" style={{ width: `${Math.round((r.receita / resumoPorObra.maxReceita) * 100)}%` }} />
                        <div className="relative z-10 px-2 h-full flex items-center justify-end text-white font-semibold">
                          {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(r.receita)}
                        </div>
                      </div>
                    </TableCell>

                    <TableCell>
                      <div className="relative h-7 rounded bg-white/5 overflow-hidden">
                        <div
                          className={cn('absolute inset-y-0 left-0', r.mc >= 0 ? 'bg-emerald-500/30' : 'bg-red-500/30')}
                          style={{ width: `${Math.round((Math.abs(r.mc) / resumoPorObra.maxMcAbs) * 100)}%` }}
                        />
                        <div className={cn(
                          'relative z-10 px-2 h-full flex items-center justify-end font-semibold',
                          r.mc >= 0 ? 'text-emerald-400' : 'text-red-400'
                        )}>
                          {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(r.mc)}
                        </div>
                      </div>
                    </TableCell>

                    <TableCell className="text-right font-bold">
                      <span className={cn(
                        'inline-flex items-center justify-center min-w-[56px] px-2 py-1 rounded',
                        r.pct >= 0 ? 'bg-violet-500/15 text-violet-300' : 'bg-red-500/15 text-red-300'
                      )}>
                        {r.pct.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}%
                      </span>
                    </TableCell>
                  </TableRow>
                  ))
                )}
              </TableBody>
              <TableFooter className="bg-transparent border-white/10">
                <TableRow className="border-white/10 hover:bg-transparent">
                  <TableCell className="text-white font-black">Total</TableCell>
                  <TableCell className="text-white font-black">
                    {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(resumoPorObra.total.receita)}
                  </TableCell>
                  <TableCell className={cn('font-black', resumoPorObra.total.mc >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                    {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(resumoPorObra.total.mc)}
                  </TableCell>
                  <TableCell className="text-right text-white font-black">
                    {resumoPorObra.total.pct.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}%
                  </TableCell>
                </TableRow>
              </TableFooter>
            </Table>
            {resumoPorObra.rows.length > 0 && (
              <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-xs text-gray-400">
                <span>
                  Mostrando {obraPagination.start + 1}-{obraPagination.end} de {obraPagination.totalRows} obras
                </span>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setObraPage((p) => Math.max(1, p - 1))}
                    disabled={obraPagination.safePage <= 1}
                    className="h-8 px-3 rounded-lg border border-white/10 bg-white/5 text-white disabled:opacity-40"
                  >
                    Anterior
                  </button>
                  <span className="min-w-[84px] text-center text-gray-300">
                    Pagina {obraPagination.safePage} / {obraPagination.totalPages}
                  </span>
                  <button
                    type="button"
                    onClick={() => setObraPage((p) => Math.min(obraPagination.totalPages, p + 1))}
                    disabled={obraPagination.safePage >= obraPagination.totalPages}
                    className="h-8 px-3 rounded-lg border border-white/10 bg-white/5 text-white disabled:opacity-40"
                  >
                    Proxima
                  </button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Status dos Pedidos</CardTitle></CardHeader>
          <CardContent className="h-[220px] sm:h-[350px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={orderStatusData} cx="50%" cy="50%" innerRadius={60} outerRadius={80} paddingAngle={5} dataKey="value">
                  {orderStatusData.map((e, index) => <Cell key={index} fill={['#f97316', '#3b82f6', '#10b981', '#f59e0b', '#6366f1'][index % 5]} />)}
                </Pie>
                <Tooltip contentStyle={{ backgroundColor: '#161618', border: 'none', borderRadius: '8px' }} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>
    </motion.div>
  );
}
