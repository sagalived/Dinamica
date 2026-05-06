import React, { useEffect, useMemo, useState } from 'react';
import { motion } from 'framer-motion';
import { TrendingUp, DollarSign, Building2, Percent } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../components/ui/card';
import { Table, TableBody, TableCell, TableFooter, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { cn } from '../../lib/utils';
import { api } from '../../lib/api';
import {
  ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, Legend,
  PieChart, Pie, Cell
} from 'recharts';
import { useSienge } from '../../contexts/SiengeContext';
import { addMonths, format, parseISO } from 'date-fns';
import { toMoney, translateStatusLabel } from '../financeiro/logic';

type OperationalSeriesRow = {
  month: string; // YYYY-MM
  receita_operacional: number;
  custo_variavel: number;
  mc: number;
  mc_percent: number;
};

type OperationalSeriesResponse = {
  range: { start: string; end: string };
  rows: OperationalSeriesRow[];
  total: {
    receita_operacional: number;
    custo_variavel: number;
    mc: number;
    mc_percent: number;
  };
};

type McByBuildingRow = {
  id: string;
  name: string;
  receita: number;
  mc: number;
  pct: number;
};

type McByBuildingAllTimeResponse = {
  rows: McByBuildingRow[];
  total: { receita: number; mc: number; pct: number };
};

export function DashboardGeral() {
  const ROWS_PER_PAGE = 10;
  const {
    orders,
    financialTitles,
    receivableTitles,
    nfeDocuments,
    activeBuildings,
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

  const [activeBuildingsModalOpen, setActiveBuildingsModalOpen] = useState(false);

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

  const fmtBRL = (n: number) => new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number.isFinite(n) ? n : 0);

  const [operationalSeries, setOperationalSeries] = useState<OperationalSeriesResponse>({
    range: { start: '', end: '' },
    rows: [],
    total: { receita_operacional: 0, custo_variavel: 0, mc: 0, mc_percent: 0 },
  });
  const [operationalSeriesLoading, setOperationalSeriesLoading] = useState(false);

  const [obraPage, setObraPage] = useState(1);

  const [mcByBuildingAllTime, setMcByBuildingAllTime] = useState<McByBuildingAllTimeResponse>({
    rows: [],
    total: { receita: 0, mc: 0, pct: 0 },
  });
  const [historicalLoading, setHistoricalLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setHistoricalLoading(true);
      try {
        const params: any = {};
        if (selectedCompany !== 'all') params.company_id = selectedCompany;
        const { data } = await api.get('/operational/mc-by-building', { params });
        if (cancelled) return;
        setMcByBuildingAllTime({
          rows: Array.isArray(data?.rows) ? data.rows : [],
          total: data?.total || { receita: 0, mc: 0, pct: 0 },
        });
      } catch {
        if (!cancelled) setMcByBuildingAllTime({ rows: [], total: { receita: 0, mc: 0, pct: 0 } });
      } finally {
        if (!cancelled) setHistoricalLoading(false);
      }
    };
    run();
    return () => { cancelled = true; };
  }, [selectedCompany, dataRevision]);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      try {
        setOperationalSeriesLoading(true);
        const range = getEffectiveRange();
        const params: any = {
          start_date: format(range.start, 'yyyy-MM-dd'),
          end_date: format(range.end, 'yyyy-MM-dd'),
        };
        if (selectedCompany !== 'all') params.company_id = selectedCompany;
        if (fcSelectedBuilding !== 'all') params.building_id = fcSelectedBuilding;

        const { data } = await api.get('/operational/series', { params });
        if (cancelled) return;
        setOperationalSeries({
          range: data?.range || { start: '', end: '' },
          rows: Array.isArray(data?.rows) ? data.rows : [],
          total: data?.total || { receita_operacional: 0, custo_variavel: 0, mc: 0, mc_percent: 0 },
        });
      } catch {
        if (cancelled) return;
        setOperationalSeries({
          range: { start: '', end: '' },
          rows: [],
          total: { receita_operacional: 0, custo_variavel: 0, mc: 0, mc_percent: 0 },
        });
      } finally {
        if (!cancelled) setOperationalSeriesLoading(false);
      }
    };

    run();
    return () => {
      cancelled = true;
    };
  }, [dataRevision, endDate, fcSelectedBuilding, selectedCompany, startDate]);

  const receitaMargemSeries = useMemo(() => {
    const monthToLabel = (m: string) => {
      const s = String(m || '').trim();
      const match = s.match(/^(\d{4})-(\d{2})$/);
      if (!match) return s || '-';
      const y = Number(match[1]);
      const mo = Number(match[2]);
      const d = new Date(y, Math.max(0, mo - 1), 1);
      return formatMonthLabel(d);
    };

    const rows = Array.isArray(operationalSeries.rows) ? operationalSeries.rows : [];
    const receitaChart = rows.map((r) => ({ name: monthToLabel(r.month), valor: Number(r.receita_operacional || 0) }));
    const margemChart = rows.map((r) => ({ name: monthToLabel(r.month), valor: Number(r.mc || 0) }));
    const mcPercentChart = rows.map((r) => ({ name: monthToLabel(r.month), valor: Number(r.mc_percent || 0) }));

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
  }, [operationalSeries.rows, endDate, startDate]);

  const seriesTotals = useMemo(() => {
    const receita = Number(operationalSeries.total?.receita_operacional || 0);
    const margem = Number(operationalSeries.total?.mc || 0);
    const mcPercent = receita > 0 ? (margem / receita) * 100 : 0;
    return { receita, margem, mcPercent };
  }, [operationalSeries.total]);

  const mcGeralPercent = useMemo(() => {
    const receita = Number(seriesTotals.receita || 0);
    const mc = Number(seriesTotals.margem || 0);
    return receita > 0 ? (mc / receita) * 100 : 0;
  }, [seriesTotals]);

  const resumoPorObra = useMemo(() => {
    const rows = Array.isArray(mcByBuildingAllTime.rows) ? mcByBuildingAllTime.rows : [];
    const total = mcByBuildingAllTime.total || { receita: 0, mc: 0, pct: 0 };
    return {
      rows,
      total,
      maxReceita: Math.max(1, ...rows.map((r) => Number(r?.receita || 0))),
      maxMcAbs: Math.max(1, ...rows.map((r) => Math.abs(Number(r?.mc || 0)))),
    };
  }, [mcByBuildingAllTime.rows, mcByBuildingAllTime.total]);

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

  const printMcReport = () => {
    const escapeHtml = (value: any) => String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');

    const selectedCompanyName = selectedCompany === 'all'
      ? 'Todas'
      : (companies?.find((c: any) => String(c.id) === String(selectedCompany))?.name || selectedCompany);

    const selectedBuildingName = fcSelectedBuilding === 'all'
      ? 'Todas'
      : (buildings?.find((b: any) => String(b.id) === String(fcSelectedBuilding))?.name || fcSelectedBuilding);

    const range = getEffectiveRange();
    const periodLabel = `${format(range.start, 'dd/MM/yyyy')} até ${format(range.end, 'dd/MM/yyyy')}`;

    const fmtMoney = (n: number) => new Intl.NumberFormat('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n || 0);

    const resolveBuildingNameByIdOrCode = (idOrCode: string): string => {
      const id = String(idOrCode ?? '').trim();
      if (!id) return '';
      const found = (Array.isArray(buildings) ? buildings : []).find((b: any) => (
        String(b?.id) === id || String(b?.code ?? '') === id
      ));
      return found?.name ? String(found.name) : id;
    };

    const extractNfeBuildingId = (doc: any): string => {
      const candidates = [
        doc?.buildingId,
        doc?.building_id,
        doc?.enterpriseId,
        doc?.enterprise_id,
        doc?.constructionId,
        doc?.obraId,
        doc?.obra_id,
        doc?.buildingCode,
        doc?.building_code,
        doc?.codigoObra,
        doc?.codigoVisivelObra,
        doc?.costCenterCode,
        doc?.costCenter,
      ];
      for (const c of candidates) {
        const s = String(c ?? '').trim();
        if (s && s !== 'undefined' && s !== 'null' && s !== 'None') return s;
      }
      return '';
    };

    const getNfeDate = (doc: any): string => {
      const raw = doc?.issueDate ?? doc?.emissionDate ?? doc?.dataEmissao ?? doc?.date ?? doc?.createdAt ?? '';
      const s = String(raw || '').trim();
      if (!s) return '';
      try {
        const d = parseISO(s);
        if (!Number.isNaN(d.getTime())) return format(d, 'dd/MM/yyyy');
      } catch {
        // ignore
      }
      const m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (m) return `${m[3]}/${m[2]}/${m[1]}`;
      return s;
    };

    const getNfeNumber = (doc: any): string => {
      return String(doc?.number ?? doc?.invoiceNumber ?? doc?.documentNumber ?? doc?.id ?? '').trim();
    };

    const getNfeAmount = (doc: any): number => {
      const raw = doc?.totalAmount ?? doc?.totalInvoiceAmount ?? doc?.amount ?? doc?.value ?? doc?.valor ?? doc?.valorTotal;
      const n = Number(raw ?? 0);
      return Number.isFinite(n) ? n : 0;
    };

    const mcRowsHtml = (Array.isArray(resumoPorObra.rows) ? resumoPorObra.rows : []).map((r: any) => {
      const pct = Number(r?.pct ?? 0);
      return (
        '<tr>' +
          `<td>${escapeHtml(r?.name ?? '')}</td>` +
          `<td class="num">${escapeHtml(fmtMoney(Number(r?.receita ?? 0)))}</td>` +
          `<td class="num">${escapeHtml(fmtMoney(Number(r?.mc ?? 0)))}</td>` +
          `<td class="num">${escapeHtml(String(pct.toLocaleString('pt-BR', { maximumFractionDigits: 0 })))}%</td>` +
        '</tr>'
      );
    }).join('');

    const nfeDocs = (Array.isArray(nfeDocuments) ? nfeDocuments : []);
    const nfeRowsHtml = nfeDocs.length === 0
      ? '<tr><td colspan="4" class="muted">Sem NF-e carregadas para o período.</td></tr>'
      : nfeDocs.map((doc: any) => {
        const bid = extractNfeBuildingId(doc);
        const buildingName = bid ? resolveBuildingNameByIdOrCode(bid) : '';
        const date = getNfeDate(doc);
        const number = getNfeNumber(doc);
        const amount = getNfeAmount(doc);
        return (
          '<tr>' +
            `<td>${escapeHtml(buildingName || '-')}</td>` +
            `<td>${escapeHtml(date)}</td>` +
            `<td>${escapeHtml(number)}</td>` +
            `<td class="num">${escapeHtml(fmtMoney(amount))}</td>` +
          '</tr>'
        );
      }).join('');

    const html = `<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Relatório - MC por Obra</title>
  <style>
    body { font-family: Arial, sans-serif; color: #111; margin: 24px; }
    h1 { font-size: 18px; margin: 0 0 12px; }
    h2 { font-size: 14px; margin: 18px 0 8px; }
    .meta { font-size: 12px; color: #333; line-height: 1.5; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border: 1px solid #ddd; padding: 6px 8px; }
    th { background: #f5f5f5; text-align: left; }
    td.num, th.num { text-align: right; }
    .muted { color: #666; }
  </style>
</head>
<body>
  <h1>Relatório — MC por Obra</h1>
  <div class="meta">
    <div><b>Gerado em:</b> ${escapeHtml(new Date().toLocaleString('pt-BR'))}</div>
    <div><b>Período (UI):</b> ${escapeHtml(periodLabel)}</div>
    <div><b>Empresa:</b> ${escapeHtml(selectedCompanyName)} &nbsp; <b>Obra:</b> ${escapeHtml(selectedBuildingName)}</div>
    <div><b>Usuário:</b> ${escapeHtml(selectedUser)} &nbsp; <b>Solicitante:</b> ${escapeHtml(selectedRequester)}</div>
  </div>

  <h2>MC por Obra (Resumo)</h2>
  <table>
    <thead>
      <tr>
        <th>Obra</th>
        <th class="num">Receita Operacional</th>
        <th class="num">Margem Contribuição</th>
        <th class="num">% MC</th>
      </tr>
    </thead>
    <tbody>
      ${mcRowsHtml || '<tr><td colspan="4" class="muted">Sem dados por obra.</td></tr>'}
    </tbody>
    <tfoot>
      <tr>
        <th>Total</th>
        <th class="num">${escapeHtml(fmtMoney(Number(resumoPorObra.total?.receita ?? 0)))}</th>
        <th class="num">${escapeHtml(fmtMoney(Number(resumoPorObra.total?.mc ?? 0)))}</th>
        <th class="num">${escapeHtml(String(Number(resumoPorObra.total?.pct ?? 0).toLocaleString('pt-BR', { maximumFractionDigits: 0 })))}%</th>
      </tr>
    </tfoot>
  </table>

  <h2>NF-e (Detalhado)</h2>
  <div class="meta muted">Lista das NF-e carregadas na UI (limite atual do backend/UI).</div>
  <table>
    <thead>
      <tr>
        <th>Obra</th>
        <th>Data</th>
        <th>Número</th>
        <th class="num">Valor</th>
      </tr>
    </thead>
    <tbody>
      ${nfeRowsHtml}
    </tbody>
  </table>
</body>
</html>`;

    const w = window.open('', '_blank', 'noopener,noreferrer');
    if (!w) return;
    w.document.open();
    w.document.write(html);
    w.document.close();
    w.focus();
    w.print();
  };

  return (
    <motion.div key="db-geral" initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -20 }} className="space-y-8">
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 sm:gap-6">
        {[
          {
            label: selectedCompany !== 'all'
              ? `RECEITA — ${companies.find((c: any) => String(c.id) === selectedCompany)?.name || 'Empresa'}`
              : 'RECEITA OPERACIONAL',
            value: fmtBRL(kpiReceitaOperacional),
            icon: TrendingUp,
            color: 'orange',
            tooltip: 'Receita Operacional calculada pelos lançamentos filtrados (empresa, obra e período).',
          },
          {
            label: selectedCompany !== 'all'
              ? `MARGEM — ${companies.find((c: any) => String(c.id) === selectedCompany)?.name || 'Empresa'}`
              : 'Margem de Contribuição',
            value: fmtBRL(kpiMargemContribuicao),
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
              {i === 2 && (
                <button
                  type="button"
                  onClick={() => setActiveBuildingsModalOpen(true)}
                  className="mt-2 h-8 px-3 rounded-lg border border-white/10 bg-white/5 text-white text-[11px] font-black uppercase tracking-wide hover:bg-white/10"
                >
                  Ver obras
                </button>
              )}
            </CardHeader>
            <div className="h-1 w-full bg-orange-600/20"><div className="h-full bg-orange-600 w-1/3" /></div>
          </Card>
        ))}
      </div>

      {activeBuildingsModalOpen && (
        <div
          className="fixed inset-0 z-[999] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4 print:hidden"
          onClick={() => setActiveBuildingsModalOpen(false)}
        >
          <div
            className="w-full max-w-2xl bg-[#161618] border border-white/10 rounded-2xl shadow-2xl overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="p-4 sm:p-6 border-b border-white/10 flex items-center justify-between">
              <div>
                <h2 className="text-white text-lg sm:text-xl font-black uppercase tracking-widest leading-tight">Obras ativas</h2>
                <p className="text-gray-400 text-xs mt-1">{(Array.isArray(activeBuildings) ? activeBuildings : []).length} obras</p>
              </div>
              <button
                type="button"
                onClick={() => setActiveBuildingsModalOpen(false)}
                className="w-9 h-9 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 text-white flex items-center justify-center"
                aria-label="Fechar"
              >
                ×
              </button>
            </div>
            <div className="max-h-[60vh] overflow-auto">
              <Table className="text-xs sm:text-sm">
                <TableHeader className="border-white/10">
                  <TableRow className="border-white/10 hover:bg-transparent">
                    <TableHead className="text-gray-300">Obra</TableHead>
                    <TableHead className="text-gray-300">Código</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(Array.isArray(activeBuildings) ? activeBuildings : []).map((b: any) => (
                    <TableRow key={String(b?.id)} className="border-white/10 hover:bg-white/5">
                      <TableCell className="text-white font-semibold">{String(b?.name || '')}</TableCell>
                      <TableCell className="text-gray-300">{String(b?.code || b?.id || '')}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
            <div className="p-4 sm:p-6 border-t border-white/10 flex justify-end">
              <button
                type="button"
                onClick={() => setActiveBuildingsModalOpen(false)}
                className="h-9 px-4 rounded-lg border border-white/10 bg-white/5 text-white text-xs font-black uppercase tracking-wide hover:bg-white/10"
              >
                Fechar
              </button>
            </div>
          </div>
        </div>
      )}

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
              <span className="font-black text-orange-500">{fmtBRL(receitaMargemSeries.bestReceita.valor)}</span>
            </div>
            <div className="text-2xl sm:text-4xl font-black tracking-tighter text-white">{fmtBRL(kpiReceitaOperacional)}</div>
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
                  labelFormatter={(label) => `Mês: ${String(label ?? '')}`}
                  formatter={(value) => fmtBRL(Number(value ?? 0))}
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
                {fmtBRL(receitaMargemSeries.bestMargem.valor)}
              </span>
            </div>
            <div className={cn(
              'text-2xl sm:text-4xl font-black tracking-tighter',
              kpiMargemContribuicao >= 0 ? 'text-emerald-400' : 'text-red-500'
            )}>{fmtBRL(kpiMargemContribuicao)}</div>
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
                  labelFormatter={(label) => `Mês: ${String(label ?? '')}`}
                  formatter={(value) => fmtBRL(Number(value ?? 0))}
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
                  labelFormatter={(label) => `Mês: ${String(label ?? '')}`}
                  formatter={(value) => `${Number(value ?? 0).toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%`}
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
            <div className="flex items-center justify-between gap-3">
              <CardTitle className="text-lg font-black uppercase tracking-tight text-white">MC por Obra</CardTitle>
              <button
                type="button"
                onClick={printMcReport}
                className="h-8 px-3 rounded-lg border border-white/10 bg-white/5 text-white text-[11px] font-black uppercase tracking-wide hover:bg-white/10"
              >
                Imprimir Relatório
              </button>
            </div>
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
                          {fmtBRL(r.receita)}
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
                          {fmtBRL(r.mc)}
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
                    {fmtBRL(resumoPorObra.total.receita)}
                  </TableCell>
                  <TableCell className={cn('font-black', resumoPorObra.total.mc >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                    {fmtBRL(resumoPorObra.total.mc)}
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
