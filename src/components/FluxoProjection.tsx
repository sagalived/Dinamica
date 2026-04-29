import React, { useMemo, useRef, useState } from 'react';
import { Upload, RefreshCw, FileText } from 'lucide-react';
import * as XLSX from 'xlsx';

interface ProjectionEntry {
  company: string;
  obra: string;
  date: string;
  entrada: number;
  saida: number;
}

interface FluxoProjectionProps {
  allFinancialTitles: any[];
  allReceivableTitles: any[];
  buildings: any[];
  companies: any[];
  syncing: boolean;
  syncSienge: () => Promise<void>;
}

function toMoney(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? Math.abs(n) : 0;
}

function monthKey(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function monthLabel(date: Date): string {
  return date.toLocaleDateString('pt-BR', { month: 'short' }).replace('.', '');
}

function parseCsv(text: string): ProjectionEntry[] {
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) return [];
  const split = (line: string) => line.split(/[;,]/).map((c) => c.trim().replace(/^"|"$/g, ''));
  const header = split(lines[0]).map((h) => h.toLowerCase());
  const idx = (names: string[]) => header.findIndex((h) => names.some((n) => h.includes(n)));
  const dateIdx = idx(['data', 'vencto', 'venc']);
  const companyIdx = idx(['empresa', 'company']);
  const obraIdx = idx(['obra', 'building']);
  const entradaIdx = idx(['credito', 'entrada']);
  const saidaIdx = idx(['debito', 'saida']);

  return lines.slice(1).map((line) => {
    const cols = split(line);
    const parseNum = (v: string) => Number(String(v || '0').replace(/\./g, '').replace(',', '.')) || 0;
    return {
      company: companyIdx >= 0 ? String(cols[companyIdx] || '') : '',
      obra: obraIdx >= 0 ? String(cols[obraIdx] || '') : '',
      date: dateIdx >= 0 ? String(cols[dateIdx] || '') : '',
      entrada: entradaIdx >= 0 ? parseNum(cols[entradaIdx]) : 0,
      saida: saidaIdx >= 0 ? parseNum(cols[saidaIdx]) : 0,
    };
  });
}

function parseXlsx(file: File): Promise<ProjectionEntry[]> {
  return file.arrayBuffer().then((ab) => {
    const wb = XLSX.read(ab, { type: 'array' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const json = XLSX.utils.sheet_to_json<any>(ws, { defval: '' });
    const get = (obj: any, candidates: string[]) => {
      const keys = Object.keys(obj);
      const match = keys.find((k) => candidates.some((c) => k.toLowerCase().includes(c)));
      return match ? obj[match] : '';
    };
    const parseNum = (v: any) => Number(String(v || '0').replace(/\./g, '').replace(',', '.')) || 0;
    return json.map((obj) => ({
      company: String(get(obj, ['empresa', 'company']) || ''),
      obra: String(get(obj, ['obra', 'building']) || ''),
      date: String(get(obj, ['data', 'vencto', 'venc']) || ''),
      entrada: parseNum(get(obj, ['credito', 'entrada'])),
      saida: parseNum(get(obj, ['debito', 'saida'])),
    }));
  });
}

export function FluxoProjection({
  allFinancialTitles,
  allReceivableTitles,
  buildings,
  companies,
  syncing,
  syncSienge,
}: FluxoProjectionProps) {
  const [selectedCompany, setSelectedCompany] = useState<string>('all');
  const [selectedBuilding, setSelectedBuilding] = useState<string>('all');
  const [uploaded, setUploaded] = useState<ProjectionEntry[] | null>(null);
  const [uploadName, setUploadName] = useState('');
  const [uploadError, setUploadError] = useState('');
  const [uploading, setUploading] = useState(false);
  const [startMonth, setStartMonth] = useState<string>(() => {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  });
  const [monthSpan, setMonthSpan] = useState<number>(6);
  const inputRef = useRef<HTMLInputElement>(null);

  const buildingMap = useMemo(() => {
    const map: Record<string, any> = {};
    buildings.forEach((b: any) => {
      map[String(b.id)] = b;
      if (b.code) map[String(b.code)] = b;
    });
    return map;
  }, [buildings]);

  const companyMap = useMemo(() => {
    const map: Record<string, string> = {};
    companies.forEach((c: any) => {
      map[String(c.id)] = c.name;
    });
    return map;
  }, [companies]);

  const normalizedSienge = useMemo((): ProjectionEntry[] => {
    const rows: ProjectionEntry[] = [];

    allReceivableTitles.forEach((t: any) => {
      const amount = toMoney(t.rawValue ?? t.amount);
      if (!amount) return;
      const b = buildingMap[String(t.buildingCode || t.buildingId || '')] || {};
      const companyId = String(t.companyId ?? b.companyId ?? b.company_id ?? '');
      rows.push({
        company: companyMap[companyId] || `Empresa ${companyId || '-'}`,
        obra: String(t.buildingName || b.name || `Obra ${t.buildingCode || t.buildingId || '-'}`),
        date: String(t.dueDate || ''),
        entrada: amount,
        saida: 0,
      });
    });

    allFinancialTitles.forEach((t: any) => {
      const amount = toMoney(t.amount);
      if (!amount) return;
      const b = buildingMap[String(t.buildingCode || t.buildingId || '')] || {};
      const companyId = String(t.companyId ?? b.companyId ?? b.company_id ?? '');
      rows.push({
        company: companyMap[companyId] || `Empresa ${companyId || '-'}`,
        obra: String(t.buildingName || b.name || `Obra ${t.buildingCode || t.buildingId || '-'}`),
        date: String(t.dueDate || ''),
        entrada: 0,
        saida: amount,
      });
    });

    return rows;
  }, [allFinancialTitles, allReceivableTitles, buildingMap, companyMap]);

  const sourceRows = uploaded && uploaded.length > 0 ? uploaded : normalizedSienge;

  const companyOptions = useMemo(() => {
    const names = new Set<string>();
    sourceRows.forEach((r) => {
      if (r.company) names.add(r.company);
    });
    const list = Array.from(names).sort((a, b) => a.localeCompare(b));
    return list;
  }, [sourceRows]);

  const rowsByCompany = useMemo(() => {
    if (selectedCompany === 'all') return sourceRows;
    return sourceRows.filter((r) => r.company === selectedCompany);
  }, [selectedCompany, sourceRows]);

  const buildingOptions = useMemo(() => {
    const names = new Set<string>();
    rowsByCompany.forEach((r) => {
      if (r.obra) names.add(r.obra);
    });
    return Array.from(names).sort((a, b) => a.localeCompare(b));
  }, [rowsByCompany]);

  const filteredRows = useMemo(() => {
    if (selectedBuilding === 'all') return rowsByCompany;
    return rowsByCompany.filter((r) => r.obra === selectedBuilding);
  }, [rowsByCompany, selectedBuilding]);

  const monthColumns = useMemo(() => {
    const [y, m] = startMonth.split('-').map(Number);
    const base = new Date(y || new Date().getFullYear(), (m || (new Date().getMonth() + 1)) - 1, 1);
    const cols: Date[] = [];
    for (let i = 0; i < monthSpan; i += 1) {
      cols.push(new Date(base.getFullYear(), base.getMonth() + i, 1));
    }
    return cols;
  }, [monthSpan, startMonth]);

  const monthly = useMemo(() => {
    const bucket: Record<string, { entrada: number; saida: number }> = {};
    monthColumns.forEach((d) => {
      bucket[monthKey(d)] = { entrada: 0, saida: 0 };
    });

    filteredRows.forEach((r) => {
      const d = new Date(r.date);
      if (Number.isNaN(d.getTime())) return;
      const key = monthKey(new Date(d.getFullYear(), d.getMonth(), 1));
      if (!bucket[key]) return;
      bucket[key].entrada += r.entrada;
      bucket[key].saida += r.saida;
    });

    return monthColumns.map((d) => {
      const key = monthKey(d);
      const item = bucket[key] || { entrada: 0, saida: 0 };
      return {
        key,
        label: monthLabel(d),
        entrada: item.entrada,
        saida: item.saida,
        saldo: item.entrada - item.saida,
      };
    });
  }, [filteredRows, monthColumns]);

  const totalEntrada = monthly.reduce((acc, m) => acc + m.entrada, 0);
  const totalSaida = monthly.reduce((acc, m) => acc + m.saida, 0);
  const totalSaldo = totalEntrada - totalSaida;

  const byBuilding = useMemo(() => {
    const map: Record<string, { entrada: number; saida: number }> = {};
    filteredRows.forEach((r) => {
      const key = r.obra || 'Obra sem nome';
      if (!map[key]) map[key] = { entrada: 0, saida: 0 };
      map[key].entrada += r.entrada;
      map[key].saida += r.saida;
    });
    return Object.entries(map)
      .map(([obra, values]) => ({ obra, ...values, saldo: values.entrada - values.saida }))
      .sort((a, b) => b.saida - a.saida)
      .slice(0, 10);
  }, [filteredRows]);

  const onSelectFile = async (file: File) => {
    setUploadError('');
    setUploading(true);
    setUploadName(file.name);
    try {
      const ext = file.name.split('.').pop()?.toLowerCase();
      if (ext === 'csv') {
        const text = await file.text();
        const rows = parseCsv(text);
        if (rows.length === 0) throw new Error('Arquivo CSV sem dados validos.');
        setUploaded(rows);
      } else if (ext === 'xlsx' || ext === 'xls') {
        const rows = await parseXlsx(file);
        if (rows.length === 0) throw new Error('Arquivo XLSX sem dados validos.');
        setUploaded(rows);
      } else if (ext === 'pdf') {
        throw new Error('PDF selecionado. Converta para CSV ou XLSX para processar os dados.');
      } else {
        throw new Error(`Formato .${ext} nao suportado.`);
      }
      setSelectedCompany('all');
      setSelectedBuilding('all');
    } catch (error: any) {
      setUploadError(error?.message || 'Erro ao processar arquivo.');
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="bg-[#161618] border border-white/5 rounded-2xl shadow-2xl p-5 space-y-4">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <div>
            <h3 className="text-xl font-black uppercase text-white">Projecao de Fluxo de Caixa</h3>
            <p className="text-xs text-gray-400">Empresa e obras vinculadas, com calculos automaticos por periodo.</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={syncSienge}
              disabled={syncing}
              className="h-10 px-4 rounded-xl bg-[#1B3C58] hover:bg-[#234b6e] text-white font-bold text-sm disabled:opacity-70"
            >
              <span className="inline-flex items-center"><RefreshCw size={14} className="mr-2" />Atualizar dados</span>
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
          <div className="md:col-span-2">
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Empresa</label>
            <select
              value={selectedCompany}
              onChange={(e) => { setSelectedCompany(e.target.value); setSelectedBuilding('all'); }}
              className="w-full h-11 mt-1 rounded-xl bg-black/40 border border-white/10 text-white font-bold px-3"
            >
              <option value="all">Todas as Empresas</option>
              {companyOptions.map((name) => (
                <option key={`company-proj-${name}`} value={name}>{name}</option>
              ))}
            </select>
          </div>
          <div className="md:col-span-2">
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obra (por empresa)</label>
            <select
              value={selectedBuilding}
              onChange={(e) => setSelectedBuilding(e.target.value)}
              className="w-full h-11 mt-1 rounded-xl bg-black/40 border border-white/10 text-white font-bold px-3"
            >
              <option value="all">Todas as Obras</option>
              {buildingOptions.map((name) => (
                <option key={`obra-proj-${name}`} value={name}>{name}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Upload</label>
            <input
              ref={inputRef}
              type="file"
              accept=".csv,.xlsx,.xls,.pdf"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) onSelectFile(file);
                e.currentTarget.value = '';
              }}
            />
            <button
              onClick={() => inputRef.current?.click()}
              className="w-full h-11 mt-1 rounded-xl border border-white/10 bg-black/30 text-white font-bold text-sm"
              disabled={uploading}
            >
              <span className="inline-flex items-center"><Upload size={14} className="mr-2" />{uploading ? 'Processando...' : 'CSV, XLSX, PDF'}</span>
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Mes inicial da projecao</label>
            <input
              type="month"
              value={startMonth}
              onChange={(e) => setStartMonth(e.target.value)}
              className="w-full h-11 mt-1 rounded-xl bg-black/40 border border-white/10 text-white font-bold px-3"
            />
          </div>
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Quantidade de meses</label>
            <select
              value={String(monthSpan)}
              onChange={(e) => setMonthSpan(Number(e.target.value) || 6)}
              className="w-full h-11 mt-1 rounded-xl bg-black/40 border border-white/10 text-white font-bold px-3"
            >
              <option value="3">3 meses</option>
              <option value="6">6 meses</option>
              <option value="9">9 meses</option>
              <option value="12">12 meses</option>
            </select>
          </div>
          <div className="text-xs text-gray-400 flex items-end pb-2">
            Valores futuros e realizados sao recalculados do Sienge conforme empresa/obra e janela de meses selecionada.
          </div>
        </div>

        {(uploadName || uploadError) && (
          <div className="text-xs">
            {uploadName && <p className="text-emerald-400">Arquivo: {uploadName}</p>}
            {uploadError && <p className="text-red-400">{uploadError}</p>}
          </div>
        )}
      </div>

      <div className="bg-[#161618] border border-white/5 rounded-2xl shadow-2xl overflow-auto">
        <table className="w-full text-xs">
          <thead className="bg-black/60">
            <tr>
              <th className="px-3 py-2 text-left text-gray-500 uppercase">Tipo</th>
              {monthly.map((m, idx) => <th key={m.key} className="px-3 py-2 text-right text-gray-500 uppercase">{idx === 0 ? 'Realizado' : 'Previsao'} {m.label}</th>)}
              <th className="px-3 py-2 text-right text-gray-500 uppercase">Total</th>
            </tr>
          </thead>
          <tbody>
            <tr className="border-t border-white/5">
              <td className="px-3 py-2 font-black text-white">Entradas</td>
              {monthly.map((m) => <td key={`in-${m.key}`} className="px-3 py-2 text-right text-emerald-400">{m.entrada.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>)}
              <td className="px-3 py-2 text-right font-black text-emerald-400">{totalEntrada.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            </tr>
            <tr className="border-t border-white/5">
              <td className="px-3 py-2 font-black text-white">Saidas</td>
              {monthly.map((m) => <td key={`out-${m.key}`} className="px-3 py-2 text-right text-red-400">{m.saida.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>)}
              <td className="px-3 py-2 text-right font-black text-red-400">{totalSaida.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            </tr>
            <tr className="border-t border-white/5">
              <td className="px-3 py-2 font-black text-white">Saldo</td>
              {monthly.map((m) => (
                <td key={`saldo-${m.key}`} className={`px-3 py-2 text-right font-bold ${m.saldo >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {m.saldo.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </td>
              ))}
              <td className={`px-3 py-2 text-right font-black ${totalSaldo >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>{totalSaldo.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="bg-[#161618] border border-white/5 rounded-2xl shadow-2xl overflow-auto max-h-[420px]">
        <table className="w-full text-xs">
          <thead className="bg-black/60 sticky top-0">
            <tr>
              <th className="px-3 py-2 text-left text-gray-500 uppercase">Empresa</th>
              <th className="px-3 py-2 text-left text-gray-500 uppercase">Obra</th>
              <th className="px-3 py-2 text-right text-gray-500 uppercase">Entradas</th>
              <th className="px-3 py-2 text-right text-gray-500 uppercase">Saidas</th>
              <th className="px-3 py-2 text-right text-gray-500 uppercase">Saldo</th>
            </tr>
          </thead>
          <tbody>
            {byBuilding.length === 0 ? (
              <tr><td colSpan={5} className="px-3 py-10 text-center text-gray-500 font-bold">Sem dados para os filtros selecionados.</td></tr>
            ) : byBuilding.map((r) => (
              <tr key={`proj-${r.obra}`} className="border-t border-white/5">
                <td className="px-3 py-2 text-gray-300">{selectedCompany === 'all' ? 'Multiplas' : selectedCompany}</td>
                <td className="px-3 py-2 font-bold text-gray-200">{r.obra}</td>
                <td className="px-3 py-2 text-right text-emerald-400">{r.entrada.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td className="px-3 py-2 text-right text-red-400">{r.saida.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                <td className={`px-3 py-2 text-right font-black ${r.saldo >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>{r.saldo.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
