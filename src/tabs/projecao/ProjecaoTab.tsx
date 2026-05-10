import React, { useMemo, useRef, useState } from 'react';
import { Upload, RefreshCw } from 'lucide-react';
import * as XLSX from 'xlsx';
import {
  type ProjectionEntry,
  aggregateByBuilding,
  aggregateMonthly,
  buildMonthColumns,
  parseCsv,
  parseXlsx,
  toMoney,
} from './logic';
import { formatarNumeroBR } from '../utilitarios/formatacaoptbr';

import type { FluxoProjectionProps } from './types';


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

  const monthColumns = useMemo(
    () => buildMonthColumns(startMonth, monthSpan),
    [monthSpan, startMonth],
  );

  const monthly = useMemo(
    () => aggregateMonthly(filteredRows, monthColumns),
    [filteredRows, monthColumns],
  );

  const totalEntrada = monthly.reduce((acc, m) => acc + m.entrada, 0);
  const totalSaida = monthly.reduce((acc, m) => acc + m.saida, 0);
  const totalSaldo = totalEntrada - totalSaida;

  const byBuilding = useMemo(
    () => aggregateByBuilding(filteredRows, 10),
    [filteredRows],
  );

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
        const rows = await parseXlsx(file, XLSX);
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
              {monthly.map((m) => <td key={`in-${m.key}`} className="px-3 py-2 text-right text-emerald-400">{formatarNumeroBR(m.entrada)}</td>)}
              <td className="px-3 py-2 text-right font-black text-emerald-400">{formatarNumeroBR(totalEntrada)}</td>
            </tr>
            <tr className="border-t border-white/5">
              <td className="px-3 py-2 font-black text-white">Saidas</td>
              {monthly.map((m) => <td key={`out-${m.key}`} className="px-3 py-2 text-right text-red-400">{formatarNumeroBR(m.saida)}</td>)}
              <td className="px-3 py-2 text-right font-black text-red-400">{formatarNumeroBR(totalSaida)}</td>
            </tr>
            <tr className="border-t border-white/5">
              <td className="px-3 py-2 font-black text-white">Saldo</td>
              {monthly.map((m) => (
                <td key={`saldo-${m.key}`} className={`px-3 py-2 text-right font-bold ${m.saldo >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {formatarNumeroBR(m.saldo)}
                </td>
              ))}
              <td className={`px-3 py-2 text-right font-black ${totalSaldo >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>{formatarNumeroBR(totalSaldo)}</td>
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
                <td className="px-3 py-2 text-right text-emerald-400">{formatarNumeroBR(r.entrada)}</td>
                <td className="px-3 py-2 text-right text-red-400">{formatarNumeroBR(r.saida)}</td>
                <td className={`px-3 py-2 text-right font-black ${r.saldo >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>{formatarNumeroBR(r.saldo)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
