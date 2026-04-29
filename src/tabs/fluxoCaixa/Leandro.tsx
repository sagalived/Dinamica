/**
 * LEANDRO — Subaba de Fluxo de Caixa
 * - Exibe dados financeiros do Sienge (entradas, saídas, por obra)
 * - Permite upload de CSV / XLSX / PDF para popular os dados manualmente
 */

import React, { useState, useCallback, useMemo, useRef } from 'react';
import { motion } from 'motion/react';
import {
  Upload, RefreshCw, TrendingUp, TrendingDown, DollarSign,
  FileText, Building2, X, CheckCircle2, AlertCircle, Download, Calendar as CalendarIcon, Printer
} from 'lucide-react';
import { addMonths, endOfDay, format, isValid, parse, startOfDay } from 'date-fns';
import { cn } from '../../lib/utils';
import * as XLSX from 'xlsx';
import { INTERNAL_BANK_ACCOUNTS, extractBankAccountCode } from './logic';

// ─── Tipos ────────────────────────────────────────────────────────────────────

interface LeandroRow {
  id?: string | number;
  data: string;
  baixa?: string;
  vencto?: string;
  companyId?: string;
  historico: string;
  clienteFornecedor?: string;
  tituloParcela?: string;
  documento?: string;
  obra: string;
  obraId?: string;
  entrada: number;
  saida: number;
  saldo: number;
}

interface LeandroPeriod {
  mes: string;
  entradas: number;
  saidas: number;
  saldo: number;
}

interface LeandroProps {
  isDark: boolean;
  allFinancialTitles: any[];
  allReceivableTitles: any[];
  orders: any[];
  buildings: any[];
  companies: any[];
  syncing: boolean;
  syncSienge: () => Promise<void>;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

const fmt = (v: number) =>
  new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(v);

function parseDateSafe(raw: string | undefined): Date | null {
  if (!raw) return null;
  const text = String(raw).trim();
  if (!text) return null;

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(text)) {
    const br = parse(text, 'dd/MM/yyyy', new Date());
    return isValid(br) ? br : null;
  }

  if (/^\d{4}-\d{2}-\d{2}(T.*)?$/.test(text)) {
    const isoDate = parse(text.slice(0, 10), 'yyyy-MM-dd', new Date());
    return isValid(isoDate) ? isoDate : null;
  }

  const iso = parse(text, 'yyyy-MM-dd', new Date());
  if (isValid(iso)) return iso;

  const native = new Date(text);
  return Number.isNaN(native.getTime()) ? null : native;
}

function parseDateFlexible(raw: string | undefined): number {
  const date = parseDateSafe(raw);
  return date ? date.getTime() : 0;
}

function formatDateSafe(raw: string | undefined, fallback = '—'): string {
  const date = parseDateSafe(raw);
  return date ? format(date, 'dd/MM/yyyy') : fallback;
}

function normalizeDocument(raw: string | undefined): string {
  const text = String(raw || '').trim();
  if (!text || text === '—') return '';

  const upper = text.toUpperCase();
  const ppc = upper.match(/PPC\D*(\d+)/);
  if (ppc) {
    return `PPC.${ppc[1]}`;
  }

  if (upper.includes('DARM')) {
    const groups = upper.match(/\d+/g) || [];
    if (groups.length >= 3) return `DARM.${groups[0]}.${groups[1]}.${groups[2]}`;
    if (groups.length === 2) return `DARM.${groups[0]}.${groups[1]}`;
    if (groups.length === 1) return `DARM.${groups[0]}`;
    return 'DARM';
  }

  const digits = upper.replace(/\D/g, '');
  if (digits.length >= 8) {
    const last8 = digits.slice(-8);
    if (!/^0+$/.test(last8)) return `${last8.slice(0, 6)}-${last8.slice(6)}`;
  }

  return upper;
}

function getDocumentParts(raw: string | undefined): { original: string; treated: string } {
  const original = String(raw || '').trim() || '—';
  const treated = normalizeDocument(raw);
  return {
    original,
    treated: treated && treated !== original.toUpperCase() ? treated : '',
  };
}

function buildEntityLabel(name: string | undefined, doc: string | number | undefined, fallback: string) {
  const safeName = String(name || '').trim();
  const safeDoc = String(doc || '').trim();
  if (safeName && safeDoc) return `${safeName} • ${safeDoc}`;
  if (safeName) return safeName;
  if (safeDoc) return `${fallback} • ${safeDoc}`;
  return fallback;
}

function pickEntityName(...values: Array<string | undefined>) {
  const placeholders = new Set(['Extrato/Cliente', 'Pagamento/Credor', 'Credor sem nome', 'Cliente sem nome']);
  for (const value of values) {
    const safe = String(value || '').trim();
    if (!safe) continue;
    if (placeholders.has(safe)) continue;
    return safe;
  }
  return '';
}

function parseCsvText(text: string): LeandroRow[] {
  const lines = text.split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  const header = lines[0].split(/[;,]/).map(h => h.trim().toLowerCase().replace(/\s+/g, '_'));
  return lines.slice(1).map(line => {
    const cols = line.split(/[;,]/);
    const obj: any = {};
    header.forEach((h, i) => { obj[h] = (cols[i] || '').trim().replace(/"/g, ''); });
    const parseNum = (s: string) => parseFloat(String(s).replace(/\./g, '').replace(',', '.')) || 0;
    return {
      data: obj.data || obj.date || obj.vencimento || '',
      historico: obj.historico || obj.descricao || obj.description || obj.nome || '',
      obra: obj.obra || obj.building || obj.empreendimento || '',
      entrada: parseNum(obj.entrada || obj.receita || obj.credito || '0'),
      saida: parseNum(obj.saida || obj.despesa || obj.debito || '0'),
      saldo: parseNum(obj.saldo || obj.balance || '0'),
    };
  }).filter(r => r.data || r.historico);
}

// ─── Componente Principal ─────────────────────────────────────────────────────

export function LeandroTab({
  isDark,
  allFinancialTitles,
  allReceivableTitles,
  orders,
  buildings,
  companies,
  syncing,
  syncSienge,
}: LeandroProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploadedRows, setUploadedRows] = useState<LeandroRow[] | null>(null);
  const [uploadFileName, setUploadFileName] = useState('');
  const [uploadError, setUploadError] = useState('');
  const [uploadLoading, setUploadLoading] = useState(false);
  const [activeSource, setActiveSource] = useState<'sienge' | 'arquivo'>('sienge');
  const [periodMode, setPeriodMode] = useState<'last6m' | 'all'>('last6m');
  const [startDate, setStartDate] = useState<Date | undefined>();
  const [endDate, setEndDate] = useState<Date | undefined>();
  const [selectedCompany, setSelectedCompany] = useState<string>('all');
  const [selectedBuilding, setSelectedBuilding] = useState<string>('all');
  const [hideInternal, setHideInternal] = useState(true);
  const [applyTick, setApplyTick] = useState(0);
  const [detailLimit, setDetailLimit] = useState(500);

  // ── Dados do Sienge ────────────────────────────────────────────────────────

  const buildingMap = useMemo(() => {
    const m: Record<string, string> = {};
    buildings.forEach((b: any) => {
      const label = `${b.name} (${b.code || b.id})`;
      m[String(b.id)] = label;
      if (b.code) {
        m[String(b.code)] = label;
      }
    });
    return m;
  }, [buildings]);

  const defaultStartDate = useMemo(() => startOfDay(new Date(new Date().getFullYear(), new Date().getMonth(), 1)), []);

  const getTitleDateRaw = useCallback((value: any): string => {
    return String(
      value?.dueDate
      || value?.dataVencimento
      || value?.date
      || value?.data
      || value?.issueDate
      || value?.dataEmissao
      || value?.dataVencimentoProjetado
      || ''
    ).trim();
  }, []);

  const buildingById = useMemo(() => {
    const map: Record<string, any> = {};
    buildings.forEach((b: any) => {
      map[String(b.id)] = b;
      if (b.code) {
        map[String(b.code)] = b;
      }
    });
    return map;
  }, [buildings]);

  const getBuildingKey = useCallback((value: any): string => {
    const direct = String(value?.buildingCode || value?.buildingId || '').trim();
    if (!direct || direct === 'undefined' || direct === 'null') return '';
    return direct;
  }, []);

  const passesCompany = useCallback((value: any): boolean => {
    if (selectedCompany === 'all') return true;
    const directCompany = String(value?.companyId ?? '');
    if (directCompany && directCompany !== 'undefined' && directCompany !== 'null') {
      return directCompany === selectedCompany;
    }
    const building = buildingById[getBuildingKey(value)];
    return String(building?.companyId ?? building?.company_id ?? '') === selectedCompany;
  }, [buildingById, getBuildingKey, selectedCompany]);

  const passesBuilding = useCallback((value: any): boolean => {
    if (selectedBuilding === 'all') return true;
    const current = getBuildingKey(value);
    if (current === selectedBuilding) return true;
    const selected = buildingById[selectedBuilding];
    if (!selected) return false;
    const selectedId = String(selected.id ?? '');
    const selectedCode = String(selected.code ?? '');
    return current === selectedId || current === selectedCode;
  }, [buildingById, getBuildingKey, selectedBuilding]);

  const passesInternalFilter = useCallback((value: any): boolean => {
    if (!hideInternal) return true;
    const bankCode = String(value?.bankAccountCode || extractBankAccountCode(value?.links || []));
    if (INTERNAL_BANK_ACCOUNTS.has(bankCode)) return false;
    if (String(value?.statementOrigin || '').toUpperCase() === 'GE') return false;
    return true;
  }, [hideInternal]);

  const maxAvailableSiengeDate = useMemo(() => {
    let maxTimestamp = 0;

    const consume = (item: any) => {
      const ts = parseDateFlexible(getTitleDateRaw(item));
      if (ts > maxTimestamp) maxTimestamp = ts;
    };

    allReceivableTitles
      .filter((item: any) => passesCompany(item) && passesBuilding(item))
      .forEach(consume);

    allFinancialTitles
      .filter((item: any) => passesCompany(item) && passesBuilding(item))
      .forEach(consume);

    return maxTimestamp > 0 ? endOfDay(new Date(maxTimestamp)) : null;
  }, [allFinancialTitles, allReceivableTitles, getTitleDateRaw, passesBuilding, passesCompany]);

  const defaultEndDate = useMemo(() => {
    return maxAvailableSiengeDate || endOfDay(addMonths(new Date(), 5));
  }, [maxAvailableSiengeDate]);

  const effectiveStartDate = startDate || (periodMode === 'last6m' ? defaultStartDate : undefined);
  const effectiveEndDate = endDate || (periodMode === 'last6m' ? defaultEndDate : undefined);

  const isInDateRange = useCallback((value: any): boolean => {
    const currentTimestamp = parseDateFlexible(getTitleDateRaw(value));
    if (!currentTimestamp) return false;
    if (effectiveStartDate && currentTimestamp < startOfDay(effectiveStartDate).getTime()) return false;
    if (effectiveEndDate && currentTimestamp > endOfDay(effectiveEndDate).getTime()) return false;
    return true;
  }, [effectiveEndDate, effectiveStartDate, getTitleDateRaw]);

  const applySiengeFilters = useCallback((value: any): boolean => (
    isInDateRange(value) &&
    passesCompany(value) &&
    passesBuilding(value) &&
    passesInternalFilter(value)
  ), [isInDateRange, passesCompany, passesBuilding, passesInternalFilter]);

  const applySiengeFiltersWithoutBuilding = useCallback((value: any): boolean => (
    isInDateRange(value) &&
    passesCompany(value) &&
    passesInternalFilter(value)
  ), [isInDateRange, passesCompany, passesInternalFilter]);

  const availableBuildings = useMemo(() => {
    const movementBuildingKeys = new Set<string>();
    allReceivableTitles.filter(applySiengeFiltersWithoutBuilding).forEach((t: any) => {
      const key = getBuildingKey(t);
      if (key) movementBuildingKeys.add(key);
    });
    allFinancialTitles.filter(applySiengeFiltersWithoutBuilding).forEach((t: any) => {
      const key = getBuildingKey(t);
      if (key) movementBuildingKeys.add(key);
    });

    const scopedByCompany = buildings.filter((b: any) => {
      if (selectedCompany === 'all') return true;
      return String(b.companyId ?? b.company_id ?? '') === selectedCompany;
    });

    const byMovement = scopedByCompany.filter((b: any) => {
      const id = String(b.id ?? '');
      const code = String(b.code ?? '');
      return movementBuildingKeys.has(id) || (code && movementBuildingKeys.has(code));
    });
    if (byMovement.length === 0) {
      return scopedByCompany;
    }
    return byMovement;
  }, [allFinancialTitles, allReceivableTitles, applySiengeFiltersWithoutBuilding, buildings, getBuildingKey, selectedCompany]);

  const resolveObraLabel = useCallback((value: any): string => {
    const fromTitle = String(value?.buildingName || '').trim();
    if (fromTitle) return fromTitle;
    const key = getBuildingKey(value);
    if (key && buildingMap[key]) return buildingMap[key];
    if (selectedCompany !== 'all' && availableBuildings.length === 1) {
      const only = availableBuildings[0];
      return `${only.name} (${only.code || only.id})`;
    }
    return `Obra ${key || 'sem nome'}`;
  }, [availableBuildings, buildingMap, getBuildingKey, selectedCompany]);

  const selectedCompanyLabel = useMemo(() => {
    if (selectedCompany === 'all') return 'Todas as Empresas';
    const found = companies.find((c: any) => String(c.id) === selectedCompany);
    return found ? `1 - ${found.name}` : `Empresa ${selectedCompany}`;
  }, [companies, selectedCompany]);

  const selectedBuildingLabel = useMemo(() => {
    if (selectedBuilding === 'all') return '1 - OBRA';
    const found = availableBuildings.find((b: any) => String(b.id) === selectedBuilding || String(b.code || '') === selectedBuilding);
    return found ? `1 - ${found.name}` : `1 - Obra ${selectedBuilding}`;
  }, [availableBuildings, selectedBuilding]);

  const siengeRows = useMemo((): LeandroRow[] => {
    const rows: LeandroRow[] = [];

    allReceivableTitles.filter(applySiengeFilters).forEach((t: any) => {
      const dueRaw = t.dueDate || t.dataVencimento || t.date || t.data || '';
      const raw = Number(t.rawValue ?? t.amount ?? 0);
      const isExpense = String(t.type || '').toUpperCase() === 'EXPENSE' || raw < 0;
      const valor = Math.abs(raw);
      if (valor === 0) return;
      const clientName = pickEntityName(t.clientName, t.nomeCliente, t.description);
      const baixaDate = t.paymentDate || t.settlementDate || t.baixaDate || (String(t.status || '').toUpperCase() === 'BAIXADO' ? (t.dueDate || '') : '');
      rows.push({
        id: t.id,
        data: dueRaw,
        baixa: baixaDate,
        vencto: dueRaw,
        companyId: String(t.companyId ?? ''),
        historico: buildEntityLabel(clientName || t.clientName, t.documentNumber || t.id, 'Extrato/Cliente'),
        clienteFornecedor: clientName || 'Extrato/Cliente',
        tituloParcela: String(t.installmentNumber != null ? `${t.id}/${t.installmentNumber}` : (t.id || '—')),
        documento: String(t.documentNumber || t.documentId || t.id || '—'),
        obra: resolveObraLabel(t),
        obraId: String(t.buildingCode || t.buildingId || ''),
        entrada: isExpense ? 0 : valor,
        saida: isExpense ? valor : 0,
        saldo: 0,
      });
    });

    allFinancialTitles.filter(applySiengeFilters).forEach((t: any) => {
      const dueRaw = t.dueDate || t.dataVencimento || t.date || t.data || '';
      const saida = Math.abs(Number(t.amount ?? 0));
      if (saida === 0) return;
      const creditorName = pickEntityName(t.creditorName, t.nomeCredor, t.description);
      const baixaDate = t.paymentDate || t.settlementDate || t.baixaDate || (String(t.status || '').toUpperCase() === 'BAIXADO' ? (t.dueDate || '') : '');
      rows.push({
        id: t.id,
        data: dueRaw,
        baixa: baixaDate,
        vencto: dueRaw,
        companyId: String(t.companyId ?? ''),
        historico: buildEntityLabel(creditorName || t.creditorName, t.documentNumber || t.id, 'Pagamento/Credor'),
        clienteFornecedor: creditorName || 'Credor sem nome',
        tituloParcela: String(t.installmentNumber != null ? `${t.id}/${t.installmentNumber}` : (t.id || '—')),
        documento: String(t.documentNumber || t.documentId || t.id || '—'),
        obra: resolveObraLabel(t),
        obraId: String(t.buildingCode || t.buildingId || ''),
        entrada: 0,
        saida,
        saldo: 0,
      });
    });

    rows.sort((a, b) => {
      const da = parseDateFlexible(a.data || a.vencto || a.baixa);
      const db = parseDateFlexible(b.data || b.vencto || b.baixa);
      return da - db;
    });

    let acc = 0;
    rows.forEach(r => {
      acc += r.entrada - r.saida;
      r.saldo = acc;
    });

    return rows;
  }, [allFinancialTitles, allReceivableTitles, applySiengeFilters, applyTick, resolveObraLabel]);

  const activeRows = activeSource === 'arquivo' && uploadedRows ? uploadedRows : siengeRows;
  const detailedRows = useMemo(() => {
    return [...activeRows].sort((a, b) => {
      const da = parseDateFlexible(a.vencto || a.data || a.baixa);
      const db = parseDateFlexible(b.vencto || b.data || b.baixa);
      return db - da;
    });
  }, [activeRows]);

  const displayedDetailRows = useMemo(() => {
    return detailedRows.slice(0, detailLimit);
  }, [detailLimit, detailedRows]);

  // ── KPIs ──────────────────────────────────────────────────────────────────

  const totalEntradas = activeRows.reduce((s, r) => s + r.entrada, 0);
  const totalSaidas   = activeRows.reduce((s, r) => s + r.saida,   0);
  const saldoFinal    = activeRows.length > 0 ? activeRows[activeRows.length - 1].saldo : 0;

  // ── Resumo por mês ────────────────────────────────────────────────────────

  const periodos = useMemo((): LeandroPeriod[] => {
    const map: Record<string, { entradas: number; saidas: number }> = {};
    activeRows.forEach(r => {
      const timestamp = parseDateFlexible(r.data || r.vencto || r.baixa);
      if (!timestamp) return;
      const d = new Date(timestamp);
      const key = format(d, 'MM/yyyy');
      if (!map[key]) map[key] = { entradas: 0, saidas: 0 };
      map[key].entradas += r.entrada;
      map[key].saidas += r.saida;
    });
    let acc = 0;
    return Object.entries(map).map(([mes, v]) => {
      acc += v.entradas - v.saidas;
      return { mes, ...v, saldo: acc };
    });
  }, [activeRows]);

  // ── Resumo por obra ───────────────────────────────────────────────────────

  const porObra = useMemo(() => {
    const map: Record<string, { obra: string; entradas: number; saidas: number }> = {};

    const addToObra = (key: string, obra: string, entrada: number, saida: number) => {
      if (!map[key]) {
        map[key] = { obra, entradas: 0, saidas: 0 };
      }
      map[key].entradas += entrada;
      map[key].saidas += saida;
    };

    const selectedBuildingName = selectedBuilding !== 'all'
      ? String((availableBuildings.find((b: any) => String(b.id) === selectedBuilding || String(b.code || '') === selectedBuilding)?.name) || `Obra ${selectedBuilding}`)
      : '';

    allReceivableTitles.filter(applySiengeFilters).forEach((t: any) => {
      let key = getBuildingKey(t);
      if (!key && selectedBuilding !== 'all') {
        key = selectedBuilding;
      }
      const obra = resolveObraLabel(t);
      if (!key) key = obra;
      if (!key) return;
      const raw = Number(t.rawValue ?? t.amount ?? 0);
      const isExpense = String(t.type || '').toUpperCase() === 'EXPENSE' || raw < 0;
      const valor = Math.abs(raw);
      addToObra(key, obra, isExpense ? 0 : valor, isExpense ? valor : 0);
    });

    allFinancialTitles.filter(applySiengeFilters).forEach((t: any) => {
      let key = getBuildingKey(t);
      if (!key && selectedBuilding !== 'all') {
        key = selectedBuilding;
      }
      const obra = resolveObraLabel(t);
      if (!key) key = obra;
      if (!key) return;
      const valor = Math.abs(Number(t.amount ?? 0));
      addToObra(key, obra, 0, valor);
    });

    const rows = Object.values(map)
      .map((v) => ({ ...v, saldo: v.entradas - v.saidas }))
      .sort((a, b) => b.saidas - a.saidas)
      .slice(0, 10);

    if (rows.length > 0) return rows;

    // fallback para manter comportamento com dados importados por arquivo
    const fallback: Record<string, { entradas: number; saidas: number }> = {};
    activeRows.forEach((r) => {
      const k = r.obra || 'Sem obra vinculada';
      if (!fallback[k]) fallback[k] = { entradas: 0, saidas: 0 };
      fallback[k].entradas += r.entrada;
      fallback[k].saidas += r.saida;
    });
    return Object.entries(fallback)
      .map(([obra, v]) => ({ obra, ...v, saldo: v.entradas - v.saidas }))
      .sort((a, b) => b.saidas - a.saidas)
      .slice(0, 10);
  }, [activeRows, allFinancialTitles, allReceivableTitles, applySiengeFilters, availableBuildings, getBuildingKey, resolveObraLabel, selectedBuilding]);

  const ultimasMovimentacoes = useMemo(() => {
    return [...activeRows]
      .sort((a, b) => {
        const da = parseDateFlexible(a.data || a.vencto || a.baixa);
        const db = parseDateFlexible(b.data || b.vencto || b.baixa);
        return db - da;
      })
      .slice(0, 10);
  }, [activeRows]);

  const totalObrasFiltradas = useMemo(() => {
    if (selectedBuilding !== 'all') return 1;
    if (availableBuildings.length > 0) return availableBuildings.length;

    const unique = new Set<string>();
    activeRows.forEach((r) => {
      const key = String(r.obra || r.obraId || '').trim();
      if (key) unique.add(key);
    });
    return unique.size;
  }, [activeRows, availableBuildings.length, selectedBuilding]);

  const periodLabel = useMemo(() => {
    const start = effectiveStartDate || defaultStartDate;
    const end = effectiveEndDate || defaultEndDate;
    return `${format(start, 'dd/MM/yyyy')} até ${format(end, 'dd/MM/yyyy')}`;
  }, [defaultEndDate, defaultStartDate, effectiveEndDate, effectiveStartDate]);

  // ── Upload ────────────────────────────────────────────────────────────────

  const handleFile = useCallback(async (file: File) => {
    setUploadError('');
    setUploadLoading(true);
    setUploadFileName(file.name);
    try {
      const ext = file.name.split('.').pop()?.toLowerCase();

      if (ext === 'csv') {
        const text = await file.text();
        const rows = parseCsvText(text);
        if (rows.length === 0) throw new Error('Nenhum dado encontrado no CSV.');
        setUploadedRows(rows);
        setActiveSource('arquivo');
      } else if (ext === 'xlsx' || ext === 'xls') {
        const ab = await file.arrayBuffer();
        const wb = XLSX.read(ab, { type: 'array' });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const json: any[] = XLSX.utils.sheet_to_json(ws, { defval: '' });
        const rows: LeandroRow[] = json.map((obj: any) => {
          const keys = Object.keys(obj).map(k => k.toLowerCase().trim());
          const get = (candidates: string[]) => {
            for (const c of candidates) {
              const k = keys.find(k2 => k2.includes(c));
              if (k) return String(obj[Object.keys(obj)[keys.indexOf(k)]] || '');
            }
            return '';
          };
          const parseNum = (s: string) => parseFloat(String(s).replace(/\./g, '').replace(',', '.')) || 0;
          return {
            data: get(['data', 'date', 'vencimento']),
            historico: get(['historico', 'descricao', 'description', 'nome', 'hist']),
            obra: get(['obra', 'building', 'empreendimento', 'project']),
            entrada: parseNum(get(['entrada', 'receita', 'credito'])),
            saida: parseNum(get(['saida', 'despesa', 'debito'])),
            saldo: parseNum(get(['saldo', 'balance'])),
          };
        }).filter(r => r.data || r.historico);
        if (rows.length === 0) throw new Error('Nenhum dado mapeado. Verifique os cabeçalhos do arquivo.');
        setUploadedRows(rows);
        setActiveSource('arquivo');
      } else if (ext === 'pdf') {
        throw new Error('Leitura de PDF não suportada automaticamente. Por favor, exporte como CSV ou XLSX.');
      } else {
        throw new Error(`Formato ".${ext}" não suportado. Use CSV ou XLSX.`);
      }
    } catch (e: any) {
      setUploadError(e.message || 'Erro ao processar arquivo.');
    } finally {
      setUploadLoading(false);
    }
  }, []);

  const onFileChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) handleFile(file);
    e.target.value = '';
  }, [handleFile]);

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const file = e.dataTransfer.files?.[0];
    if (file) handleFile(file);
  }, [handleFile]);

  const clearUpload = () => {
    setUploadedRows(null);
    setUploadFileName('');
    setUploadError('');
    setActiveSource('sienge');
  };

  const clearFilters = () => {
    setPeriodMode('last6m');
    setStartDate(undefined);
    setEndDate(undefined);
    setSelectedCompany('all');
    setSelectedBuilding('all');
    setHideInternal(true);
    setDetailLimit(500);
  };

  const handlePrintDetalhamento = useCallback(() => {
    const printableRows = detailedRows.map((row) => {
      const baixa = row.baixa ? formatDateSafe(row.baixa, row.baixa) : '00/00/0000';
      const vencto = (row.vencto || row.data) ? formatDateSafe(row.vencto || row.data, String(row.vencto || row.data)) : '—';
      const doc = getDocumentParts(row.documento);
      return `
        <tr>
          <td>${baixa}</td>
          <td>${vencto}</td>
          <td>${row.clienteFornecedor || row.historico || '—'}</td>
          <td>${row.tituloParcela || '—'}</td>
          <td>${doc.original}${doc.treated ? ` (${doc.treated})` : ''}</td>
          <td style="text-align:right;color:#0f9d58;">${fmt(row.entrada || 0)}</td>
          <td style="text-align:right;color:#d93025;">${fmt(row.saida || 0)}</td>
          <td style="text-align:right;${row.saldo >= 0 ? 'color:#0f9d58;' : 'color:#d93025;'}">${fmt(row.saldo)}</td>
        </tr>`;
    }).join('');

    const html = `
      <html>
        <head>
          <title>Detalhamento Leandro</title>
          <style>
            body { font-family: Arial, sans-serif; padding: 16px; }
            h1 { margin: 0 0 8px; font-size: 18px; }
            .meta { margin-bottom: 10px; font-size: 12px; }
            table { width: 100%; border-collapse: collapse; font-size: 11px; }
            th, td { border: 1px solid #ddd; padding: 6px; }
            th { background: #f5f5f5; text-transform: uppercase; font-size: 10px; }
          </style>
        </head>
        <body>
          <h1>Relatorio Leandro - Detalhamento</h1>
          <div class="meta">Periodo: ${periodLabel} | Selecao por: Data de vencimento/pagamento | Tipo de analise: A realizar</div>
          <div class="meta">Empresa: ${selectedCompanyLabel} | Area de negocio: ${selectedBuildingLabel} | Centro de custo: 28 - ALMOXARIFADO DINAMICA</div>
          <table>
            <thead>
              <tr>
                <th>Baixa</th><th>Vencto</th><th>Cliente/Fornecedor/Com</th><th>Titulo/Par</th><th>Documento</th><th>Credito</th><th>Debito</th><th>Saldo</th>
              </tr>
            </thead>
            <tbody>${printableRows}</tbody>
          </table>
        </body>
      </html>`;

    const printWindow = window.open('', '_blank', 'width=1200,height=800');
    if (!printWindow) return;
    printWindow.document.open();
    printWindow.document.write(html);
    printWindow.document.close();
    printWindow.focus();
    printWindow.print();
  }, [detailedRows, periodLabel, selectedBuildingLabel, selectedCompanyLabel]);

  const downloadTemplate = () => {
    const ws = XLSX.utils.aoa_to_sheet([
      ['Data', 'Historico', 'Obra', 'Entrada', 'Saida', 'Saldo'],
      ['01/01/2026', 'Recebimento cliente', 'Obra Exemplo', '50000', '0', '50000'],
      ['05/01/2026', 'Pagamento fornecedor', 'Obra Exemplo', '0', '20000', '30000'],
    ]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Leandro');
    XLSX.writeFile(wb, 'template_leandro.xlsx');
  };

  // ── Render ────────────────────────────────────────────────────────────────

  const card = 'bg-[#161618] border border-white/5 rounded-2xl shadow-xl';
  const th = 'text-[10px] font-black uppercase tracking-widest text-gray-500 px-3 py-2 text-left whitespace-nowrap';
  const td = 'text-xs px-3 py-2 align-middle';

  return (
    <motion.div
      key="leandro"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -16 }}
      className="space-y-6"
    >
      {/* ── Header ─────────────────────────────────────────────── */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-2xl font-black text-white flex items-center gap-3">
            <DollarSign className="text-emerald-500" size={26} />
            Relatório Leandro — Fluxo de Caixa
          </h2>
          <p className="text-xs text-gray-500 mt-1">
            {activeSource === 'arquivo'
              ? `📂 Dados do arquivo: ${uploadFileName}`
              : '🔗 Dados sincronizados do Sienge'}
          </p>
        </div>

        {/* Botões de ação */}
        <div className="flex flex-wrap items-center gap-2">
          {/* Fonte de dados */}
          <div className="flex items-center bg-black/30 border border-white/10 rounded-xl p-1">
            <button
              onClick={() => setActiveSource('sienge')}
              className={cn(
                'px-3 py-1.5 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all',
                activeSource === 'sienge' ? 'bg-emerald-600 text-white' : 'text-gray-400 hover:text-white'
              )}
            >
              Sienge
            </button>
            <button
              onClick={() => uploadedRows && setActiveSource('arquivo')}
              disabled={!uploadedRows}
              className={cn(
                'px-3 py-1.5 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all',
                activeSource === 'arquivo' ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-white disabled:opacity-30'
              )}
            >
              Arquivo
            </button>
          </div>

          <button
            onClick={syncSienge}
            disabled={syncing}
            className="h-9 px-4 bg-[#1B3C58] hover:bg-[#234b6e] text-white font-bold rounded-xl flex items-center gap-2 text-xs transition-all"
          >
            <RefreshCw size={14} className={cn(syncing && 'animate-spin')} />
            {syncing ? 'Sincronizando...' : 'Sincronizar Sienge'}
          </button>

          <button
            onClick={downloadTemplate}
            className="h-9 px-3 bg-white/5 hover:bg-white/10 border border-white/10 text-gray-300 font-bold rounded-xl flex items-center gap-2 text-xs transition-all"
            title="Baixar modelo XLSX"
          >
            <Download size={14} />
            Modelo
          </button>
        </div>
      </div>

      <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl relative z-10 flex flex-wrap gap-4 items-end">
        <div className="space-y-2 flex-1 min-w-[260px]">
          <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Período</label>
          <div className="flex items-center gap-2 bg-black/30 border border-white/10 rounded-xl p-1 h-11">
            <button
              onClick={() => {
                setPeriodMode('last6m');
                setStartDate(undefined);
                setEndDate(undefined);
              }}
              className={cn(
                'h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all',
                periodMode === 'last6m'
                  ? 'bg-orange-600 text-white'
                  : 'text-gray-300 hover:text-white hover:bg-white/10'
              )}
            >
              Últimos 6 meses
            </button>
            <button
              onClick={() => {
                setPeriodMode('all');
                setStartDate(undefined);
                setEndDate(undefined);
              }}
              className={cn(
                'h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all',
                periodMode === 'all'
                  ? 'bg-sky-600 text-white'
                  : 'text-gray-300 hover:text-white hover:bg-white/10'
              )}
            >
              Período total
            </button>
          </div>
        </div>

        <div className="space-y-2 flex-1 min-w-[200px]">
          <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Inicial</label>
          <div className="relative">
            <CalendarIcon className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={14} />
            <input
              type="date"
              value={(startDate || (periodMode === 'last6m' ? defaultStartDate : undefined)) ? format(startDate || defaultStartDate, 'yyyy-MM-dd') : ''}
              onChange={(e) => {
                const value = e.target.value ? parse(e.target.value, 'yyyy-MM-dd', new Date()) : undefined;
                setPeriodMode('last6m');
                setStartDate(value);
              }}
              className="w-full h-11 pl-9 pr-3 rounded-xl bg-black/40 border border-white/10 text-white font-bold focus:outline-none focus:border-orange-500/50"
            />
          </div>
        </div>

        <div className="space-y-2 flex-1 min-w-[200px]">
          <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Final</label>
          <div className="relative">
            <CalendarIcon className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={14} />
            <input
              type="date"
              value={(endDate || (periodMode === 'last6m' ? defaultEndDate : undefined)) ? format(endDate || defaultEndDate, 'yyyy-MM-dd') : ''}
              onChange={(e) => {
                const value = e.target.value ? endOfDay(parse(e.target.value, 'yyyy-MM-dd', new Date())) : undefined;
                setPeriodMode('last6m');
                setEndDate(value);
              }}
              className="w-full h-11 pl-9 pr-3 rounded-xl bg-black/40 border border-white/10 text-white font-bold focus:outline-none focus:border-orange-500/50"
            />
          </div>
          {maxAvailableSiengeDate && (
            <p className="text-[10px] text-gray-500 mt-1">
              Data maxima disponivel no Sienge: {format(maxAvailableSiengeDate, 'dd/MM/yyyy')}
            </p>
          )}
        </div>

        <div className="space-y-2 flex-1 min-w-[220px]">
          <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Empresa (Sienge)</label>
          <select
            value={selectedCompany}
            onChange={(e) => {
              const value = e.target.value;
              setSelectedCompany(value);
              setSelectedBuilding('all');
            }}
            className="w-full h-11 rounded-xl bg-black/40 border border-white/10 text-white font-bold px-3 focus:outline-none focus:border-orange-500/50"
          >
            <option value="all">Todas as Empresas</option>
            {companies.map((c: any) => (
              <option key={`leandro-company-${c.id}`} value={String(c.id)}>
                {c.name} (ID: {c.id})
              </option>
            ))}
          </select>
        </div>

        <div className="space-y-2 flex-1 min-w-[220px]">
          <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obras</label>
          <select
            value={selectedBuilding}
            onChange={(e) => setSelectedBuilding(e.target.value)}
            className="w-full h-11 rounded-xl bg-black/40 border border-white/10 text-white font-bold px-3 focus:outline-none focus:border-orange-500/50"
          >
            <option value="all">Todas as Obras</option>
            {availableBuildings.map((b: any) => (
              <option key={`leandro-building-${b.id}`} value={String(b.id)}>
                {b.name} (ID: {b.id})
              </option>
            ))}
          </select>
        </div>

        <div className="space-y-2 flex-1 min-w-[200px]">
          <label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Transf. Internas</label>
          <button
            onClick={() => setHideInternal((prev) => !prev)}
            className={cn(
              'w-full h-11 rounded-xl justify-center font-bold transition-all border',
              hideInternal
                ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/20'
                : 'bg-black/40 border-white/10 text-gray-400 hover:bg-white/5 hover:text-white'
            )}
          >
            {hideInternal ? 'Ocultas' : 'Visíveis'}
          </button>
        </div>

        <div className="flex items-end gap-2 min-w-[220px]">
          <button
            onClick={clearFilters}
            className="h-11 px-4 rounded-xl border border-white/10 bg-black/30 text-gray-300 hover:bg-white/10 hover:text-white font-bold text-sm"
          >
            Limpar Filtros
          </button>
          <button
            onClick={() => setApplyTick((prev) => prev + 1)}
            disabled={syncing}
            className="h-11 px-4 rounded-xl font-bold bg-[#1B3C58] hover:bg-[#234b6e] text-white text-sm disabled:opacity-70"
          >
            <span className="inline-flex items-center">
              <CheckCircle2 size={15} className='mr-2' />
              {syncing ? 'Filtrando...' : 'Filtrar'}
            </span>
          </button>
        </div>
      </div>

      {/* ── Upload Zone ───────────────────────────────────────────── */}
      <div
        onDrop={onDrop}
        onDragOver={e => e.preventDefault()}
        className={cn(
          'border-2 border-dashed rounded-2xl p-6 transition-all',
          uploadedRows
            ? 'border-emerald-500/40 bg-emerald-500/5'
            : 'border-white/10 bg-white/[0.02] hover:border-blue-500/40 hover:bg-blue-500/5'
        )}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv,.xlsx,.xls,.pdf"
          className="hidden"
          onChange={onFileChange}
        />

        {uploadedRows ? (
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <CheckCircle2 className="text-emerald-500" size={20} />
              <div>
                <p className="text-sm font-bold text-emerald-400">{uploadFileName}</p>
                <p className="text-xs text-gray-500">{uploadedRows.length} registros importados com sucesso</p>
              </div>
            </div>
            <button
              onClick={clearUpload}
              className="text-gray-500 hover:text-white transition-colors p-2 rounded-lg hover:bg-white/10"
            >
              <X size={16} />
            </button>
          </div>
        ) : (
          <div className="flex flex-col sm:flex-row items-center gap-4">
            <div className="w-12 h-12 rounded-xl bg-blue-500/10 flex items-center justify-center flex-shrink-0">
              <Upload className="text-blue-400" size={22} />
            </div>
            <div className="flex-1 text-center sm:text-left">
              <p className="text-sm font-bold text-white mb-1">
                Importar arquivo financeiro
              </p>
              <p className="text-xs text-gray-500">
                Arraste um arquivo ou{' '}
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="text-blue-400 hover:text-blue-300 font-bold underline transition-colors"
                >
                  clique para selecionar
                </button>
                . Formatos suportados: <span className="text-gray-400 font-bold">CSV, XLSX, XLS</span>
              </p>
              {uploadError && (
                <div className="flex items-center gap-2 mt-2 text-red-400 text-xs font-bold">
                  <AlertCircle size={14} />
                  {uploadError}
                </div>
              )}
            </div>
            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={uploadLoading}
              className="h-9 px-5 bg-blue-600 hover:bg-blue-700 text-white font-bold rounded-xl text-xs transition-all flex items-center gap-2 flex-shrink-0"
            >
              {uploadLoading ? <RefreshCw size={14} className="animate-spin" /> : <Upload size={14} />}
              {uploadLoading ? 'Processando...' : 'Selecionar Arquivo'}
            </button>
          </div>
        )}
      </div>

      {/* ── KPI Cards ─────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        {[
          {
            label: 'Total Entradas',
            value: fmt(totalEntradas),
            icon: TrendingUp,
            color: 'emerald',
            sub: `${activeRows.filter(r => r.entrada > 0).length} registros`,
          },
          {
            label: 'Total Saídas',
            value: fmt(totalSaidas),
            icon: TrendingDown,
            color: 'red',
            sub: `${activeRows.filter(r => r.saida > 0).length} registros`,
          },
          {
            label: 'Saldo do Período',
            value: fmt(saldoFinal),
            icon: DollarSign,
            color: saldoFinal >= 0 ? 'emerald' : 'red',
            sub: 'Acumulado',
          },
          {
            label: 'Total de Obras',
            value: totalObrasFiltradas,
            icon: Building2,
            color: 'blue',
            sub: 'Baseado no filtro',
          },
        ].map((kpi, i) => (
          <div key={i} className={card + ' p-4 relative overflow-hidden group'}>
            <div className="absolute top-0 right-0 p-3 opacity-10 group-hover:opacity-20 transition-opacity">
              <kpi.icon size={36} className={`text-${kpi.color}-500`} />
            </div>
            <p className={`text-[10px] font-black uppercase tracking-widest text-${kpi.color}-500/70 mb-1`}>
              {kpi.label}
            </p>
            <p className={cn('text-xl sm:text-2xl font-black', `text-${kpi.color}-400`)}>
              {kpi.value}
            </p>
            <p className="text-[10px] text-gray-600 mt-1">{kpi.sub}</p>
          </div>
        ))}
      </div>

      {/* ── Resumo por Mês e por Obra ──────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Por Mês */}
        <div className={card + ' flex flex-col'}>
          <div className="p-4 border-b border-white/5">
            <h3 className="font-black uppercase tracking-tight text-white text-sm">
              Movimentação Mensal
            </h3>
          </div>
          <div className="overflow-auto max-h-60 custom-scrollbar">
            <table className="w-full">
              <thead className="bg-black/60 sticky top-0">
                <tr>
                  {['Mês', 'Entradas', 'Saídas', 'Saldo'].map(h => (
                    <th key={h} className={th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {periodos.length === 0 ? (
                  <tr><td colSpan={4} className="text-center py-6 text-gray-600 text-xs">Sem dados</td></tr>
                ) : periodos.map((p, i) => (
                  <tr key={i} className="border-t border-white/5 hover:bg-white/[0.03]">
                    <td className={td + ' font-bold text-white'}>{p.mes}</td>
                    <td className={td + ' text-emerald-400 font-mono'}>{fmt(p.entradas)}</td>
                    <td className={td + ' text-red-400 font-mono'}>{fmt(p.saidas)}</td>
                    <td className={cn(td, 'font-mono font-black', p.saldo >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                      {fmt(p.saldo)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Por Obra */}
        <div className={card + ' flex flex-col'}>
          <div className="p-4 border-b border-white/5">
            <h3 className="font-black uppercase tracking-tight text-white text-sm">
              Resumo por Obra (Top 10)
            </h3>
          </div>
          <div className="overflow-auto max-h-60 custom-scrollbar">
            <table className="w-full">
              <thead className="bg-black/60 sticky top-0">
                <tr>
                  {['Obra', 'Entradas', 'Saídas', 'Saldo'].map(h => (
                    <th key={h} className={th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {porObra.length === 0 ? (
                  <tr><td colSpan={4} className="text-center py-6 text-gray-600 text-xs">Sem dados</td></tr>
                ) : porObra.map((p, i) => (
                  <tr key={i} className="border-t border-white/5 hover:bg-white/[0.03]">
                    <td className={td + ' font-bold text-gray-200 max-w-[240px] truncate'} title={p.obra}>{p.obra}</td>
                    <td className={td + ' text-emerald-400 font-mono'}>{fmt(p.entradas)}</td>
                    <td className={td + ' text-red-400 font-mono'}>{fmt(p.saidas)}</td>
                    <td className={cn(td, 'font-mono font-black', p.saldo >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                      {fmt(p.saldo)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className={card + ' flex flex-col'}>
        <div className="p-4 border-b border-white/5">
          <h3 className="font-black uppercase tracking-tight text-white text-sm">Ultimas 10 Movimentacoes</h3>
        </div>
        <div className="overflow-auto max-h-72 custom-scrollbar">
          <table className="w-full">
            <thead className="bg-black/60 sticky top-0">
              <tr>
                <th className={th}>Data</th>
                <th className={th}>Cliente/Fornecedor</th>
                <th className={th}>Obra</th>
                <th className={th + ' text-right'}>Credito</th>
                <th className={th + ' text-right'}>Debito</th>
                <th className={th + ' text-right'}>Saldo</th>
              </tr>
            </thead>
            <tbody>
              {ultimasMovimentacoes.length === 0 ? (
                <tr><td colSpan={6} className="text-center py-6 text-gray-600 text-xs">Sem dados</td></tr>
              ) : ultimasMovimentacoes.map((r, idx) => (
                <tr key={`ult-${idx}`} className="border-t border-white/5 hover:bg-white/[0.03]">
                  <td className={td + ' text-gray-300 whitespace-nowrap'}>{formatDateSafe(r.data || r.vencto || r.baixa, r.data || '—')}</td>
                  <td className={td + ' text-gray-200 truncate max-w-[360px]'} title={r.clienteFornecedor || r.historico}>{r.clienteFornecedor || r.historico || '—'}</td>
                  <td className={td + ' text-gray-400 truncate max-w-[260px]'} title={r.obra}>{r.obra || '—'}</td>
                  <td className={td + ' text-right text-emerald-400 font-mono'}>{fmt(r.entrada || 0)}</td>
                  <td className={td + ' text-right text-red-400 font-mono'}>{fmt(r.saida || 0)}</td>
                  <td className={cn(td, 'text-right font-black font-mono', r.saldo >= 0 ? 'text-emerald-400' : 'text-red-400')}>{fmt(r.saldo)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── Tabela Detalhada ───────────────────────────────────────── */}
      <div className={card + ' flex flex-col'}>
        <div className="p-4 border-b border-white/5 flex items-center justify-between">
          <h3 className="font-black uppercase tracking-tight text-white text-sm flex items-center gap-2">
            <FileText size={16} className="text-orange-500" />
            Detalhamento — {activeRows.length.toLocaleString('pt-BR')} registros
          </h3>
          <div className="flex items-center gap-2">
            <button
              onClick={handlePrintDetalhamento}
              className="h-8 px-3 rounded-lg border border-white/10 bg-black/30 text-gray-200 hover:bg-white/10 font-bold text-xs inline-flex items-center"
            >
              <Printer size={13} className="mr-2" /> Imprimir
            </button>
            {activeRows.length > 0 && (
              <span className="text-[10px] font-bold bg-orange-500/10 text-orange-400 px-2 py-1 rounded-full border border-orange-500/20">
                {activeSource === 'arquivo' ? '📂 Arquivo' : '🔗 Sienge'}
              </span>
            )}
          </div>
        </div>
        <div className="px-4 py-3 border-b border-white/5 text-xs text-gray-300 space-y-1 bg-black/20">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            <div><span className="font-black text-gray-400">Periodo:</span> {periodLabel}</div>
            <div><span className="font-black text-gray-400">Selecao por:</span> Data de vencimento/pagamento</div>
            <div><span className="font-black text-gray-400">Tipo de analise:</span> A realizar</div>
          </div>
          <div><span className="font-black text-gray-400">Valores corrigidos por:</span> REAL <span className="ml-3 font-black text-gray-400">Valores apresentados em:</span> REAL</div>
          <div><span className="font-black text-gray-400">Empresa:</span> {selectedCompanyLabel}</div>
          <div><span className="font-black text-gray-400">Area de negocio:</span> {selectedBuildingLabel}</div>
          <div><span className="font-black text-gray-400">Centro de custo:</span> 28 - ALMOXARIFADO DINAMICA</div>
        </div>

        <div className="overflow-auto max-h-[560px] custom-scrollbar">
          <table className="w-full min-w-[1450px] table-fixed border-separate border-spacing-0">
            <thead className="bg-black/60 sticky top-0 z-10">
              <tr>
                <th className={th + ' w-[130px]'}>Baixa</th>
                <th className={th + ' w-[130px]'}>Vencto</th>
                <th className={th + ' w-[420px]'}>Cliente/Fornecedor/Com</th>
                <th className={th + ' w-[140px]'}>Título/Par</th>
                <th className={th + ' w-[190px]'}>Documento</th>
                <th className={th + ' w-[150px] text-right'}>Crédito</th>
                <th className={th + ' w-[150px] text-right'}>Débito</th>
                <th className={th + ' w-[150px] text-right'}>Saldo</th>
              </tr>
            </thead>
            <tbody>
              {detailedRows.length === 0 ? (
                <tr>
                  <td colSpan={8} className="text-center py-12 text-gray-600 font-bold text-sm">
                    {activeSource === 'sienge'
                      ? 'Nenhuma movimentação encontrada para os filtros selecionados. Ajuste os filtros ou importe um arquivo.'
                      : 'Nenhum dado no arquivo importado.'}
                  </td>
                </tr>
              ) : displayedDetailRows.map((row, i) => (
                <tr
                  key={i}
                  className={cn(
                    'border-t border-white/5 hover:bg-white/[0.03] transition-colors',
                    row.entrada > 0 ? 'border-l-2 border-l-emerald-600/30' : 'border-l-2 border-l-red-600/20'
                  )}
                >
                  <td className={td + ' text-gray-400 whitespace-nowrap'}>
                    {row.baixa ? formatDateSafe(row.baixa, row.baixa) : '00/00/0000'}
                  </td>
                  <td className={td + ' text-gray-400 whitespace-nowrap'}>
                    {(row.vencto || row.data) ? formatDateSafe(row.vencto || row.data, String(row.vencto || row.data)) : '—'}
                  </td>
                  <td className={td + ' font-bold text-gray-200 truncate'} title={row.clienteFornecedor || row.historico}>
                    {row.clienteFornecedor || row.historico || '—'}
                  </td>
                  <td className={td + ' text-gray-300 font-mono whitespace-nowrap'} title={row.tituloParcela}>
                    {row.tituloParcela || '—'}
                  </td>
                  <td className={td + ' text-gray-400 font-mono whitespace-nowrap truncate'} title={row.documento}>
                    {(() => {
                      const doc = getDocumentParts(row.documento);
                      return (
                        <div className="leading-tight">
                          <div className="truncate">{doc.original}</div>
                          {doc.treated && (
                            <div className="text-[10px] text-orange-400 truncate">{doc.treated}</div>
                          )}
                        </div>
                      );
                    })()}
                  </td>
                  <td className={td + ' text-right font-mono text-emerald-400 whitespace-nowrap'}>
                    {fmt(row.entrada || 0)}
                  </td>
                  <td className={td + ' text-right font-mono text-red-400 whitespace-nowrap'}>
                    {fmt(row.saida || 0)}
                  </td>
                  <td className={cn(td, 'text-right font-mono font-black whitespace-nowrap', row.saldo >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                    {fmt(row.saldo)}
                  </td>
                </tr>
              ))}
              {detailedRows.length > detailLimit && (
                <tr>
                  <td colSpan={8} className="text-center py-3 text-gray-600 text-xs font-bold">
                    Exibindo {detailLimit.toLocaleString('pt-BR')} de {detailedRows.length.toLocaleString('pt-BR')} registros.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {detailedRows.length > detailLimit && (
          <div className="p-3 border-t border-white/5 flex justify-center">
            <button
              onClick={() => setDetailLimit((prev) => prev + 500)}
              className="h-9 px-4 rounded-xl border border-white/10 bg-black/30 text-gray-200 hover:bg-white/10 font-bold text-xs"
            >
              Mostrar mais 500 registros
            </button>
          </div>
        )}
      </div>
    </motion.div>
  );
}
