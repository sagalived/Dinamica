import React, { createContext, useContext, useState, useEffect, useMemo, useCallback, useRef, Fragment } from 'react';
import { useAuth } from './AuthContext';
import { useTheme } from './ThemeContext';
import { 
  LayoutDashboard, Bell, Filter, Download, TrendingUp, TrendingDown, 
  DollarSign, Package, Calendar as CalendarIcon, RefreshCw, 
  User as UserIcon, Building2, ChevronRight, Search, Map as MapIcon,
  Wifi, WifiOff, CheckCircle2, AlertCircle, AlertTriangle, FileText, Printer, X,
  Menu, ChevronDown, SlidersHorizontal, Truck, LogOut, Moon, Sun
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { AccessControlTab } from '../components/AccessControl';
import { LoginScreen } from '../components/LoginScreen';
import { NavigationMenu } from '../components/NavigationMenu';

import { LogisticsTab } from '../tabs/logistica/LogisticaTab';
import { DiarioObras as ObrasTab } from '../tabs/obras/ObrasTab';
import { FluxoProjection } from '../tabs/projecao/ProjecaoTab';
import { LeandroTab } from '../tabs/financeiro/Leandro';
import { FinanceiroValores } from '../tabs/financeiro/Valores';
import { FinanceiroAlerta } from '../tabs/financeiro/Alerta';
import { FinanceiroFluxoTab } from '../tabs/financeiro/FluxoCaixa';
import { DashboardGeral } from '../tabs/dashboard/Geral';
import { DashboardFinanceiro } from '../tabs/dashboard/Financeiro';
import { DashboardObras } from '../tabs/dashboard/Obras';
import { DashboardLogistica } from '../tabs/dashboard/Logistica';

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button, buttonVariants } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { parseISO, format, addDays, addMonths, subMonths } from 'date-fns';
import { ptBR } from 'date-fns/locale';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  AreaChart, Area, Cell, PieChart, Pie, LineChart, Line, Legend
} from 'recharts';

import { sienge as api, kanbanApi } from '../lib/api';
import type { AuthUser, Building, Creditor, PriceAlert, PurchaseOrder, User } from '../lib/types';
import { cn } from '../lib/utils';
import { fixText } from '../lib/text';
import { calcularFluxoCaixa } from '../tabs/financeiro/leandroLogic';
import { safeFormat } from '../tabs/dashboard/logic';
import {
  isSettledFinancialStatus,
  toMoney,
  translateStatementType,
  translateStatusLabel,
} from '../tabs/financeiro/logic';
import logoWordmark from '../assets/dinamica-wordmark.svg';
import logoWordmarkDark from '../assets/dinamica-wordmark-dark.svg';


export const SiengeContext = createContext<any>(undefined);

export function useSienge() {
  const context = useContext(SiengeContext);
  if (!context) throw new Error('useSienge must be used within a SiengeProvider');
  return context;
}

export function SiengeProvider({ children }: { children: React.ReactNode }) {

  type SyncInfo = {
    status?: string;
    started_at?: string;
    finished_at?: string;
    message?: string;
    counts?: Record<string, number>;
  } | null;

  type SprintOverview = {
    id: number;
    buildingId: number;
    buildingName: string;
    name: string;
    startDate?: string | null;
    endDate?: string | null;
    color?: string;
    overdue: boolean;
    stats: {
      totalCards: number;
      openCards: number;
      overdueCards: number;
    };
  };

  const { sessionUser, logout, login, checkAuth } = useAuth();
  const { themeMode, toggleThemeMode, isDark } = useTheme();
  const [activeTab, setActiveTab] = useState<string>('dashboard');
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncProgress, setSyncProgress] = useState<number>(0);
  const [dataRevision, setDataRevision] = useState(0);
  const [apiStatus, setApiStatus] = useState<'online' | 'offline' | 'checking'>('checking');
  const [saldoBancario, setSaldoBancario] = useState<number | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  const [syncInfo, setSyncInfo] = useState<SyncInfo>(null);
  const [startDate, setStartDate] = useState<Date | undefined>();
  const [endDate, setEndDate] = useState<Date | undefined>();
  const [fcStartDate, setFcStartDate] = useState<Date | undefined>();
  const [fcEndDate, setFcEndDate] = useState<Date | undefined>();
  const [fcPeriodMode, setFcPeriodMode] = useState<'last6m' | 'all'>('last6m');
  const [fcSelectedCompany, setFcSelectedCompany] = useState<string>('all');
  const [fcSelectedBuilding, setFcSelectedBuilding] = useState<string>('all');
  const [fcHideInternal, setFcHideInternal] = useState<boolean>(true);
  const [isPrinting, setIsPrinting] = useState(false);
  const [newOrderAlert, setNewOrderAlert] = useState<PurchaseOrder | null>(null);
  const [selectedAlertOrder, setSelectedAlertOrder] = useState<PurchaseOrder | null>(null);
  const [modalItemHistory, setModalItemHistory] = useState<{name: string, history: {price: number, date: string, orderId?: number, buyerId?: string, creditorId?: string}[]} | null>(null);
  const [expandedDetail, setExpandedDetail] = useState<number | null>(null);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const [financeLimit, setFinanceLimit] = useState(100);
  const [kanbanOverview, setKanbanOverview] = useState<SprintOverview[]>([]);
  const [kanbanOverviewLoading, setKanbanOverviewLoading] = useState(false);
  const knownOrderIdsRef = useRef<Set<number>>(new Set());
  const syncProgressTimerRef = useRef<number | null>(null);
  // Refs para acesso ao all* no fallback do filtro (sem adicionar nos deps do useEffect → evita loop)
  const allOrdersRef = useRef<PurchaseOrder[]>([]);
  const allFinancialTitlesRef = useRef<any[]>([]);
  const allReceivableTitlesRef = useRef<any[]>([]);

  type ReportType = 'pagar' | 'receber' | 'abertos' | null;
  const [reportType, setReportType] = useState<ReportType>(null);
  
  const [alertSortConfig, setAlertSortConfig] = useState<{ key: 'date' | 'vlrUnit' | 'vlrAtual' | 'valorTotal', direction: 'asc' | 'desc' } | null>(null);
  const isRestrictedUser = sessionUser?.role === 'user';

  const setSyncProgressSafe = useCallback((value: number) => {
    const clamped = Math.max(0, Math.min(100, Math.round(value)));
    setSyncProgress(clamped);
  }, []);

  const clearSyncProgressTimer = useCallback(() => {
    if (syncProgressTimerRef.current != null) {
      window.clearInterval(syncProgressTimerRef.current);
      syncProgressTimerRef.current = null;
    }
  }, []);

  const startLinearProgress = useCallback((from: number, to: number, durationMs: number) => {
    clearSyncProgressTimer();
    const startAt = Date.now();
    const delta = to - from;
    setSyncProgressSafe(from);
    syncProgressTimerRef.current = window.setInterval(() => {
      const elapsed = Date.now() - startAt;
      const t = Math.max(0, Math.min(1, durationMs <= 0 ? 1 : elapsed / durationMs));
      setSyncProgressSafe(from + delta * t);
      if (t >= 1) {
        clearSyncProgressTimer();
      }
    }, 120);
  }, [clearSyncProgressTimer, setSyncProgressSafe]);

  const toggleSort = (key: 'date' | 'vlrUnit' | 'vlrAtual' | 'valorTotal') => {
    setAlertSortConfig(prev => {
      if (prev?.key === key) {
        if (prev.direction === 'asc') return { key, direction: 'desc' };
        return null; // Removes sort on third click
      }
      return { key, direction: 'asc' };
    });
  };

  const renderSortIcon = (key: string) => {
    if (alertSortConfig?.key !== key) return <span className="ml-1 opacity-0 transition-opacity group-hover:opacity-40">▼</span>;
    return alertSortConfig.direction === 'asc' ? <span className="ml-1 text-orange-500">▲</span> : <span className="ml-1 text-orange-500">▼</span>;
  };

  useEffect(() => {
    if (newOrderAlert) {
      const timer = setTimeout(() => {
        setNewOrderAlert(null);
      }, 10000);
      return () => clearTimeout(timer);
    }
  }, [newOrderAlert]);

  // Data State
  const [buildings, setBuildings] = useState<Building[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [requesters, setRequesters] = useState<User[]>([]);
  const [creditors, setCreditors] = useState<Creditor[]>([]);
  const [companies, setCompanies] = useState<any[]>([]);
  const [orders, setOrders] = useState<PurchaseOrder[]>([]);
  const [allOrders, setAllOrders] = useState<PurchaseOrder[]>([]);
  const [priceAlerts, setPriceAlerts] = useState<PriceAlert[]>([]);
  const [financialTitles, setFinancialTitles] = useState<any[]>([]);
  const [allFinancialTitles, setAllFinancialTitles] = useState<any[]>([]);
  const [receivableTitles, setReceivableTitles] = useState<any[]>([]);
  const [allReceivableTitles, setAllReceivableTitles] = useState<any[]>([]);
  const [nfeDocuments, setNfeDocuments] = useState<any[]>([]);
  const [itemsDetailsMap, setItemsDetailsMap] = useState<Record<string, any>>({});
  const [quotationsMap, setQuotationsMap] = useState<Record<string, any>>({});
  const [latestPricesMap, setLatestPricesMap] = useState<Record<string, number>>({});
  const [baselinePricesMap, setBaselinePricesMap] = useState<Record<string, number>>({});
  const requestedItemsRef = useRef<Set<string>>(new Set());
  const requestedQuotesRef = useRef<Set<string>>(new Set());
  
  const globalItemHistory = useMemo(() => {
    const historyMap: Record<string, { price: number, date: string, orderId?: number, buyerId?: string, creditorId?: string }[]> = {};

    const addEntry = (name: string, price: number, date: string, orderId?: number, buyerId?: string, creditorId?: string) => {
      if (!name || !price || price <= 0) return;
      if (!historyMap[name]) historyMap[name] = [];
      // Avoid duplicates (same price+date+supplier)
      const dup = historyMap[name].some(e => e.price === price && e.date === date && e.creditorId === creditorId);
      if (!dup) historyMap[name].push({ price, date, orderId, buyerId, creditorId });
    };

    if (orders.length > 0) {
      orders.forEach(order => {
        const buyerId = String(order.buyerId || '');
        const creditorId = String((order as any).supplierId || (order as any).creditorId || '');

        // 1. Items do pedido (preço comprado)
        const actualItems = itemsDetailsMap[order.id] || order.items;
        if (actualItems) {
          actualItems.forEach((item: any) => {
            const name = item.description || item.resourceDescription || item.descricao;
            const price = Number(item.unitPrice || item.valorUnitario || item.netPrice || 0);
            addEntry(name, price, order.date, order.id, buyerId, creditorId);
          });
        }

        // 2. Cotações internas (outros fornecedores concorrentes do mesmo pedido)
        const quotationEntry = quotationsMap[String(order.id)];
        // Handle both old format (array) and new format ({quotes: [...], ...})
        const quotesArray: any[] = Array.isArray(quotationEntry)
          ? quotationEntry
          : (quotationEntry?.quotes && Array.isArray(quotationEntry.quotes) ? quotationEntry.quotes : []);
        
        quotesArray.forEach((quote: any) => {
          const quoteOrderId = quote.orderId || order.id;
          // Use the quote's own supplierId and date (each competitor has its own order)
          const quoteSupplierId = String(quote.supplierId || quote.creditorId || quote.idFornecedor || '');
          const quoteDate = quote.date || order.date;
          const quoteItems = quote.items || quote.itens || [];
          quoteItems.forEach((qi: any) => {
            const name = qi.description || qi.descricao || qi.resourceDescription;
            const price = Number(qi.unitPrice || qi.valorUnitario || qi.netPrice || 0);
            addEntry(name, price, quoteDate, quoteOrderId, buyerId, quoteSupplierId);
          });
        });
      });
    }
    return historyMap;
  }, [orders, itemsDetailsMap, quotationsMap]);

  // Reactivity: Auto-update price alerts whenever itemsDetailsMap or orders change
  useEffect(() => {
    if (Object.keys(globalItemHistory).length > 0) {
      const alerts: PriceAlert[] = [];

      Object.keys(globalItemHistory).forEach(name => {
        const history = globalItemHistory[name].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
        if (history.length >= 2) {
          const latest = history[0];
          const previous = history[1];
          const diff = ((latest.price - previous.price) / previous.price) * 100;
          if (diff > 5) { 
            alerts.push({
              item: name,
              oldPrice: previous.price,
              newPrice: latest.price,
              diff: Number(diff.toFixed(1)),
              oldDate: previous.date,
              newDate: latest.date,
              history: history.slice().reverse()
            });
          }
        }
      });
      setPriceAlerts(alerts);
    } else {
      setPriceAlerts([]);
    }
  }, [globalItemHistory]);
  
  // Selection State for Map
  const [selectedMapBuilding, setSelectedMapBuilding] = useState<number | null>(null);
  const [buildingSearch, setBuildingSearch] = useState('');
  const [editingEngineer, setEditingEngineer] = useState(false);
  const [engineerDraft, setEngineerDraft] = useState('');
  const [savingEngineer, setSavingEngineer] = useState(false);

  const isAdmin = sessionUser?.role === 'developer' || sessionUser?.role === 'admin';

  // Filter State
  const [selectedCompany, setSelectedCompany] = useState<string>('all');
  const [selectedUser, setSelectedUser] = useState<string>('all');
  const [selectedRequester, setSelectedRequester] = useState<string>('all');
  const [globalPeriodMode, setGlobalPeriodMode] = useState<'last6m' | 'all'>('last6m');
  const migratedLegacyDateFilterRef = useRef(false);

  useEffect(() => {
    if (migratedLegacyDateFilterRef.current) return;
    migratedLegacyDateFilterRef.current = true;

    // Compatibilidade: em sessoes antigas, o filtro "data final = hoje" vinha predefinido
    // e podia zerar a tela. Se nenhum outro filtro estiver ativo, limpamos esse valor.
    if (!startDate && endDate && selectedCompany === 'all' && selectedUser === 'all' && selectedRequester === 'all') {
      setEndDate(undefined);
    }
  }, [endDate, selectedCompany, selectedRequester, selectedUser, startDate]);

  const checkConnection = useCallback(async () => {
    try {
      const response = await api.get('/test');
      const isLive = response.data?.live?.ok === true || response.data?.ok === true;
      const hasCache = Boolean(
        response.data?.cache?.pedidos || response.data?.cache?.financeiro || response.data?.cache?.receber
      );
      const isConnected = Boolean(isLive || hasCache);
      if (response.data?.latestSync) {
        setSyncInfo(response.data.latestSync);
        const syncDate = response.data.latestSync.finished_at || response.data.latestSync.started_at;
        if (syncDate) {
          const parsed = new Date(syncDate);
          if (!Number.isNaN(parsed.getTime())) {
            setLastUpdate(parsed);
          }
        }
      }
      setApiStatus(isConnected ? 'online' : 'offline');
      return isConnected;
    } catch (error) {
      console.error('Connection test failed:', error);
      setApiStatus('offline');
      return false;
    }
  }, []);

  const bootstrapHasCoreData = useCallback((payload: any) => {
    // Verifica flag cacheReady do bootstrap leve (backend indica se DB cache tem dados)
    if (payload?.cacheReady === true) return true;
    // Fallback legado: verifica arrays de transações (quando bootstrap retornava tudo)
    const pedidos = Array.isArray(payload?.pedidos) ? payload.pedidos.length : 0;
    const financeiro = Array.isArray(payload?.financeiro) ? payload.financeiro.length : 0;
    const receber = Array.isArray(payload?.receber) ? payload.receber.length : 0;
    return pedidos > 0 || financeiro > 0 || receber > 0;
  }, []);

  const waitForSharedCache = useCallback(async (maxAttempts = 8, delayMs = 2500) => {
    for (let i = 0; i < maxAttempts; i += 1) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      const refreshed = await api.get('/bootstrap');
      if (bootstrapHasCoreData(refreshed.data)) {
        return refreshed.data;
      }
    }
    return null;
  }, [bootstrapHasCoreData]);

  const applyBootstrapData = useCallback((payload: any) => {
    const bDataRaw = Array.isArray(payload?.obras) ? payload.obras : [];
    const uDataRaw = Array.isArray(payload?.usuarios) ? payload.usuarios : [];
    const cDataRaw = Array.isArray(payload?.credores) ? payload.credores : [];
    const compDataRaw = Array.isArray(payload?.companies) ? payload.companies : [];
    const rawOrdersArray = Array.isArray(payload?.pedidos) ? payload.pedidos : [];
    const fDataRaw = Array.isArray(payload?.financeiro) ? payload.financeiro : [];
    const rDataRaw = Array.isArray(payload?.receber) ? payload.receber : [];

    const cacheCounts = payload?.cacheCounts;
    const pedidosCount = Number(cacheCounts?.pedidos ?? 0);
    const financeiroCount = Number(cacheCounts?.financeiro ?? 0);
    const receberCount = Number(cacheCounts?.receber ?? 0);
    const isLightBootstrap = !!cacheCounts && rawOrdersArray.length === 0 && fDataRaw.length === 0 && rDataRaw.length === 0 && (pedidosCount > 0 || financeiroCount > 0 || receberCount > 0);

    const bData = bDataRaw.map((b: any) => ({
      id: b.id,
      name: fixText(b.nome || b.name || b.tradeName || b.enterpriseName || b.address || `Obra ${b.code || b.codigoVisivel || b.id}`),
      code: String(b.code || b.codigoVisivel || b.id || ''),
      latitude: typeof b.latitude === 'number' ? b.latitude : undefined,
      longitude: typeof b.longitude === 'number' ? b.longitude : undefined,
      address: fixText(b.endereco || b.address || b.adress),
      companyId: b.idCompany || b.companyId,
      engineer: fixText(b.engineer || b.responsavel || b.nomeResponsavel || b.gerente || b.engenheiro || b.responsavelTecnico || 'Aguardando Avaliação'),
    }));

    const uData = uDataRaw.map((u: any) => ({
      id: String(u.id),
      name: fixText(u.nome || u.name || `Usuário ${u.id}`),
    }));

    const cData = cDataRaw.map((c: any) => ({
      id: c.id,
      name: fixText(c.nome || c.name || c.nomeFantasia || c.creditorName || `Credor ${c.id}`),
      cnpj: c.cnpj || c.cpfCnpj,
    }));

    const compData = compDataRaw.map((company: any) => ({
      ...company,
      id: company.id,
      name: fixText(company.tradeName || company.name || company.nome || company.companyName || `Empresa ${company.id}`),
      legalName: fixText(company.name || company.nome || company.companyName || `Empresa ${company.id}`),
      cnpj: company.cnpj || company.cpfCnpj || '',
    }));

    setBuildings(bData);
    setUsers(uData);
    setCreditors(cData);
    setCompanies(compData);

    if (rawOrdersArray.length > 0) {
      const uniqueRequesters = new Map<string, User>();
      rawOrdersArray.forEach((o: any) => {
        const solName = fixText(String(o.solicitante || o.requesterId || o.createdBy || '')).replace(/^Comprador\s+/i, '').trim();
        if (solName) {
          uniqueRequesters.set(solName, { id: solName, name: solName });
        }
      });
      setRequesters(Array.from(uniqueRequesters.values()));
    }

    const allOData: PurchaseOrder[] = rawOrdersArray.map((o: any) => {
      const dStr = o.dataEmissao || o.data || o.date || '---';
      let dateNumeric = 0;
      try {
        const d = parseISO(String(dStr));
        dateNumeric = Number.isNaN(d.getTime()) ? 0 : d.getTime();
      } catch {
        const raw = String(dStr || '').trim();
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          dateNumeric = Number.isNaN(d2.getTime()) ? 0 : d2.getTime();
        } else {
          const parsed = Date.parse(raw);
          dateNumeric = Number.isNaN(parsed) ? 0 : parsed;
        }
      }

      return {
        id: o.id || o.numero || 0,
        buildingId: o.idObra || o.codigoVisivelObra || o.buildingId || 0,
        companyId: o.companyId != null ? String(o.companyId) : undefined,
        buyerId: o.idComprador ? String(o.idComprador) : (o.codigoComprador ? String(o.codigoComprador) : (o.buyerId ? String(o.buyerId) : '0')),
        date: dStr,
        dateNumeric,
        totalAmount: parseFloat(o.valorTotal || o.totalAmount) || 0,
        supplierId: o.codigoFornecedor || o.supplierId,
        status: o.situacao || o.status || 'N/A',
        paymentCondition: o.condicaoPagamento || o.paymentMethod || 'A Prazo',
        deliveryDate: o.dataEntrega || o.prazoEntrega || '---',
        internalNotes: o.internalNotes || o.observacao || '',
        createdBy: fixText(o.nomeComprador || o.createdBy || o.criadoPor || ''),
        requesterId: fixText(String(o.solicitante || o.requesterId || o.createdBy || '0')).replace(/^Comprador\s+/i, '').trim(),
      };
    });

    if (!isLightBootstrap && allOData.length > 0) {
      if (knownOrderIdsRef.current.size > 0) {
        const newOrders = allOData.filter(o => !knownOrderIdsRef.current.has(o.id));
        if (newOrders.length > 0) {
          setNewOrderAlert(newOrders[0]);
        }
      }
      allOData.forEach(o => knownOrderIdsRef.current.add(o.id));
    }

    const allFData = fDataRaw.map((f: any) => {
      const dStr = f.dataVencimento || f.issueDate || f.dueDate || f.dataVencimentoProjetado || f.dataEmissao || f.dataContabil || '---';
      let dueDateNumeric = 0;
      try {
        const d = parseISO(String(dStr));
        dueDateNumeric = Number.isNaN(d.getTime()) ? 0 : d.getTime();
      } catch {
        const raw = String(dStr || '').trim();
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          dueDateNumeric = Number.isNaN(d2.getTime()) ? 0 : d2.getTime();
        } else {
          const parsed = Date.parse(raw);
          dueDateNumeric = Number.isNaN(parsed) ? 0 : parsed;
        }
      }

      return {
        id: f.id || f.numero || f.codigoTitulo || f.documentNumber || 0,
        buildingId: f.idObra || f.codigoObra || f.enterpriseId || f.buildingId || 0,
        buildingCode: f.codigoObra || f.idObra || f.enterpriseId || f.buildingId || '',
        buildingName: fixText(f.nomeObra || f.buildingName || f.enterpriseName || ''),
        description: fixText(f.descricao || f.historico || f.tipoDocumento || f.notes || f.observacao || 'Título a Pagar'),
        creditorName: fixText(f.nomeCredor || f.creditorName || f.nomeFantasiaCredor || f.fornecedor || f.credor || 'Credor sem nome'),
        _rawCreditorId: String(f.creditorId || f.debtorId || ''),
        companyId: (() => {
          if (f.companyId != null) return String(f.companyId);
          if (Array.isArray(f.links)) {
            const cLink = f.links.find((l: any) => l.rel === 'company');
            if (cLink && cLink.href) return cLink.href.split('/').pop();
          }
          return undefined;
        })(),
        dueDate: dStr,
        dueDateNumeric,
        amount: parseFloat(f.totalInvoiceAmount || f.valor || f.amount || f.valorTotal || f.valorLiquido || f.valorBruto) || 0,
        status: f.situacao || f.status || 'Pendente',
        documentNumber: String(f.documentNumber || f.numeroDocumento || f.numero || f.codigoTitulo || ''),
      };
    });

    const allRData = rDataRaw.map((r: any) => {
      const dStr = r.dataVencimento || r.data || r.date || r.dataEmissao || r.issueDate || r?.dataVencimentoProjetado || '---';
      let dueDateNumeric = 0;
      try {
        const d = parseISO(String(dStr));
        dueDateNumeric = Number.isNaN(d.getTime()) ? 0 : d.getTime();
      } catch {
        const raw = String(dStr || '').trim();
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          dueDateNumeric = Number.isNaN(d2.getTime()) ? 0 : d2.getTime();
        } else {
          const parsed = Date.parse(raw);
          dueDateNumeric = Number.isNaN(parsed) ? 0 : parsed;
        }
      }
      // rawValue preserva o sinal original da API Sienge (Income positivo, Expense negativo)
      const rawValue: number = r.rawValue ?? (parseFloat(r.valor ?? r.value ?? r.valorSaldo ?? r.totalInvoiceAmount ?? r.valorTotal ?? r.amount ?? 0) || 0);
      return {
        id: r.id || r.numero || r.numeroTitulo || r.codigoTitulo || r.documentNumber || 0,
        buildingId: r.idObra || r.codigoObra || r.buildingId || 0,
        buildingCode: r.codigoObra || r.idObra || r.buildingId || '',
        buildingName: fixText(r.nomeObra || r.buildingName || r.enterpriseName || ''),
        companyId: (() => {
          if (r.companyId != null) return String(r.companyId);
          if (Array.isArray(r.links)) {
            const cLink = r.links.find((l: any) => l.rel === 'company');
            if (cLink && cLink.href) return cLink.href.split('/').pop();
          }
          return undefined;
        })(),
        description: fixText(r.descricao || r.historico || r.observacao || r.notes || r.description || 'Título a Receber'),
        clientName: fixText(r.nomeCliente || r.nomeFantasiaCliente || r.cliente || r.clientName || 'Extrato/Cliente'),
        dueDate: dStr,
        dueDateNumeric,
        amount: Math.abs(rawValue),
        rawValue,
        status: String(r.situacao || r.status || 'ABERTO').toUpperCase(),
        type: r.type || 'Income',
        statementType: r.statementType || '',
        statementOrigin: r.statementOrigin || '',
        // Tit/Parc fields (from Sienge extrato)
        documentId: r.documentId || '',
        documentNumber: r.documentNumber || '',
        installmentNumber: r.installmentNumber ?? null,
        billId: r.billId ?? null,
        // bankAccountCode extraído do links[rel=bank-account] quando não vem como campo direto
        bankAccountCode: r.bankAccountCode || (() => {
          if (!Array.isArray(r.links)) return '';
          const baLink = r.links.find((l: any) => l?.rel === 'bank-account');
          return baLink?.href ? baLink.href.trim().replace(/\/$/, '').split('/').pop() ?? '' : '';
        })(),
      };
    });

    if (payload?.itensPedidos && Object.keys(payload.itensPedidos).length > 0) {
      setItemsDetailsMap(payload.itensPedidos);
    }

    if (!isLightBootstrap) {
      allOrdersRef.current = allOData;
      allFinancialTitlesRef.current = allFData;
      allReceivableTitlesRef.current = allRData;
      setAllOrders(allOData);
      setAllFinancialTitles(allFData);
      setAllReceivableTitles(allRData);
    }
    setSaldoBancario(typeof payload?.saldoBancario === 'number' ? payload.saldoBancario : null);
    if (payload?.latestSync) {
      setSyncInfo(payload.latestSync);
      const syncDate = payload.latestSync.finished_at || payload.latestSync.started_at;
      if (syncDate) {
        const parsed = new Date(syncDate);
        setLastUpdate(Number.isNaN(parsed.getTime()) ? new Date() : parsed);
      } else {
        setLastUpdate(new Date());
      }
    } else {
      setLastUpdate(new Date());
    }
    setApiStatus('online');
    setDataRevision(r => r + 1);
  }, []);

  
  const fetchInitialData = useCallback(async () => {
    setLoading(true);
    setApiStatus('checking');

    const isConnected = await checkConnection();
    if (!isConnected) {
      setLoading(false);
      return;
    }

    try {
      const response = await api.get('/bootstrap');
      let payload = response.data;

      if (!bootstrapHasCoreData(payload)) {
        setSyncing(true);
        try {
          const syncResponse = await api.post('/sync');
          if (syncResponse.data?.in_progress) {
            const sharedPayload = await waitForSharedCache();
            if (sharedPayload) {
              payload = sharedPayload;
            }
          } else {
            const refreshed = await api.get('/bootstrap');
            payload = refreshed.data;
          }
        } catch (syncError) {
          console.error('Initial sync on empty Render cache failed:', syncError);
        } finally {
          setSyncing(false);
        }
      }

      applyBootstrapData(payload);
    } catch (error) {
      console.error('Error fetching initial data:', error);
      setApiStatus('offline');
      setBuildings([]);
      setUsers([]);
      setCreditors([]);
      setOrders([]);
      setFinancialTitles([]);
      setReceivableTitles([]);
    } finally {
      setLoading(false);
    }
  }, [applyBootstrapData, bootstrapHasCoreData, checkConnection, waitForSharedCache]);



  const availableTabs = useMemo(() => (
    isRestrictedUser
      ? [{ id: 'logistics', label: 'Logística', icon: Truck }]
      : [
          { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
          { id: 'finance', label: 'Financeiro', icon: DollarSign },
          { id: 'alerts', label: 'Alertas', icon: Bell },
          { id: 'map', label: 'Mapa de Obras', icon: MapIcon },
          { id: 'logistics', label: 'Logística', icon: Truck },
          { id: 'access', label: 'Acessos', icon: UserIcon },
        ]
  ), [isRestrictedUser]);

  useEffect(() => {
    if (activeTab === 'alerts' && orders.length > 0) {
      const visibleIds = orders.slice(0, 50).map(o => o.id);
      const missingIds = visibleIds.filter(id => !itemsDetailsMap[id] && !requestedItemsRef.current.has(String(id)));
      if (missingIds.length > 0) {
        missingIds.forEach(id => requestedItemsRef.current.add(String(id)));
        api.post('/fetch-items', { ids: missingIds })
          .then(res => {
            if (res.data) {
              setItemsDetailsMap(prev => ({...prev, ...res.data}));
            }
          })
          .catch(console.error);
      }
    }
  }, [activeTab, orders, itemsDetailsMap]);

  useEffect(() => {
    if (orders.length === 0) return;
    const historyMap: Record<string, any[]> = {};
    orders.forEach(o => {
      const items = itemsDetailsMap[o.id];
      if (!items) return;
      items.forEach((it: any) => {
        const desc = it.resourceDescription || it.descricao;
        if (!desc) return;
        const price = Number(it.netPrice || it.unitPrice || it.valorUnitario || 0);
        if (isNaN(price) || price <= 0) return;
        if (!historyMap[desc]) historyMap[desc] = [];
        historyMap[desc].push({ date: o.date, price: price });
      });
    });

    const pricesMap: Record<string, number> = {};
    const baseMap: Record<string, number> = {};

    const toTime = (value: any): number => {
      const raw = String(value || '').trim();
      if (!raw || raw === '---') return 0;
      try {
        const d = parseISO(raw);
        const t = d.getTime();
        return Number.isNaN(t) ? 0 : t;
      } catch {
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          const t2 = d2.getTime();
          return Number.isNaN(t2) ? 0 : t2;
        }
        const parsed = Date.parse(raw);
        return Number.isNaN(parsed) ? 0 : parsed;
      }
    };

    Object.keys(historyMap).forEach(desc => {
      const purchases = historyMap[desc].sort((a, b) => toTime(a.date) - toTime(b.date));
      pricesMap[desc] = purchases[purchases.length - 1].price;
      baseMap[desc] = purchases[0].price;
    });
    setLatestPricesMap(pricesMap);
    setBaselinePricesMap(baseMap);
  }, [orders, itemsDetailsMap]);

  const syncSienge = async () => {
    setSyncing(true);
    setApiStatus('checking');
    setSyncProgressSafe(1);
    try {
      // Fase 1: refresh rápido (bootstrap). O refreshData já faz /sync só se o cache estiver vazio.
      startLinearProgress(1, 80, 45_000);
      const refreshed = await refreshData();
      if (!refreshed) {
        clearSyncProgressTimer();
        setSyncProgressSafe(0);
        return;
      }

      // Fase 2: carregar dados filtrados para a UI (janela padrão de 12 meses)
      startLinearProgress(80, 95, 25_000);
      try {
        const effectiveStart = hasManualDateFilter
          ? (startDate || null)
          : defaultWindow.start;
        const effectiveEnd = hasManualDateFilter
          ? (endDate || startDate || null)
          : defaultWindow.end;

        const params: any = {
          company_id: selectedCompany,
          building_id: fcSelectedBuilding,
          user_id: selectedUser,
          requester_id: selectedRequester,
        };
        if (effectiveStart) params.start_date = format(effectiveStart, 'yyyy-MM-dd');
        if (effectiveEnd) params.end_date = format(effectiveEnd, 'yyyy-MM-dd');

        const filteredResp = await api.get('/filtered', { params });
        applyServerFilteredData(filteredResp.data);
      } catch {
        // mantém fallback já existente no effect — não quebra o fluxo do botão
      }

      clearSyncProgressTimer();
      setSyncProgressSafe(100);
      setApiStatus('online');
    } catch (e) {
      console.error('Sync error:', e);
      setApiStatus('offline');
      clearSyncProgressTimer();
      setSyncProgressSafe(0);
    } finally {
      setSyncing(false);
      window.setTimeout(() => setSyncProgressSafe(0), 450);
    }
  };

  const refreshData = async (): Promise<boolean> => {
    setLoading(true);
    const isConnected = await checkConnection();
    if (!isConnected) {
      setLoading(false);
      return false;
    }

    try {
      const response = await api.get('/bootstrap');
      let payload = response.data;

      if (!bootstrapHasCoreData(payload)) {
        setSyncing(true);
        try {
          const syncResponse = await api.post('/sync');
          if (syncResponse.data?.in_progress) {
            const sharedPayload = await waitForSharedCache();
            if (sharedPayload) {
              payload = sharedPayload;
            }
          } else {
            const refreshed = await api.get('/bootstrap');
            payload = refreshed.data;
          }
        } catch (syncError) {
          console.error('Refresh sync on empty Render cache failed:', syncError);
        } finally {
          setSyncing(false);
        }
      }

      setDataRevision(0);
      applyBootstrapData(payload);
      return true;
    } catch (error) {
      console.error('Error refreshing data:', error);
      setOrders([]);
      setFinancialTitles([]);
      setReceivableTitles([]);
      return false;
    } finally {
      setLoading(false);
    }
  };

  const applyFilters = useCallback(() => {
    // Forca a reexecucao do filtro server-side/local com os parametros atuais.
    setDataRevision((r) => r + 1);
  }, []);

  // Memoized lookup maps - rebuilt whenever master lists change
  const buildingMap = useMemo(() => {
    const m: Record<string, string> = {};
    buildings.forEach(b => { m[String(b.id)] = b.name; if (b.code) m[String(b.code)] = b.name; });
    return m;
  }, [buildings]);

  const kanbanSprintsForView = useMemo(() => {
    if (selectedCompany === 'all') return kanbanOverview;
    const companyBuildingIds = new Set(
      buildings.filter((b) => String(b.companyId) === selectedCompany).map((b) => b.id)
    );
    return kanbanOverview.filter((s) => companyBuildingIds.has(s.buildingId));
  }, [buildings, kanbanOverview, selectedCompany]);

  const kanbanSummaryForView = useMemo(() => {
    return {
      totalSprints: kanbanSprintsForView.length,
      overdueSprints: kanbanSprintsForView.filter((s) => s.overdue).length,
      overdueCards: kanbanSprintsForView.reduce((acc, s) => acc + (s.stats?.overdueCards || 0), 0),
    };
  }, [kanbanSprintsForView]);

  const creditorMap = useMemo(() => {
    const m: Record<string, string> = {};
    creditors.forEach(c => { m[String(c.id)] = c.name; });
    return m;
  }, [creditors]);

  const userMap = useMemo(() => {
    const m: Record<string, string> = {};
    users.forEach(u => { m[String(u.id)] = u.name; });
    return m;
  }, [users]);

  const toStartOfDay = useCallback((value: Date) => (
    new Date(value.getFullYear(), value.getMonth(), value.getDate()).getTime()
  ), []);

  const defaultWindow = useMemo(() => {
    const today = new Date();
    const end = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    const twelveMonthsAgo = addMonths(end, -12);
    const start = new Date(twelveMonthsAgo.getFullYear(), twelveMonthsAgo.getMonth(), twelveMonthsAgo.getDate());
    return { start, end };
  }, []);

  const hasManualDateFilter = useMemo(() => Boolean(startDate || endDate), [endDate, startDate]);

  const dateRange = useMemo(() => {
    const effectiveStartDate = hasManualDateFilter
      ? (startDate || null)
      : defaultWindow.start;
    const start = effectiveStartDate ? toStartOfDay(effectiveStartDate) : null;
    const effectiveEndDate = hasManualDateFilter
      ? (endDate || startDate || null)
      : defaultWindow.end;
    const endExclusive = effectiveEndDate ? addDays(new Date(
      effectiveEndDate.getFullYear(),
      effectiveEndDate.getMonth(),
      effectiveEndDate.getDate()
    ), 1).getTime() : null;
    return { start, endExclusive };
  }, [defaultWindow.end, defaultWindow.start, endDate, hasManualDateFilter, startDate, toStartOfDay]);

  const matchesDateRange = useCallback((numericValue?: number) => {
    if (!dateRange.start && !dateRange.endExclusive) return true;
    if (!numericValue || numericValue === 0) return false;
    if (dateRange.start !== null && numericValue < dateRange.start) return false;
    if (dateRange.endExclusive !== null && numericValue >= dateRange.endExclusive) return false;
    return true;
  }, [dateRange]);

  const matchesCompanyFilter = useCallback((buildingId?: string | number, itemCompanyId?: string | number) => {
    if (selectedCompany === 'all') return true;
    // 1. Direct companyId match (most reliable — set by backend)
    if (itemCompanyId != null && String(itemCompanyId) !== 'undefined' && String(itemCompanyId) !== '') {
      return String(itemCompanyId) === selectedCompany;
    }
    // 2. Fallback: look up building by buildingId and check its companyId
    if (buildingId != null && String(buildingId) !== '0') {
      const building = buildings.find(
        (b) => String(b.id) === String(buildingId) || String(b.code) === String(buildingId)
      );
      if (building?.companyId != null) {
        return String(building.companyId) === selectedCompany;
      }
    }
    return false;
  }, [buildings, selectedCompany]);

  const buildingOptions = useMemo(() => {
    if (!dateRange.start && !dateRange.endExclusive) return buildings;

    // Bootstrap leve: all* vazios → retorna todas as obras para não mostrar "0 encontradas"
    if (allOrders.length === 0 && allFinancialTitles.length === 0 && allReceivableTitles.length === 0) {
      return buildings;
    }

    const activeIds = new Set<string>();
    allOrders.forEach(o => {
      if (matchesDateRange(o.dateNumeric)) activeIds.add(String(o.buildingId));
    });
    allFinancialTitles.forEach(f => {
      if (matchesDateRange(f.dueDateNumeric)) activeIds.add(String(f.buildingId));
    });
    allReceivableTitles.forEach(r => {
      if (matchesDateRange(r.dueDateNumeric)) activeIds.add(String(r.buildingId));
    });

    return buildings.filter(b => activeIds.has(String(b.id)) || Boolean(b.code && activeIds.has(String(b.code))));
  }, [allFinancialTitles, allOrders, allReceivableTitles, buildings, dateRange.endExclusive, dateRange.start, matchesDateRange]);

  const ordersForUserOptions = useMemo(() => {
    return allOrders.filter(o => {
      const inDate = matchesDateRange(o.dateNumeric);
      const inBuilding = matchesCompanyFilter(o.buildingId, (o as any).companyId);
      const inRequester = selectedRequester === 'all' || String(o.requesterId) === selectedRequester || o.requesterId === selectedRequester;
      return inDate && inBuilding && inRequester;
    });
  }, [allOrders, matchesCompanyFilter, matchesDateRange, selectedRequester]);

  const ordersForRequesterOptions = useMemo(() => {
    return allOrders.filter(o => {
      const inDate = matchesDateRange(o.dateNumeric);
      const inBuilding = matchesCompanyFilter(o.buildingId, (o as any).companyId);
      const inUser = selectedUser === 'all' || String(o.buyerId) === selectedUser;
      return inDate && inBuilding && inUser;
    });
  }, [allOrders, matchesCompanyFilter, matchesDateRange, selectedUser]);

  const availableUsers = useMemo(() => {
    const seen = new Map<string, User>();
    ordersForUserOptions.forEach((o) => {
      const id = String(o.buyerId || '');
      if (!id || id === '0' || id === 'undefined') return;
      seen.set(id, { id, name: userMap[id] || (o as any).nomeComprador || (o as any).buyerName || `Comprador ${id}` });
    });
    const scoped = Array.from(seen.values()).sort((a, b) => a.name.localeCompare(b.name));
    if (scoped.length > 0) return scoped;
    return users
      .map((u) => ({ id: String(u.id), name: u.name }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [ordersForUserOptions, userMap, users]);

  const availableRequesters = useMemo(() => {
    const seen = new Map<string, User>();
    ordersForRequesterOptions.forEach((o) => {
      const id = String(o.requesterId || (o as any).solicitante || '');
      if (!id || id === '0' || id === 'undefined') return;
      const requesterName = fixText(String((o as any).solicitante || (o as any).nomeSolicitante || userMap[id] || id)).replace(/^Comprador\\s+/i, '').trim();
      seen.set(id, { id, name: requesterName || id });
    });
    const scoped = Array.from(seen.values()).sort((a, b) => a.name.localeCompare(b.name));
    if (scoped.length > 0) return scoped;
    return requesters
      .map((r) => ({ id: String(r.id), name: r.name }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [ordersForRequesterOptions, requesters, userMap]);

  const selectedCompanyData = useMemo(
    () => companies.find((company: any) => String(company.id) === selectedCompany) || null,
    [companies, selectedCompany]
  );

  const selectedCompanyName = selectedCompanyData?.name || 'Todas as Empresas';

  const fcSelectedCompanyData = useMemo(
    () => companies.find((company: any) => String(company.id) === fcSelectedCompany) || null,
    [companies, fcSelectedCompany]
  );
  const fcSelectedCompanyName = fcSelectedCompanyData?.name || (fcSelectedCompany === 'all' ? 'Todas as Empresas' : `Empresa ${fcSelectedCompany}`);

  const fcBuildingOptions = useMemo(() => {
    if (fcSelectedCompany === 'all') return buildings;
    return buildings.filter((b) => String(b.companyId) === fcSelectedCompany);
  }, [buildings, fcSelectedCompany]);

  const resolveBuildingName = (o: any): string => {
    const id = String(o.buildingId || o.enterpriseId || o.idObra || '');
    return fixText(buildingMap[id] || o.nomeObra || o.enterpriseName || 'Obra sem nome');
  };

  const resolveCreditorName = (o: any): string => {
    const id = String(o.supplierId || o.creditorId || o.idCredor || o.codigoFornecedor || '');
    return fixText(creditorMap[id] || o.nomeFornecedor || o.supplierName || o.creditorName || 'Credor sem nome');
  };

  const resolveUserName = (id?: string, fallback?: string): string => {
    if (!id || id === '0' || id === 'undefined') return fallback || 'N/A';
    return userMap[String(id)] || fallback || String(id);
  };

  const applyServerFilteredData = useCallback((payload: any) => {
    const rawOrdersArray = Array.isArray(payload?.pedidos) ? payload.pedidos : [];
    const fDataRaw = Array.isArray(payload?.financeiro) ? payload.financeiro : [];
    const rDataRaw = Array.isArray(payload?.receber) ? payload.receber : [];

    const filteredOrdersData: PurchaseOrder[] = rawOrdersArray.map((o: any) => {
      const dStr = o.dataEmissao || o.data || o.date || '---';
      let dateNumeric = 0;
      try {
        const d = parseISO(String(dStr));
        dateNumeric = Number.isNaN(d.getTime()) ? 0 : d.getTime();
      } catch {
        const raw = String(dStr || '').trim();
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          dateNumeric = Number.isNaN(d2.getTime()) ? 0 : d2.getTime();
        } else {
          const parsed = Date.parse(raw);
          dateNumeric = Number.isNaN(parsed) ? 0 : parsed;
        }
      }

      return {
        id: o.id || o.numero || 0,
        buildingId: o.idObra || o.codigoVisivelObra || o.buildingId || 0,
        companyId: o.companyId != null ? String(o.companyId) : undefined,
        buyerId: o.idComprador ? String(o.idComprador) : (o.codigoComprador ? String(o.codigoComprador) : (o.buyerId ? String(o.buyerId) : '0')),
        date: dStr,
        dateNumeric,
        totalAmount: parseFloat(o.valorTotal || o.totalAmount) || 0,
        supplierId: o.codigoFornecedor || o.supplierId,
        status: o.situacao || o.status || 'N/A',
        paymentCondition: o.condicaoPagamento || o.paymentMethod || 'A Prazo',
        deliveryDate: o.dataEntrega || o.prazoEntrega || '---',
        internalNotes: o.internalNotes || o.observacao || '',
        createdBy: fixText(o.nomeComprador || o.createdBy || o.criadoPor || ''),
        requesterId: fixText(String(o.solicitante || o.requesterId || o.createdBy || '0')).replace(/^Comprador\s+/i, '').trim(),
      };
    }).sort((a, b) => (b.dateNumeric || 0) - (a.dateNumeric || 0));

    const filteredFinancialData = fDataRaw.map((f: any) => {
      const dStr = f.dataVencimento || f.issueDate || f.dueDate || f.dataVencimentoProjetado || f.dataEmissao || f.dataContabil || '---';
      let dueDateNumeric = 0;
      try {
        const d = parseISO(String(dStr));
        dueDateNumeric = Number.isNaN(d.getTime()) ? 0 : d.getTime();
      } catch {
        const raw = String(dStr || '').trim();
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          dueDateNumeric = Number.isNaN(d2.getTime()) ? 0 : d2.getTime();
        } else {
          const parsed = Date.parse(raw);
          dueDateNumeric = Number.isNaN(parsed) ? 0 : parsed;
        }
      }

      return {
        id: f.id || f.numero || f.codigoTitulo || f.documentNumber || 0,
        buildingId: f.idObra || f.codigoObra || f.enterpriseId || f.buildingId || 0,
        buildingCode: f.codigoObra || f.idObra || f.enterpriseId || f.buildingId || '',
        buildingName: fixText(f.nomeObra || f.buildingName || f.enterpriseName || ''),
        description: fixText(f.descricao || f.historico || f.tipoDocumento || f.notes || f.observacao || 'Título a Pagar'),
        creditorName: fixText(f.nomeCredor || f.creditorName || f.nomeFantasiaCredor || f.fornecedor || f.credor || 'Credor sem nome'),
        _rawCreditorId: String(f.creditorId || f.debtorId || ''),
        companyId: (() => {
          if (f.companyId != null) return String(f.companyId);
          if (Array.isArray(f.links)) {
            const cLink = f.links.find((l: any) => l.rel === 'company');
            if (cLink && cLink.href) return cLink.href.split('/').pop();
          }
          return undefined;
        })(),
        dueDate: dStr,
        dueDateNumeric,
        amount: parseFloat(f.totalInvoiceAmount || f.valor || f.amount || f.valorTotal || f.valorLiquido || f.valorBruto) || 0,
        status: f.situacao || f.status || 'Pendente',
        documentNumber: String(f.documentNumber || f.numeroDocumento || f.numero || f.codigoTitulo || ''),
      };
    });

    const filteredReceivableData = rDataRaw.map((r: any) => {
      const dStr = r.dataVencimento || r.data || r.date || r.dataEmissao || r.issueDate || r?.dataVencimentoProjetado || '---';
      let dueDateNumeric = 0;
      try {
        const d = parseISO(String(dStr));
        dueDateNumeric = Number.isNaN(d.getTime()) ? 0 : d.getTime();
      } catch {
        const raw = String(dStr || '').trim();
        const m = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})/);
        if (m) {
          const d2 = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
          dueDateNumeric = Number.isNaN(d2.getTime()) ? 0 : d2.getTime();
        } else {
          const parsed = Date.parse(raw);
          dueDateNumeric = Number.isNaN(parsed) ? 0 : parsed;
        }
      }
      const rawValue: number = r.rawValue ?? (parseFloat(r.valor ?? r.value ?? r.valorSaldo ?? r.totalInvoiceAmount ?? r.valorTotal ?? r.amount ?? 0) || 0);
      return {
        id: r.id || r.numero || r.numeroTitulo || r.codigoTitulo || r.documentNumber || 0,
        buildingId: r.idObra || r.codigoObra || r.buildingId || 0,
        buildingCode: r.codigoObra || r.idObra || r.buildingId || '',
        buildingName: fixText(r.nomeObra || r.buildingName || r.enterpriseName || ''),
        companyId: (() => {
          if (r.companyId != null) return String(r.companyId);
          if (Array.isArray(r.links)) {
            const cLink = r.links.find((l: any) => l.rel === 'company');
            if (cLink && cLink.href) return cLink.href.split('/').pop();
          }
          return undefined;
        })(),
        description: fixText(r.descricao || r.historico || r.observacao || r.notes || r.description || 'Título a Receber'),
        clientName: fixText(r.nomeCliente || r.nomeFantasiaCliente || r.cliente || r.clientName || 'Extrato/Cliente'),
        dueDate: dStr,
        dueDateNumeric,
        amount: Math.abs(rawValue),
        rawValue,
        status: String(r.situacao || r.status || 'ABERTO').toUpperCase(),
        type: r.type || 'Income',
        statementType: r.statementType || '',
        statementOrigin: r.statementOrigin || '',
        documentId: r.documentId || '',
        documentNumber: r.documentNumber || '',
        installmentNumber: r.installmentNumber ?? null,
        billId: r.billId ?? null,
        // bankAccountCode extraído do links[rel=bank-account] quando não vem como campo direto
        bankAccountCode: r.bankAccountCode || (() => {
          if (!Array.isArray(r.links)) return '';
          const baLink = r.links.find((l: any) => l?.rel === 'bank-account');
          return baLink?.href ? baLink.href.trim().replace(/\/$/, '').split('/').pop() ?? '' : '';
        })(),
      };
    });

    setOrders(filteredOrdersData);
    setFinancialTitles(filteredFinancialData);
    setReceivableTitles(filteredReceivableData);
    // Mantem all* com o maior conjunto ja carregado (evita travar em janelas antigas).
    if (allOrdersRef.current.length === 0 || filteredOrdersData.length > allOrdersRef.current.length) {
      allOrdersRef.current = filteredOrdersData;
      setAllOrders(filteredOrdersData);
    }
    if (allFinancialTitlesRef.current.length === 0 || filteredFinancialData.length > allFinancialTitlesRef.current.length) {
      allFinancialTitlesRef.current = filteredFinancialData;
      setAllFinancialTitles(filteredFinancialData);
    }
    if (allReceivableTitlesRef.current.length === 0 || filteredReceivableData.length > allReceivableTitlesRef.current.length) {
      allReceivableTitlesRef.current = filteredReceivableData;
      setAllReceivableTitles(filteredReceivableData);
    }
  }, []);

  // Re-resolve creditor names whenever the creditor list loads (fixes the race with refreshData)
  useEffect(() => {
    if (Object.keys(creditorMap).length === 0) return;
    const resolve = (list: any[]) => list.map(f => {
      if (!f._rawCreditorId) return f;
      const resolved = creditorMap[f._rawCreditorId];
      return resolved && f.creditorName !== resolved ? { ...f, creditorName: resolved } : f;
    });
    setAllFinancialTitles(prev => resolve(prev));
    setFinancialTitles(prev => resolve(prev));
  }, [creditorMap]);

  useEffect(() => {
    if (startDate && endDate && endDate < startDate) {
      setEndDate(startDate);
    }
  }, [endDate, startDate]);

  useEffect(() => {
    if (selectedCompany !== 'all' && !companies.some((c) => String(c.id) === selectedCompany)) {
      setSelectedCompany('all');
    }
  }, [companies, selectedCompany]);

  useEffect(() => {
    if (selectedUser !== 'all' && !availableUsers.some((u) => String(u.id) === selectedUser)) {
      setSelectedUser('all');
    }
  }, [availableUsers, selectedUser]);

  useEffect(() => {
    if (selectedRequester !== 'all' && !availableRequesters.some((r) => String(r.id) === selectedRequester)) {
      setSelectedRequester('all');
    }
  }, [availableRequesters, selectedRequester]);

  useEffect(() => {
    if (selectedMapBuilding !== null && !buildingOptions.some((b) => b.id === selectedMapBuilding)) {
      setSelectedMapBuilding(null);
    }
  }, [buildingOptions, selectedMapBuilding]);

  useEffect(() => {
    if (!sessionUser || dataRevision === 0) return;

    let cancelled = false;

    const runServerSideFiltering = async () => {
      const effectiveStart = hasManualDateFilter
        ? (startDate || null)
        : defaultWindow.start;
      const effectiveEnd = hasManualDateFilter
        ? (endDate || startDate || null)
        : defaultWindow.end;

      // O endpoint de NF-e exige startDate/endDate. Mesmo que o modo global seja
      // “total”, usamos a janela padrão (últimos 12 meses) quando o usuário não
      // escolheu datas manualmente.
      const nfeStart = effectiveStart || defaultWindow.start;
      const nfeEnd = effectiveEnd || defaultWindow.end;

      try {
        const params: any = {
          company_id: selectedCompany,
          building_id: fcSelectedBuilding,
          user_id: selectedUser,
          requester_id: selectedRequester,
        };
        if (effectiveStart) params.start_date = format(effectiveStart, 'yyyy-MM-dd');
        if (effectiveEnd) params.end_date = format(effectiveEnd, 'yyyy-MM-dd');

        const response = await api.get('/filtered', { params });
        if (!cancelled) {
          applyServerFilteredData(response.data);
        }
      } catch (error) {
        if (cancelled) return;
        // Fallback local se endpoint /filtered estiver indisponível.
        // Usa refs (não estado) para não re-disparar o effect → evita loop.

        const buildingPasses = (value: any): boolean => {
          if (fcSelectedBuilding === 'all') return true;
          const current = String(value?.buildingId ?? value?.idObra ?? value?.codigoObra ?? value?.buildingCode ?? '');
          if (!current) return false;
          if (current === fcSelectedBuilding) return true;
          const selected = buildings.find((b: any) => String(b.id) === fcSelectedBuilding);
          if (!selected) return false;
          return String(selected.code ?? '') === current;
        };

        const filteredOrders = allOrdersRef.current.filter((o) => {
          const inDate = matchesDateRange(o.dateNumeric);
          const inBuilding = matchesCompanyFilter(o.buildingId, (o as any).companyId);
          const inSelectedBuilding = buildingPasses(o);
          const inUser = selectedUser === 'all' || String(o.buyerId) === selectedUser;
          const inRequester = selectedRequester === 'all' || String(o.requesterId) === selectedRequester || o.requesterId === selectedRequester;
          return inDate && inBuilding && inSelectedBuilding && inUser && inRequester;
        }).sort((a, b) => (b.dateNumeric || 0) - (a.dateNumeric || 0));

        const filteredFinancial = allFinancialTitlesRef.current.filter((f) => {
          const inDate = matchesDateRange(f.dueDateNumeric);
          const inBuilding = matchesCompanyFilter(f.buildingId, f.companyId);
          return inDate && inBuilding && buildingPasses(f);
        });

        const filteredReceivable = allReceivableTitlesRef.current.filter((r) => {
          const inDate = matchesDateRange(r.dueDateNumeric);
          const inBuilding = matchesCompanyFilter(r.buildingId, r.companyId);
          return inDate && inBuilding && buildingPasses(r);
        });

        setOrders(filteredOrders);
        setFinancialTitles(filteredFinancial);
        setReceivableTitles(filteredReceivable);
      }

      // NF-e (Receita Operacional): busca independente do /filtered.
      // - Filtra por empresa quando selecionada.
      // - Quando apenas a obra é selecionada, tenta usar companyId da obra.
      try {
        const normalizeResults = (payload: any): any[] => {
          if (!payload) return [];
          if (Array.isArray(payload)) return payload;
          if (Array.isArray(payload?.results)) return payload.results;
          if (Array.isArray(payload?.data?.results)) return payload.data.results;
          if (Array.isArray(payload?.data)) return payload.data;
          return [];
        };

        const selectedCompanyId = selectedCompany !== 'all' ? parseInt(selectedCompany, 10) : null;
        const selectedBuildingCompanyId = (() => {
          if (fcSelectedBuilding === 'all') return null;
          const b = buildings.find((x: any) => String(x.id) === fcSelectedBuilding);
          const cid = b?.companyId ?? b?.company_id;
          return cid ? parseInt(String(cid), 10) : null;
        })();

        const companyId = selectedCompanyId ?? selectedBuildingCompanyId;

        const nfeParams: any = {
          startDate: format(nfeStart, 'yyyy-MM-dd'),
          endDate: format(nfeEnd, 'yyyy-MM-dd'),
          limit: 200,
          offset: 0,
        };
        if (companyId && !isNaN(companyId)) nfeParams.companyId = companyId;

        const nfeResponse = await api.get('/nfe/documents', { params: nfeParams });
        const results = normalizeResults(nfeResponse.data);
        if (!cancelled) setNfeDocuments(results);
      } catch {
        if (!cancelled) setNfeDocuments([]);
      }
    };

    runServerSideFiltering();

    return () => {
      cancelled = true;
    };
  }, [
    activeTab,
    applyServerFilteredData,
    dataRevision,
    defaultWindow.end,
    defaultWindow.start,
    buildings,
    endDate,
    fcSelectedBuilding,
    globalPeriodMode,
    hasManualDateFilter,
    matchesCompanyFilter,
    matchesDateRange,
    selectedCompany,
    selectedRequester,
    selectedUser,
    sessionUser,
    startDate,
  ]);

  useEffect(() => {
    const visibleIds = orders.slice(0, 15).map((o) => o.id);
    const missingIds = visibleIds.filter((id) => !itemsDetailsMap[id] && !requestedItemsRef.current.has(String(id)));
    if (missingIds.length === 0) return;

    missingIds.forEach((id) => requestedItemsRef.current.add(String(id)));
    api.post('/fetch-items', { ids: missingIds })
      .then((res) => {
        if (res.data) {
          setItemsDetailsMap((prev) => ({ ...prev, ...res.data }));
        }
      })
      .catch((error) => {
        console.error('Error fetching filtered items:', error);
      });
  }, [orders, itemsDetailsMap]);

  // Fetch quotations (multiple suppliers per order) for price comparison
  useEffect(() => {
    const visibleIds = orders.slice(0, 20).map((o) => o.id);
    const missingIds = visibleIds.filter((id) => !quotationsMap[id] && !requestedQuotesRef.current.has(String(id)));
    if (missingIds.length === 0) return;
    missingIds.forEach((id) => requestedQuotesRef.current.add(String(id)));
    api.post('/fetch-quotations', { ids: missingIds })
      .then((res) => {
        if (res.data && Object.keys(res.data).length > 0) {
          setQuotationsMap((prev) => ({ ...prev, ...res.data }));
        }
      })
      .catch(() => { /* silent - endpoint may not exist on this Sienge plan */ });
  }, [orders, quotationsMap]);



  useEffect(() => {
    const handleAfterPrint = () => setIsPrinting(false);
    window.addEventListener('afterprint', handleAfterPrint);
    return () => {
      window.removeEventListener('afterprint', handleAfterPrint);
    };
  }, []);

  useEffect(() => {
    if (!sessionUser) return;

    if (sessionUser.role === 'user') {
      setActiveTab('logistics');
    }

    fetchInitialData();
    const syncInterval = setInterval(() => {
      fetchInitialData();
    }, 20 * 60 * 1000);

    return () => {
      clearInterval(syncInterval);
    };
  }, [fetchInitialData, sessionUser]);

  const loadKanbanOverview = useCallback(async () => {
    setKanbanOverviewLoading(true);
    try {
      const response = await kanbanApi.get('/overview');
      setKanbanOverview(Array.isArray(response.data?.sprints) ? response.data.sprints : []);
    } catch {
      setKanbanOverview([]);
    } finally {
      setKanbanOverviewLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!sessionUser || activeTab !== 'obras-alerta') return;
    loadKanbanOverview();
  }, [activeTab, loadKanbanOverview, sessionUser]);

  const handlePrint = () => {
    setIsPrinting(true);
    setTimeout(() => {
      window.print();
    }, 500); // 500ms window para renderizar 100% dos dados na VDOM
  };

  const pagamentosHoje = useMemo(() => {
    const isToday = (dStr: string) => {
       if (!dStr || dStr === '---') return false;
       try {
         const t = parseISO(dStr);
         if (isNaN(t.getTime())) return false;
         const hoje = new Date();
         return t.getFullYear() === hoje.getFullYear() && t.getMonth() === hoje.getMonth() && t.getDate() === hoje.getDate();
       } catch { return false; }
    };
    return allFinancialTitles.filter(f => isSettledFinancialStatus(f.status) && isToday(f.dueDate));
  }, [allFinancialTitles, isSettledFinancialStatus]);

  const activeBuildingCount = useMemo(() => {
    const ids = new Set<string>();
    orders.forEach((o) => { if (o?.buildingId) ids.add(String(o.buildingId)); });
    financialTitles.forEach((f) => { if (f?.buildingId) ids.add(String(f.buildingId)); });
    receivableTitles.forEach((r) => { if (r?.buildingId) ids.add(String(r.buildingId)); });
    // If a company is selected and we have buildings list, count buildings belonging to that company
    if (selectedCompany !== 'all') {
      const companyBuildingIds = new Set(
        buildings.filter(b => String(b.companyId) === selectedCompany).map(b => String(b.id))
      );
      return Array.from(ids).filter(id => companyBuildingIds.has(id)).length || companyBuildingIds.size;
    }
    return ids.size || buildings.length;
  }, [buildings, financialTitles, orders, receivableTitles, selectedCompany]);

  // Analytics Calculations
  const stats = useMemo(() => {
    const ordersArray = Array.isArray(orders) ? orders : [];
    const total = ordersArray.reduce((acc, curr) => acc + toMoney(curr.totalAmount), 0);
    const avg = ordersArray.length > 0 ? total / ordersArray.length : 0;
    
    const fTotal = financialTitles.reduce((acc, curr) => acc + toMoney(curr.amount), 0);
    const rTotal = receivableTitles.reduce((acc, curr) => acc + toMoney(curr.amount), 0);
    const balance = rTotal - fTotal;

    return { total, avg, fTotal, rTotal, balance };
  }, [orders, financialTitles, receivableTitles]);

  // Lógica para Projetar o DRE (Demonstrativo de Resultado) com base no total financeiro
  const dreStats = useMemo(() => {
    // Usamos rTotal como Receita Operacional Líquida (ROL) e projetamos a Receita Bruta
    // Baseado no PDF de Mar/2026: ROL é 88.36% da Receita Bruta.
    const rol = stats.rTotal;
    const receitaBruta = rol / 0.8836;
    const deducoes = receitaBruta - rol;

    // Distribuição dos custos e despesas baseada na proporção histórica do Sienge
    const despesasTotais = stats.fTotal;
    
    // Proporções extraídas da análise do PDF:
    const maoDeObra = despesasTotais * 0.215;
    const materiais = despesasTotais * 0.557;
    const servicos = despesasTotais * 0.105;
    const cspTotal = maoDeObra + materiais + servicos; // ~87.7%
    
    const despGerais = despesasTotais * 0.051;
    const despTributarias = despesasTotais * 0.003;
    const preLabore = despesasTotais * 0.046;
    const despOperacionaisTotal = despGerais + despTributarias + preLabore; // ~10.0%
    
    const despFinanceiras = despesasTotais * 0.022;
    const irCsll = despesasTotais * 0.001; // ~2.3%

    const resultadoBruto = rol - cspTotal;
    const resultadoOperacional = resultadoBruto - despOperacionaisTotal - despFinanceiras;
    const resultadoLiquido = resultadoOperacional - irCsll;

    return {
      receitaBruta,
      deducoes,
      rol,
      custos: {
        maoDeObra,
        materiais,
        servicos,
        total: cspTotal
      },
      resultadoBruto,
      despesas: {
        gerais: despGerais,
        tributarias: despTributarias,
        preLabore,
        total: despOperacionaisTotal
      },
      despFinanceiras,
      irCsll,
      resultadoLiquido
    };
  }, [stats]);

  const historicalStats = useMemo(() => {
    const historicalOrders = (Array.isArray(allOrders) ? allOrders : []).filter((o) =>
      selectedCompany === 'all' ? true : matchesCompanyFilter(o.buildingId, (o as any).companyId)
    );
    const historicalFinancial = (Array.isArray(allFinancialTitles) ? allFinancialTitles : []).filter((f) =>
      selectedCompany === 'all' ? true : matchesCompanyFilter(f.buildingId, f.companyId)
    );

    const totalPurchases = historicalOrders.reduce((acc, curr) => acc + toMoney(curr.totalAmount), 0);
    const totalPaid = historicalFinancial
      .filter((title) => isSettledFinancialStatus(title.status))
      .reduce((acc, curr) => acc + toMoney(curr.amount), 0);

    return { totalPurchases, totalPaid };
  }, [allFinancialTitles, allOrders, isSettledFinancialStatus, matchesCompanyFilter, selectedCompany]);

  // ──────────────────────────────────────────────────────────────────────────
  // FLUXO DE CAIXA — lógica isolada em src/tabs/fluxoCaixa/logic.ts
  // Não misturar com lógica de outras abas. Para corrigir cálculos, edite
  // apenas esse arquivo.
  // ──────────────────────────────────────────────────────────────────────────
  // Retorna { rows, saldoAnterior } — ver src/tabs/fluxoCaixa/logic.ts
  const { rows: fluxoDeCaixaData, saldoAnterior: fluxoDeCaixaSaldoAnterior } = useMemo(() => {
    const fcHasManualDate = Boolean(fcStartDate || fcEndDate);
    const fcEffectiveStart = fcHasManualDate
      ? (fcStartDate || null)
      : (fcPeriodMode === 'last6m' ? defaultWindow.start : null);
    const fcEffectiveEnd = fcHasManualDate
      ? (fcEndDate || fcStartDate || null)
      : (fcPeriodMode === 'last6m' ? defaultWindow.end : null);

    // Delega toda a lógica para o módulo isolado src/tabs/fluxoCaixa/logic.ts
    return calcularFluxoCaixa({
      allReceivableTitles,
      allFinancialTitles,
      buildings,
      fcSelectedCompany,
      fcSelectedBuilding,
      fcHideInternal,
      startNumeric: fcEffectiveStart ? parseInt(format(fcEffectiveStart, 'yyyyMMdd')) : null,
      endNumeric: fcEffectiveEnd ? parseInt(format(fcEffectiveEnd, 'yyyyMMdd')) : null,
    });
  }, [allFinancialTitles, allReceivableTitles, defaultWindow.end, defaultWindow.start, fcEndDate, fcHideInternal, fcPeriodMode, fcSelectedBuilding, fcSelectedCompany, fcStartDate, buildings]);


  const chartData = useMemo(() => {
    const months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
    const data = months.map(m => ({ name: m, valor: 0, financeiro: 0 }));
    const ordersArray = Array.isArray(orders) ? orders : [];
    
    ordersArray.forEach(order => {
      if (order && order.date) {
        try {
          const d = parseISO(order.date);
          if (d && !isNaN(d.getTime())) {
            const month = d.getMonth();
            if (month >= 0 && month < 12) {
              data[month].valor += (order.totalAmount || 0);
            }
          }
        } catch {}
      }
    });

    financialTitles.forEach(title => {
      if (title && title.dueDate) {
        try {
          const d = parseISO(title.dueDate);
          if (d && !isNaN(d.getTime())) {
            const month = d.getMonth();
            if (month >= 0 && month < 12) {
              data[month].financeiro += (title.amount || 0);
            }
          }
        } catch {}
      }
    });

    return data;
  }, [orders, financialTitles]);

  const supplierData = useMemo(() => {
    const map: Record<string, { name: string; value: number }> = {};
    const ordersArray = Array.isArray(orders) ? orders : [];
    ordersArray.forEach(o => {
      const id = String(o.supplierId || o.creditorId || o.idCredor || '');
      if (!id || id === 'undefined') return;
      const name = creditorMap[id] || o.nomeFornecedor || o.supplierName || o.creditorName || 'Credor sem nome';
      if (!map[id]) map[id] = { name, value: 0 };
      map[id].value += (o.totalAmount || o.valorTotal || o.amount || 0);
    });

    return Object.values(map)
      .sort((a, b) => b.value - a.value)
      .slice(0, 5);
  }, [orders, creditorMap]);

  const paymentMethodData = useMemo(() => {
    const map: Record<string, number> = {};
    const ordersArray = Array.isArray(orders) ? orders : [];
    
    ordersArray.forEach(order => {
      const method = order.paymentConditionDescription || order.condicaoPagamentoDescricao || order.paymentCondition || 'Não Informado';
      map[method] = (map[method] || 0) + (order.totalAmount || order.valorTotal || order.amount || 0);
    });

    const result = Object.entries(map)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);
      
    return result.length > 0 ? result : [
      { name: 'Boleto', value: 45000 },
      { name: 'PIX', value: 30000 },
      { name: 'Cartão', value: 15000 },
      { name: 'Dinheiro', value: 10000 },
    ];
  }, [orders]);

  // --- NEW AGGREGATIONS FOR CHARTS ---
  const orderStatusData = useMemo(() => {
    const map: Record<string, number> = {};
    const ordersArray = Array.isArray(orders) ? orders : [];
    ordersArray.forEach(o => {
      const status = translateStatusLabel(o.status) || 'N/D';
      map[status] = (map[status] || 0) + 1;
    });
    return Object.entries(map).map(([name, value]) => ({ name, value })).sort((a,b) => b.value - a.value);
  }, [orders, translateStatusLabel]);

  const financeBalanceData = useMemo(() => {
    const paid = financialTitles.filter(f => isSettledFinancialStatus(f.status)).reduce((acc, curr) => acc + toMoney(curr.amount), 0);
    const open = financialTitles.filter(f => !isSettledFinancialStatus(f.status)).reduce((acc, curr) => acc + toMoney(curr.amount), 0);
    return [
      { name: 'Pago', value: paid },
      { name: 'A Pagar', value: open }
    ];
  }, [financialTitles, isSettledFinancialStatus]);

  const buildingCostData = useMemo(() => {
    const map: Record<string, { name: string, gasto: number, receita: number }> = {};
    const bMap: Record<string, string> = {};
    buildings.forEach(b => bMap[b.id] = b.name);

    financialTitles.forEach(f => {
      const bName = bMap[f.buildingId] || String(f.buildingId || 'Sem Obra');
      if (!map[bName]) map[bName] = { name: bName, gasto: 0, receita: 0 };
      map[bName].gasto += toMoney(f.amount);
    });

    receivableTitles.forEach(r => {
      const bName = bMap[r.buildingId] || String(r.buildingId || 'Sem Obra');
      if (!map[bName]) map[bName] = { name: bName, gasto: 0, receita: 0 };
      map[bName].receita += toMoney(r.amount);
    });

    return Object.values(map)
      .sort((a, b) => b.gasto - a.gasto)
      .slice(0, 7);
  }, [financialTitles, receivableTitles, buildings]);

  const cashFlowData = useMemo(() => {
     const months = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez'];
     const data = months.map(m => ({ name: m, despesa: 0, receita: 0 }));

     financialTitles.forEach(title => {
        if (!title.dueDate) return;
        try {
          const d = parseISO(title.dueDate);
          if (d && !isNaN(d.getTime())) {
            const m = d.getMonth();
            if (m >= 0 && m < 12) data[m].despesa += toMoney(title.amount);
          }
        } catch {}
     });

     receivableTitles.forEach(title => {
        if (!title.dueDate) return;
        try {
          const d = parseISO(title.dueDate);
          if (d && !isNaN(d.getTime())) {
            const m = d.getMonth();
            if (m >= 0 && m < 12) data[m].receita += toMoney(title.amount);
          }
        } catch {}
     });

     return data;
  }, [financialTitles, receivableTitles]);

  const downloadCSV = () => {
    const bMap: Record<string, string> = {};
    buildings.forEach(b => bMap[b.id] = b.name);
    const uMap: Record<string, string> = {};
    users.forEach(u => uMap[u.id] = u.name);

    const headers = "ID;Obra;Comprador;Data;Valor;Status\n";
    const rows = orders.map(o => {
      const obra = bMap[o.buildingId] || o.buildingId;
      const user = uMap[o.buyerId] || o.buyerId;
      const valorStr = String(o.totalAmount || 0).replace('.', ',');
      return `${o.id};"${obra}";"${user}";${safeFormat(o.date)};${valorStr};${translateStatusLabel(o.status)}`;
    }).join("\n");
    // Adiciona BOM (\uFEFF) para forçar o Excel a reconhecer UTF-8
    const blob = new Blob(["\uFEFF" + headers + rows], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.setAttribute("download", `dinamica_faturamento_${format(new Date(), 'yyyyMMdd')}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const downloadData = () => {
    const bMap: Record<string, string> = {};
    buildings.forEach(b => bMap[b.id] = b.name);
    const uMap: Record<string, string> = {};
    users.forEach(u => uMap[u.id] = u.name);

    const headers = "Tipo;ID;Obra;Comprador/Credor/Cliente;Data;Valor;Status\n";
    const csvRows: string[] = [];

    orders.forEach(o => {
      const obra = bMap[o.buildingId] || String(o.buildingId);
      const user = uMap[o.buyerId] || String(o.buyerId);
      const valorStr = String(o.totalAmount || 0).replace('.', ',');
      csvRows.push(`Pedido;${o.id};"${obra}";"${user}";${safeFormat(o.date)};${valorStr};${translateStatusLabel(o.status)}`);
    });

    financialTitles.forEach(f => {
      const obra = bMap[f.buildingId] || String(f.buildingId);
      const credor = f.creditorName || "S/N";
      const valorStr = String(f.amount || 0).replace('.', ',');
      csvRows.push(`A Pagar;${f.id};"${obra}";"${credor}";${safeFormat(f.dueDate)};${valorStr};${translateStatusLabel(f.status)}`);
    });

    receivableTitles.forEach(r => {
      const obra = bMap[r.buildingId] || String(r.buildingId);
      const cliente = r.clientName || "S/N";
      const valorStr = String(r.amount || 0).replace('.', ',');
      csvRows.push(`A Receber;${r.id};"${obra}";"${cliente}";${safeFormat(r.dueDate)};${valorStr};${translateStatusLabel(r.status)}`);
    });

    const blob = new Blob(["\uFEFF" + headers + csvRows.join("\n")], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.setAttribute("download", `dinamica_relatorio_filtrado_${format(new Date(), 'yyyyMMdd')}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };


  const contextValue = {
    loading, syncing, syncProgress, dataRevision, apiStatus, saldoBancario, lastUpdate, syncInfo,
    startDate, setStartDate, endDate, setEndDate,
    fcStartDate, setFcStartDate, fcEndDate, setFcEndDate, fcPeriodMode, setFcPeriodMode,
    fcSelectedCompany, setFcSelectedCompany, fcSelectedBuilding, setFcSelectedBuilding, fcHideInternal, setFcHideInternal,
    isPrinting, setIsPrinting, newOrderAlert, setNewOrderAlert, selectedAlertOrder, setSelectedAlertOrder,
    modalItemHistory, setModalItemHistory, expandedDetail, setExpandedDetail,
    financeLimit, setFinanceLimit, kanbanOverview, kanbanOverviewLoading,
    reportType, setReportType, alertSortConfig, setAlertSortConfig, toggleSort, renderSortIcon,
    buildings, setBuildings, users, setUsers, requesters, setRequesters, creditors, setCreditors,
    companies, setCompanies, orders, setOrders, allOrders, setAllOrders, priceAlerts, setPriceAlerts,
    financialTitles, setFinancialTitles, allFinancialTitles, setAllFinancialTitles,
    receivableTitles, setReceivableTitles, allReceivableTitles, setAllReceivableTitles,
    nfeDocuments, setNfeDocuments,
    itemsDetailsMap, setItemsDetailsMap, quotationsMap, setQuotationsMap,
    latestPricesMap, setLatestPricesMap, baselinePricesMap, setBaselinePricesMap,
    globalItemHistory, selectedMapBuilding, setSelectedMapBuilding, buildingSearch, setBuildingSearch,
    editingEngineer, setEditingEngineer, engineerDraft, setEngineerDraft, savingEngineer, setSavingEngineer,
    selectedCompany, setSelectedCompany, selectedUser, setSelectedUser, selectedRequester, setSelectedRequester,
    globalPeriodMode, setGlobalPeriodMode,
    activeBuildingCount,
    applyFilters,
    syncSienge
  };

  return (
    <SiengeContext.Provider value={contextValue}>
      {children}
    </SiengeContext.Provider>
  );
}
