import { useState, useEffect, useMemo, useCallback, useRef, Fragment } from 'react';
import { 
  LayoutDashboard, Bell, Filter, Download, TrendingUp, TrendingDown, 
  DollarSign, Package, Calendar as CalendarIcon, RefreshCw, 
  User as UserIcon, Building2, ChevronRight, Search, Map as MapIcon,
  Wifi, WifiOff, CheckCircle2, AlertCircle, AlertTriangle, FileText, Printer, X,
  Menu, ChevronDown, SlidersHorizontal, Truck, LogOut, Moon, Sun
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { LogisticsTab } from './components/LogisticsTab';
import { AccessControlTab } from './components/AccessControl';
import { LoginScreen } from './components/LoginScreen';
import { NavigationMenu } from './components/NavigationMenu';
import { FluxoProjection } from './components/FluxoProjection';
import { LeandroTab } from './tabs/fluxoCaixa/Leandro';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button, buttonVariants } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { parseISO, format, addDays, addMonths } from 'date-fns';
import { ptBR } from 'date-fns/locale';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  AreaChart, Area, Cell, PieChart, Pie, LineChart, Line, Legend
} from 'recharts';
import { sienge as api, kanbanApi, Building, User, Creditor, PurchaseOrder, PriceAlert, type AuthUser } from './lib/api';
import { cn } from './lib/utils';
import { fixText } from './lib/text';
import { calcularFluxoCaixa } from './tabs/fluxoCaixa/logic';
import { safeFormat } from './tabs/dashboard/logic';
import {
  isSettledFinancialStatus,
  toMoney,
  translateStatementType,
  translateStatusLabel,
} from './tabs/financeiro/logic';
import logoWordmark from './assets/dinamica-wordmark.svg';
import logoWordmarkDark from './assets/dinamica-wordmark-dark.svg';

export default function App() {
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

  const [sessionUser, setSessionUser] = useState<AuthUser | null>(null);
  type ThemeMode = 'light' | 'dark';
  const [themeMode, setThemeMode] = useState<ThemeMode>(() => {
    const stored = localStorage.getItem('dinamica_theme');
    return stored === 'light' || stored === 'dark' ? stored : 'dark';
  });
  const [authReady, setAuthReady] = useState(false);
  const [activeTab, setActiveTab] = useState<string>('dashboard');
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
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
  // Refs para acesso ao all* no fallback do filtro (sem adicionar nos deps do useEffect → evita loop)
  const allOrdersRef = useRef<PurchaseOrder[]>([]);
  const allFinancialTitlesRef = useRef<any[]>([]);
  const allReceivableTitlesRef = useRef<any[]>([]);

  type ReportType = 'pagar' | 'receber' | 'abertos' | null;
  const [reportType, setReportType] = useState<ReportType>(null);
  
  const [alertSortConfig, setAlertSortConfig] = useState<{ key: 'date' | 'vlrUnit' | 'vlrAtual' | 'valorTotal', direction: 'asc' | 'desc' } | null>(null);
  const isRestrictedUser = sessionUser?.role === 'user';
  const isDark = themeMode === 'dark';

  const toggleThemeMode = useCallback(() => {
    setThemeMode((prev) => (prev === 'dark' ? 'light' : 'dark'));
  }, []);

  useEffect(() => {
    localStorage.setItem('dinamica_theme', themeMode);
    document.documentElement.classList.toggle('dark', themeMode === 'dark');
  }, [themeMode]);

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
  const [itemsDetailsMap, setItemsDetailsMap] = useState<Record<string, any>>({});
  const [quotationsMap, setQuotationsMap] = useState<Record<string, any[]>>({});
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

    const uniqueRequesters = new Map<string, User>();
    rawOrdersArray.forEach((o: any) => {
      const solName = fixText(String(o.solicitante || o.requesterId || o.createdBy || '')).replace(/^Comprador\s+/i, '').trim();
      if (solName) {
        uniqueRequesters.set(solName, { id: solName, name: solName });
      }
    });
    setRequesters(Array.from(uniqueRequesters.values()));

    const allOData: PurchaseOrder[] = rawOrdersArray.map((o: any) => {
      const dStr = o.dataEmissao || o.data || o.date || '---';
      const d = parseISO(dStr);
      return {
        id: o.id || o.numero || 0,
        buildingId: o.idObra || o.codigoVisivelObra || o.buildingId || 0,
        companyId: o.companyId != null ? String(o.companyId) : undefined,
        buyerId: o.idComprador ? String(o.idComprador) : (o.codigoComprador ? String(o.codigoComprador) : (o.buyerId ? String(o.buyerId) : '0')),
        date: dStr,
        dateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
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

    if (knownOrderIdsRef.current.size > 0) {
      const newOrders = allOData.filter(o => !knownOrderIdsRef.current.has(o.id));
      if (newOrders.length > 0) {
        setNewOrderAlert(newOrders[0]);
      }
    }
    allOData.forEach(o => knownOrderIdsRef.current.add(o.id));

    const allFData = fDataRaw.map((f: any) => {
      const dStr = f.dataVencimento || f.issueDate || f.dueDate || f.dataVencimentoProjetado || f.dataEmissao || f.dataContabil || '---';
      const d = parseISO(dStr);
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
        dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
        amount: parseFloat(f.totalInvoiceAmount || f.valor || f.amount || f.valorTotal || f.valorLiquido || f.valorBruto) || 0,
        status: f.situacao || f.status || 'Pendente',
        documentNumber: String(f.documentNumber || f.numeroDocumento || f.numero || f.codigoTitulo || ''),
      };
    });

    const allRData = rDataRaw.map((r: any) => {
      const dStr = r.dataVencimento || r.data || r.date || r.dataEmissao || r.issueDate || r?.dataVencimentoProjetado || '---';
      const d = parseISO(dStr);
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
        dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
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

    setItemsDetailsMap(payload?.itensPedidos || {});
    allOrdersRef.current = allOData;
    allFinancialTitlesRef.current = allFData;
    allReceivableTitlesRef.current = allRData;
    setAllOrders(allOData);
    setAllFinancialTitles(allFData);
    setAllReceivableTitles(allRData);
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

  const handleLogin = useCallback((user: AuthUser) => {
    localStorage.setItem('dinamica_session', JSON.stringify(user));
    setSessionUser(user);
  }, []);

  const handleLogout = useCallback(() => {
    localStorage.removeItem('dinamica_session');
    setSessionUser(null);
  }, []);

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

    Object.keys(historyMap).forEach(desc => {
      const purchases = historyMap[desc].sort((a,b) => parseISO(a.date).getTime() - parseISO(b.date).getTime());
      pricesMap[desc] = purchases[purchases.length - 1].price;
      baseMap[desc] = purchases[0].price;
    });
    setLatestPricesMap(pricesMap);
    setBaselinePricesMap(baseMap);
  }, [orders, itemsDetailsMap]);

  const syncSienge = async () => {
    setSyncing(true);
    setApiStatus('checking');
    try {
      const response = await api.post('/sync');
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
      if (response.data?.in_progress) {
        const sharedPayload = await waitForSharedCache();
        if (sharedPayload) {
          setDataRevision(0);
          applyBootstrapData(sharedPayload);
        } else {
          await refreshData();
        }
      } else {
        await refreshData();
      }
      setApiStatus('online');
    } catch (e) {
      console.error('Sync error:', e);
      setApiStatus('offline');
    } finally {
      setSyncing(false);
    }
  };

  const refreshData = async () => {
    setLoading(true);
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
          console.error('Refresh sync on empty Render cache failed:', syncError);
        } finally {
          setSyncing(false);
        }
      }

      setDataRevision(0);
      applyBootstrapData(payload);
    } catch (error) {
      console.error('Error refreshing data:', error);
      setOrders([]);
      setFinancialTitles([]);
      setReceivableTitles([]);
    } finally {
      setLoading(false);
    }
  };

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
    const sixMonthsAgo = addMonths(end, -6);
    const start = new Date(sixMonthsAgo.getFullYear(), sixMonthsAgo.getMonth(), sixMonthsAgo.getDate());
    return { start, end };
  }, []);

  const hasManualDateFilter = useMemo(() => Boolean(startDate || endDate), [endDate, startDate]);

  const dateRange = useMemo(() => {
    const effectiveStartDate = hasManualDateFilter
      ? (startDate || null)
      : (globalPeriodMode === 'last6m' ? defaultWindow.start : null);
    const start = effectiveStartDate ? toStartOfDay(effectiveStartDate) : null;
    const effectiveEndDate = hasManualDateFilter
      ? (endDate || startDate || null)
      : (globalPeriodMode === 'last6m' ? defaultWindow.end : null);
    const endExclusive = effectiveEndDate ? addDays(new Date(
      effectiveEndDate.getFullYear(),
      effectiveEndDate.getMonth(),
      effectiveEndDate.getDate()
    ), 1).getTime() : null;
    return { start, endExclusive };
  }, [defaultWindow.end, defaultWindow.start, endDate, globalPeriodMode, hasManualDateFilter, startDate, toStartOfDay]);

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
      const d = parseISO(dStr);
      return {
        id: o.id || o.numero || 0,
        buildingId: o.idObra || o.codigoVisivelObra || o.buildingId || 0,
        companyId: o.companyId != null ? String(o.companyId) : undefined,
        buyerId: o.idComprador ? String(o.idComprador) : (o.codigoComprador ? String(o.codigoComprador) : (o.buyerId ? String(o.buyerId) : '0')),
        date: dStr,
        dateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
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
      const d = parseISO(dStr);
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
        dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
        amount: parseFloat(f.totalInvoiceAmount || f.valor || f.amount || f.valorTotal || f.valorLiquido || f.valorBruto) || 0,
        status: f.situacao || f.status || 'Pendente',
        documentNumber: String(f.documentNumber || f.numeroDocumento || f.numero || f.codigoTitulo || ''),
      };
    });

    const filteredReceivableData = rDataRaw.map((r: any) => {
      const dStr = r.dataVencimento || r.data || r.date || r.dataEmissao || r.issueDate || r?.dataVencimentoProjetado || '---';
      const d = parseISO(dStr);
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
        dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
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
      try {
        const isLeandroTab = activeTab === 'financeiro-leandro';
        const params: Record<string, string> = {
          company_id: selectedCompany,
          user_id: selectedUser,
          requester_id: selectedRequester,
        };
        const effectiveStart = isLeandroTab
          ? null
          : (hasManualDateFilter
              ? (startDate || null)
              : (globalPeriodMode === 'last6m' ? defaultWindow.start : null));
        const effectiveEnd = isLeandroTab
          ? null
          : (hasManualDateFilter
              ? (endDate || startDate || null)
              : (globalPeriodMode === 'last6m' ? defaultWindow.end : null));
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
        const filteredOrders = allOrdersRef.current.filter((o) => {
          const inDate = matchesDateRange(o.dateNumeric);
          const inBuilding = matchesCompanyFilter(o.buildingId, (o as any).companyId);
          const inUser = selectedUser === 'all' || String(o.buyerId) === selectedUser;
          const inRequester = selectedRequester === 'all' || String(o.requesterId) === selectedRequester || o.requesterId === selectedRequester;
          return inDate && inBuilding && inUser && inRequester;
        }).sort((a, b) => (b.dateNumeric || 0) - (a.dateNumeric || 0));

        const filteredFinancial = allFinancialTitlesRef.current.filter((f) => {
          const inDate = matchesDateRange(f.dueDateNumeric);
          const inBuilding = matchesCompanyFilter(f.buildingId, f.companyId);
          return inDate && inBuilding;
        });

        const filteredReceivable = allReceivableTitlesRef.current.filter((r) => {
          const inDate = matchesDateRange(r.dueDateNumeric);
          const inBuilding = matchesCompanyFilter(r.buildingId, r.companyId);
          return inDate && inBuilding;
        });

        setOrders(filteredOrders);
        setFinancialTitles(filteredFinancial);
        setReceivableTitles(filteredReceivable);
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
    endDate,
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
    try {
      const storedSession = localStorage.getItem('dinamica_session');
      setSessionUser(storedSession ? JSON.parse(storedSession) : null);
    } catch {
      setSessionUser(null);
    } finally {
      setAuthReady(true);
    }
  }, []);

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
  if (!authReady) {
    return <div className={cn("min-h-screen", isDark ? "bg-[#0F1115]" : "bg-[#F3F5F7]")} />;
  }

  if (!sessionUser) {
    return <LoginScreen onLogin={handleLogin} themeMode={themeMode} onToggleTheme={toggleThemeMode} />;
  }

  return (
    <>
    <div className={cn(
      "min-h-screen overflow-x-hidden font-sans selection:bg-emerald-500/20",
      isDark ? "bg-[#0F1115] text-slate-100" : "bg-[#F3F5F7] text-[#102A40]",
      reportType ? "print:hidden" : ""
    )}>
      {/* Header */}
      <header className={cn(
        "border-b backdrop-blur-xl sticky top-0 z-50 print:hidden shadow-sm",
        isDark ? "border-slate-800 bg-[#11141A]/95" : "border-slate-200 bg-white/95"
      )}>
        <div className="tablet-safe-wrap w-full max-w-[98%] 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 h-16 sm:h-20 flex items-center justify-between gap-3">
          {/* Logo */}
          <div className="flex items-center gap-3 shrink-0 min-w-0">
            <img
              src={isDark ? logoWordmarkDark : logoWordmark}
              alt="Dinâmica Empreendimentos"
              className={cn(
                "w-auto",
                isDark ? "h-10 sm:h-11" : "h-9 sm:h-11"
              )}
            />
            <div>
              <h1 className="hidden">Dinâmica</h1>
              <div className="flex items-center gap-2">
                <p className="hidden sm:block text-[10px] font-bold tracking-[0.2em] text-[#4CB232] uppercase">Dashboard Financeiro</p>
                {apiStatus === 'online' && (
                  <span className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-[9px] font-bold text-emerald-500 uppercase tracking-wider">
                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                    Live
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Desktop Nav */}
          <NavigationMenu activeTab={activeTab} setActiveTab={setActiveTab} isRestrictedUser={isRestrictedUser} />

          {/* Desktop Actions */}
          <div className="hidden xl:flex items-center gap-2 2xl:gap-3">
            <div className={cn(
              "flex flex-col rounded-xl border px-3 py-2 text-xs font-bold min-w-[128px] max-w-[150px]",
              isDark ? "border-slate-700 bg-slate-900 text-slate-200" : "border-slate-200 bg-slate-50 text-slate-700"
            )}>
              <div className="flex items-center gap-2">
                <UserIcon size={14} className="text-[#4CB232]" />
                <span className="truncate">{sessionUser.name}</span>
              </div>
              <Button
                onClick={handleLogout}
                variant="outline"
                className={cn(
                  "mt-2 h-9 font-bold rounded-lg px-3 gap-2",
                  isDark ? "border-slate-700 bg-slate-800 text-slate-100 hover:bg-slate-700" : "border-slate-200 bg-white text-slate-700 hover:bg-slate-100"
                )}
              >
                <LogOut size={14} />
                <span>Sair</span>
              </Button>
            </div>
            <Button
              onClick={toggleThemeMode}
              variant="outline"
              className={cn(
                "rounded-xl h-11 px-3 2xl:px-4 gap-2 font-bold",
                isDark
                  ? "border-slate-700 bg-slate-900 text-slate-100 hover:bg-slate-800"
                  : "border-slate-200 bg-white text-slate-700 hover:bg-slate-100"
              )}
              title={isDark ? "Ativar modo claro" : "Ativar modo escuro"}
            >
              {isDark ? <Sun size={16} /> : <Moon size={16} />}
              <span>{isDark ? 'Dia' : 'Noite'}</span>
            </Button>
            <Button 
              onClick={syncSienge}
              disabled={syncing}
              className={cn(
                "text-white font-bold rounded-xl h-11 px-3 2xl:px-4 gap-2 shrink-0 min-w-[176px]",
                isDark ? "bg-[#1B3C58] hover:bg-[#234b6e]" : "bg-[#102A40] hover:bg-[#173A57]"
              )}
            >
              <RefreshCw size={16} className={cn(syncing && "animate-spin")} />
              <span>{syncing ? "Atualizando..." : "Atualizar Dados"}</span>
            </Button>
            <Button 
              onClick={downloadData}
              variant="outline"
              className="bg-[#4CB232]/10 text-[#3A9928] border-[#4CB232]/30 hover:bg-[#4CB232] hover:text-white font-bold rounded-xl h-11 px-3 2xl:px-4 gap-2"
            >
              <Download size={16} />
              <span className="hidden 2xl:inline">Baixar Dados</span>
            </Button>
          </div>

          {/* Mobile Action Buttons */}
          <div className="flex xl:hidden items-center gap-2 ml-auto">
            <button
              onClick={syncSienge}
              disabled={syncing}
              className={cn(
                "w-9 h-9 flex items-center justify-center rounded-xl text-white",
                isDark ? "bg-[#1B3C58]" : "bg-[#102A40]"
              )}
            >
              <RefreshCw size={16} className={cn(syncing && "animate-spin")} />
            </button>
            <button
              onClick={toggleThemeMode}
              className={cn(
                "w-9 h-9 flex items-center justify-center rounded-xl",
                isDark ? "bg-slate-900 text-slate-100" : "bg-white text-slate-700 border border-slate-200"
              )}
              title={isDark ? "Ativar modo claro" : "Ativar modo escuro"}
            >
              {isDark ? <Sun size={15} /> : <Moon size={15} />}
            </button>
            <button
              onClick={downloadData}
              className="w-9 h-9 flex items-center justify-center rounded-xl bg-[#4CB232]/15 text-[#3A9928]"
            >
              <Download size={16} />
            </button>
            <button
              onClick={handleLogout}
              className={cn(
                "w-9 h-9 flex items-center justify-center rounded-xl",
                isDark ? "bg-slate-800 text-slate-200" : "bg-slate-200 text-slate-700"
              )}
            >
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </header>

      {/* Mobile Bottom Navigation */}
      <nav className={cn(
        "xl:hidden fixed bottom-0 left-0 right-0 z-50 backdrop-blur-xl border-t flex flex-wrap print:hidden",
        isDark ? "bg-[#11141A]/95 border-slate-800" : "bg-white/95 border-slate-200"
      )}>
        {availableTabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id as any)}
            className={cn(
              "flex-1 flex flex-col items-center justify-center py-2 gap-1 text-[10px] font-bold transition-all",
              activeTab === tab.id ? "text-[#4CB232]" : (isDark ? "text-slate-400" : "text-slate-500")
            )}
          >
            <tab.icon size={20} />
            <span>{tab.label}</span>
          </button>
        ))}
      </nav>

      <main className="w-full max-w-full 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 py-6 sm:py-10 pb-24 xl:pb-10">
        {/* Global Date Filter - Mobile Collapsible */}
        {activeTab !== 'logistics' && activeTab !== 'access' && activeTab !== 'obras-diario' && activeTab !== 'financeiro-fluxo' && activeTab !== 'financeiro-leandro' && (
          <div className="mb-6 sm:mb-10 bg-[#161618] rounded-2xl border border-white/5 shadow-xl print:hidden overflow-hidden">
            {/* Filter Header - Mobile Toggle */}
          <button
            onClick={() => setMobileFiltersOpen(!mobileFiltersOpen)}
            className="w-full flex items-center justify-between p-4 sm:p-6 md:cursor-default"
          >
            <div className="flex items-center gap-3">
              <SlidersHorizontal size={16} className="text-orange-500" />
              <span className="text-sm font-black uppercase tracking-widest text-orange-500">Filtros</span>
              {(selectedCompany !== 'all' || selectedUser !== 'all' || selectedRequester !== 'all' || startDate || endDate) && (
                <span className="bg-orange-600 text-white text-[9px] font-black px-2 py-0.5 rounded-full uppercase">Ativo</span>
              )}
              {(!startDate && !endDate && selectedCompany === 'all' && selectedUser === 'all' && selectedRequester === 'all') && (
                <span className="bg-sky-600/20 border border-sky-500/30 text-sky-300 text-[9px] font-black px-2 py-0.5 rounded-full uppercase">
                  {globalPeriodMode === 'last6m' ? 'Padrão: 6 meses' : 'Padrão: período total'}
                </span>
              )}
            </div>
            <ChevronDown
              size={16}
              className={cn("text-gray-500 transition-transform md:hidden", mobileFiltersOpen && "rotate-180")}
            />
          </button>

          {/* Filter Body */}
          <div className={cn("md:block", mobileFiltersOpen ? "block" : "hidden")}>
            <div className="flex flex-col sm:flex-row flex-wrap items-stretch sm:items-end gap-4 px-4 sm:px-6 pb-4 sm:pb-6 pt-0">
              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Período</Label>
                <div className="flex items-center gap-2 bg-black/30 border border-white/10 rounded-xl p-1 h-11">
                  <button
                    onClick={() => {
                      setGlobalPeriodMode('last6m');
                      setStartDate(undefined);
                      setEndDate(undefined);
                    }}
                    className={cn(
                      "h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all",
                      globalPeriodMode === 'last6m'
                        ? "bg-orange-600 text-white"
                        : "text-gray-300 hover:text-white hover:bg-white/10"
                    )}
                  >
                    Últimos 6 meses
                  </button>
                  <button
                    onClick={() => {
                      setGlobalPeriodMode('all');
                      setStartDate(undefined);
                      setEndDate(undefined);
                    }}
                    className={cn(
                      "h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all",
                      globalPeriodMode === 'all'
                        ? "bg-sky-600 text-white"
                        : "text-gray-300 hover:text-white hover:bg-white/10"
                    )}
                  >
                    Período total
                  </button>
                </div>
              </div>

              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Inicial</Label>
                <Popover>
                  <PopoverTrigger className={cn(buttonVariants({ variant: "outline" }), "w-full sm:w-[160px] h-11 justify-start bg-black/40 border-white/10 rounded-xl text-white font-bold")}>
                    <CalendarIcon className="mr-2 h-4 w-4 text-orange-500" />
                    {startDate ? format(startDate, "dd/MM/yyyy") : "Início"}
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0 bg-[#161618] border-white/10" align="start">
                    <Calendar
                      mode="single"
                      selected={startDate}
                      onSelect={(date: any) => {
                        if (!date) return;
                        setGlobalPeriodMode('last6m');
                        setStartDate(date);
                      }}
                      className="text-white"
                    />
                  </PopoverContent>
                </Popover>
              </div>

              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Final</Label>
                <Popover>
                  <PopoverTrigger className={cn(buttonVariants({ variant: "outline" }), "w-full sm:w-[160px] h-11 justify-start bg-black/40 border-white/10 rounded-xl text-white font-bold")}>
                    <CalendarIcon className="mr-2 h-4 w-4 text-orange-500" />
                    {endDate ? format(endDate, "dd/MM/yyyy") : "Fim"}
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0 bg-[#161618] border-white/10" align="start">
                    <Calendar
                      mode="single"
                      selected={endDate}
                      onSelect={(date: any) => {
                        if (!date) return;
                        setGlobalPeriodMode('last6m');
                        setEndDate(date);
                      }}
                      className="text-white"
                    />
                  </PopoverContent>
                </Popover>
              </div>

              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Empresa</Label>
                <Select value={selectedCompany} onValueChange={setSelectedCompany}>
                  <SelectTrigger className="w-full sm:w-[180px] bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                    <span className="truncate">{selectedCompanyName}</span>
                  </SelectTrigger>
                  <SelectContent className="bg-[#161618] border-white/10 text-white">
                    <SelectItem value="all">Todas as Empresas</SelectItem>
                     {companies.map(c => (
                       <SelectItem key={`empresa-${c.id}`} value={String(c.id)}>{c.name || `Empresa ${c.id}`}</SelectItem>
                     ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Comprador</Label>
                <Select value={selectedUser} onValueChange={setSelectedUser}>
                  <SelectTrigger className="w-full sm:w-[180px] bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                    <SelectValue placeholder="Todos" />
                  </SelectTrigger>
                  <SelectContent className="bg-[#161618] border-white/10 text-white">
                    <SelectItem value="all">Todos os Compradores</SelectItem>
                     {availableUsers.map(u => (
                       <SelectItem key={u.id} value={String(u.id)}>{u.name}</SelectItem>
                     ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Solicitante</Label>
                <Select value={selectedRequester} onValueChange={setSelectedRequester}>
                  <SelectTrigger className="w-full sm:w-[180px] bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                    <SelectValue placeholder="Todos" />
                  </SelectTrigger>
                  <SelectContent className="bg-[#161618] border-white/10 text-white">
                    <SelectItem value="all">Todos os Solicitantes</SelectItem>
                     {availableRequesters.map(r => (
                       <SelectItem key={`req-${r.id}`} value={String(r.id)}>{r.name}</SelectItem>
                     ))}
                  </SelectContent>
                </Select>
              </div>

              <Button 
                onClick={() => { setMobileFiltersOpen(false); }} 
                className="h-11 px-6 bg-orange-600 hover:bg-orange-700 text-white font-black rounded-xl shadow-lg shadow-orange-600/20 w-full sm:w-auto"
              >
                Filtrar Dados
              </Button>
            </div>
          </div>
        </div>
        )}

        <AnimatePresence mode="wait">
          {/* 1. DASHBOARD GERAL */}
          {activeTab === 'dashboard' && (
            <motion.div key="db-geral" initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -20 }} className="space-y-8">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-6">
                {[
                  { label: selectedCompany !== 'all' ? `COMPRAS — ${companies.find((c: any) => String(c.id) === selectedCompany)?.name || 'Empresa'}` : 'COMPRAS EFETUADAS', value: `R$ ${stats.total.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`, icon: TrendingUp, color: 'orange' },
                  { label: selectedCompany !== 'all' ? `SALDO — ${companies.find((c: any) => String(c.id) === selectedCompany)?.name || 'Empresa'}` : 'Saldo Financeiro', value: `R$ ${stats.balance.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`, icon: DollarSign, color: stats.balance >= 0 ? 'green' : 'red' },
                  { label: 'Obras Ativas', value: activeBuildingCount, icon: Building2, color: 'orange' },
                  { label: 'Total de Pedidos', value: orders.length, icon: Package, color: 'orange' }
                ].map((kpi, i) => (
                  <Card key={i} className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative group">
                    <div className="absolute top-0 right-0 p-3 opacity-10 group-hover:opacity-20 transition-opacity"><kpi.icon size={40} className="text-orange-500" /></div>
                    <CardHeader className="pb-2 p-4 sm:p-6">
                      <CardDescription className="text-[9px] sm:text-[10px] font-black uppercase tracking-widest text-orange-500/70 leading-tight">{kpi.label}</CardDescription>
                      <CardTitle className={cn("text-xl sm:text-3xl font-black tracking-tighter mt-1", kpi.color === 'red' ? 'text-red-500' : kpi.color === 'green' ? 'text-green-500' : 'text-white')}>{kpi.value}</CardTitle>
                    </CardHeader>
                    <div className="h-1 w-full bg-orange-600/20"><div className="h-full bg-orange-600 w-1/3" /></div>
                  </Card>
                ))}
              </div>
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-8">
                <Card className="lg:col-span-2 bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Evolução Histórica</CardTitle></CardHeader>
                  <CardContent className="h-[220px] sm:h-[350px] pt-4">
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={chartData}>
                        <defs>
                          <linearGradient id="colorVal" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#f97316" stopOpacity={0.3}/>
                            <stop offset="95%" stopColor="#f97316" stopOpacity={0}/>
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ffffff05" />
                        <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 12}} />
                        <YAxis axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 12}} tickFormatter={(v) => `R$${v/1000}k`} />
                        <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: '1px solid rgba(255,255,255,0.1)' }} />
                        <Legend />
                        <Area type="monotone" dataKey="valor" name="Compras Globais" stroke="#f97316" strokeWidth={4} fillOpacity={1} fill="url(#colorVal)" />
                      </AreaChart>
                    </ResponsiveContainer>
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
          )}

          {/* 2. FINANCEIROS (Fluxos Futuros e Recentes) */}
          {activeTab === 'dashboard-financeiro' && (
            <motion.div key="db-fin" initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.95 }} className="space-y-8">
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-8">
                <Card className="lg:col-span-2 bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Fluxo de Caixa (A Pagar x A Receber) Mensal</CardTitle></CardHeader>
                  <CardContent className="h-[250px] sm:h-[400px] pt-4">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={cashFlowData}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ffffff05" />
                        <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 12}} />
                        <YAxis axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 12}} tickFormatter={(v) => `R$${v/1000}k`} />
                        <Tooltip cursor={{fill: '#ffffff05'}} formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Legend />
                        <Bar dataKey="despesa" name="Despesas" fill="#ef4444" radius={[4, 4, 0, 0]} />
                        <Bar dataKey="receita" name="Receitas" fill="#22c55e" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Balanço de Saldo Atual</CardTitle></CardHeader>
                  <CardContent className="h-[250px] sm:h-[400px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie data={financeBalanceData} cx="50%" cy="50%" outerRadius={100} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`} dataKey="value">
                          <Cell fill="#10b981" />
                          <Cell fill="#f59e0b" />
                        </Pie>
                        <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>
            </motion.div>
          )}

          {/* 3. OBRAS (Custo Acumulado) */}
          {activeTab === 'dashboard-obras' && (
            <motion.div key="db-obras" initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.95 }} className="space-y-8">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-8">
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Custo por Obra (Top 7)</CardTitle></CardHeader>
                  <CardContent className="h-[300px] sm:h-[400px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={buildingCostData} layout="vertical" margin={{ left: 50 }}>
                        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#ffffff05" />
                        <XAxis type="number" hide />
                        <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{fill: '#fff', fontSize: 10}} width={120} />
                        <Tooltip cursor={{fill: '#ffffff05'}} formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Bar dataKey="gasto" name="Gasto Total" fill="#f97316" radius={[0, 4, 4, 0]} barSize={20} />
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Concentração de Gasto</CardTitle></CardHeader>
                  <CardContent className="h-[300px] sm:h-[400px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie data={buildingCostData} cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={2} dataKey="gasto">
                           {buildingCostData.map((e, index) => <Cell key={index} fill={['#f97316', '#3b82f6', '#10b981', '#ef4444', '#8b5cf6', '#6366f1', '#14b8a6'][index % 7]} />)}
                        </Pie>
                        <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>
            </motion.div>
          )}

          {/* 4. LOGÍSTICA (Fornecedores e Condições) */}
          {activeTab === 'dashboard-logistica' && (
            <motion.div key="db-log" initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.95 }} className="space-y-8">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-8">
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Top Fornecedores (Valores)</CardTitle></CardHeader>
                  <CardContent className="h-[300px] sm:h-[400px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={supplierData} margin={{ bottom: 20 }}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ffffff05" />
                        <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 10}} angle={-45} textAnchor="end" />
                        <YAxis axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 12}} tickFormatter={(v) => `R$${v/1000}k`} />
                        <Tooltip cursor={{fill: '#ffffff05'}} formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Bar dataKey="value" name="Volume Comprado" fill="#3b82f6" radius={[4, 4, 0, 0]} maxBarSize={60} />
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Formas de Pagamento Aprovadas</CardTitle></CardHeader>
                  <CardContent className="h-[300px] sm:h-[400px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie data={paymentMethodData} cx="50%" cy="50%" innerRadius={0} outerRadius={110} dataKey="value">
                           {paymentMethodData.map((e, index) => <Cell key={index} fill={['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ef4444'][index % 5]} />)}
                        </Pie>
                        <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>
            </motion.div>
          )}

          {activeTab === 'obras-alerta' && (
            <motion.div
              key="obras-alerta"
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full space-y-6"
            >
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <h3 className="text-xl sm:text-2xl font-black text-white flex items-center gap-3">
                  <AlertTriangle className="text-orange-500" size={24} />
                  Painel de Sprints (Kanban)
                </h3>
                <Button
                  onClick={loadKanbanOverview}
                  className="bg-white text-black hover:bg-gray-200 font-black tracking-tight rounded-xl text-sm h-9"
                >
                  <RefreshCw size={14} className={cn('mr-2', kanbanOverviewLoading && 'animate-spin')} />
                  Atualizar Painel
                </Button>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-4">
                    <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Total de Sprints</p>
                    <p className="text-2xl font-black text-white">{kanbanSummaryForView.totalSprints}</p>
                  </CardContent>
                </Card>
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-4">
                    <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Sprints em Atraso</p>
                    <p className="text-2xl font-black text-red-400">{kanbanSummaryForView.overdueSprints}</p>
                  </CardContent>
                </Card>
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-4">
                    <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Cards em Atraso</p>
                    <p className="text-2xl font-black text-orange-400">{kanbanSummaryForView.overdueCards}</p>
                  </CardContent>
                </Card>
              </div>

              <Card className="bg-[#161618] border-white/5 shadow-2xl">
                <CardHeader>
                  <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Sprints Criadas no Kanban</CardTitle>
                </CardHeader>
                <CardContent className="p-0 overflow-x-auto overflow-y-auto max-h-[620px] custom-scrollbar">
                  <Table>
                    <TableHeader className="bg-black/80 sticky top-0 z-10 backdrop-blur-md border-b border-white/10">
                      <TableRow className="border-none">
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Sprint</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Obra</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Início</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Prazo Final</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-center">Progresso</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-center">Status</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {kanbanOverviewLoading ? (
                        <TableRow>
                          <TableCell colSpan={6} className="text-center py-10 text-gray-400 font-bold">Carregando sprints...</TableCell>
                        </TableRow>
                      ) : kanbanSprintsForView.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={6} className="text-center py-10 text-gray-500 font-bold">Nenhuma sprint encontrada para os filtros atuais.</TableCell>
                        </TableRow>
                      ) : (
                        kanbanSprintsForView.map((sprint) => {
                          const totalCards = sprint.stats?.totalCards || 0;
                          const openCards = sprint.stats?.openCards || 0;
                          const doneCards = Math.max(totalCards - openCards, 0);
                          const progress = totalCards > 0 ? Math.round((doneCards / totalCards) * 100) : 0;

                          return (
                            <TableRow key={sprint.id} className="border-white/5 hover:bg-white/[0.03]">
                              <TableCell>
                                <div className="flex items-center gap-2">
                                  <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: sprint.color || '#f97316' }} />
                                  <span className="font-bold text-white text-xs">{sprint.name}</span>
                                </div>
                              </TableCell>
                              <TableCell className="text-xs font-bold text-gray-300">{sprint.buildingName || `Obra ${sprint.buildingId}`}</TableCell>
                              <TableCell className="text-xs text-gray-400">{sprint.startDate ? safeFormat(sprint.startDate, 'dd/MM/yyyy') : '—'}</TableCell>
                              <TableCell className="text-xs text-gray-400">{sprint.endDate ? safeFormat(sprint.endDate, 'dd/MM/yyyy') : '—'}</TableCell>
                              <TableCell className="text-center">
                                <Badge className="bg-white/10 text-white border-white/10 font-black text-[10px]">
                                  {doneCards}/{totalCards} ({progress}%)
                                </Badge>
                              </TableCell>
                              <TableCell className="text-center">
                                {sprint.overdue ? (
                                  <Badge className="bg-red-600 text-white font-black text-[10px] animate-pulse">
                                    ALERTA: ATRASADA
                                  </Badge>
                                ) : (
                                  <Badge className="bg-emerald-600 text-white font-black text-[10px]">
                                    NO PRAZO
                                  </Badge>
                                )}
                              </TableCell>
                            </TableRow>
                          );
                        })
                      )}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </motion.div>
          )}

          {(activeTab === 'alerts' || activeTab === 'financeiro-alerta') && (
            <motion.div
              key="alerts"
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full space-y-6"
            >
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
                <h3 className="text-xl sm:text-2xl font-black text-white flex items-center gap-3">
                  <Bell className="text-orange-500" size={24} />
                  Variações de Preço
                </h3>
                <div className="flex items-center gap-3">
                  <Badge className="bg-orange-600 text-white font-black px-3 py-1 print:hidden text-xs">
                    {priceAlerts.length} {priceAlerts.length === 1 ? 'ALERTA' : 'ALERTAS'}
                  </Badge>
                  {pagamentosHoje.length > 0 && (
                    <Badge className="bg-emerald-600 animate-pulse font-black px-3 py-1 text-xs text-white">
                      {pagamentosHoje.length} PAGAMENTO(S) HOJE
                    </Badge>
                  )}
                  <Button 
                    onClick={handlePrint}
                    className="bg-white text-black hover:bg-gray-200 font-black tracking-tight rounded-xl print:hidden text-sm h-9"
                  >
                    <Printer size={14} className="mr-2" />
                    PDF
                  </Button>
                </div>
              </div>

              {pagamentosHoje.length > 0 && (
                <div className="mb-8">
                  <h4 className="text-emerald-500 font-bold uppercase tracking-widest text-sm mb-3">Pagamentos Efetuados Hoje</h4>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                    {pagamentosHoje.map(p => (
                      <Card key={p.id} className="bg-emerald-950/20 border-emerald-500/20 shadow-none">
                        <CardContent className="p-4">
                          <p className="text-emerald-400 text-xs font-bold uppercase mb-1">{p.creditorName}</p>
                          <h3 className="text-white font-black text-lg">
                            {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(p.amount)}
                          </h3>
                          <p className="text-gray-400 text-[10px] mt-1">{p.description} (Obra: {resolveBuildingName(p)})</p>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </div>
              )}

              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3 sm:gap-4">
                {priceAlerts.slice(0, 8).map((alert, idx) => (
                  <Card key={idx} onClick={() => setModalItemHistory({ name: alert.item, history: alert.history?.filter(h => h.date.substring(0, 10) === alert.newDate.substring(0, 10)) || [] })} className="cursor-pointer hover:border-orange-500/50 hover:scale-[1.02] bg-gradient-to-br from-orange-600/20 to-transparent border-orange-500/20 shadow-none overflow-hidden transition-all">
                    <CardContent className="p-4 sm:p-5 pb-0 relative">
                      <div className="flex flex-col mb-4">
                        <h4 className="text-white font-black uppercase text-xs sm:text-[13px] leading-tight w-full mb-3 pb-2 border-b border-white/5" title={alert.item}>
                          {alert.item}
                        </h4>
                        
                        <div className="flex items-start gap-2 w-full justify-between">
                          <div className="flex flex-1 items-center gap-3 sm:gap-6">
                            <div className="flex flex-col flex-1">
                              <p className="text-gray-500 text-[9px] font-bold tracking-widest uppercase mb-1">Anterior</p>
                              <h3 className="text-xs sm:text-sm font-bold text-gray-400 decoration-red-500/30">
                                {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(alert.oldPrice)}
                              </h3>
                              <p className="text-[#666] text-[8px] font-bold mt-0.5">{safeFormat(alert.oldDate)}</p>
                            </div>

                            <div className="w-px h-8 bg-white/5 mx-1" />

                            <div className="flex flex-col flex-1">
                              <p className="text-orange-500 text-[9px] font-bold tracking-widest uppercase mb-1">Atual</p>
                              <h3 className="text-sm sm:text-base font-black text-white">
                                {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(alert.newPrice)}
                              </h3>
                              <p className="text-orange-600/50 text-[8px] font-bold mt-0.5">{safeFormat(alert.newDate)}</p>
                            </div>
                          </div>
                          
                          <div className="shrink-0 flex items-center justify-center bg-orange-500/10 border border-orange-500/20 rounded-lg px-2 sm:px-3 py-1 sm:py-1.5 h-fit mt-3">
                            <span className="text-orange-500 font-black text-[10px] sm:text-xs tracking-tighter">
                              +{alert.diff > 1000 ? '>1000' : alert.diff}%
                            </span>
                          </div>
                        </div>
                        </div>

                        {alert.history && alert.history.length > 0 && (
                          <div className="mt-4 h-[50px] sm:h-16 w-full opacity-60 hover:opacity-100 transition-opacity pointer-events-none">
                            <ResponsiveContainer width="100%" height="100%">
                              <LineChart data={alert.history}>
                                <Line type="monotone" dataKey="price" stroke="#f97316" strokeWidth={2} dot={{ fill: '#f97316', strokeWidth: 1, r: 2 }} activeDot={{ r: 4 }} />
                              </LineChart>
                            </ResponsiveContainer>
                          </div>
                        )}
                      </CardContent>
                    </Card>
                ))}
              </div>

              {/* MODAL HISTÓRICO DE PREÇOS */}
              {modalItemHistory && (
                <div className="fixed inset-0 z-[999] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4" onClick={() => { setModalItemHistory(null); setExpandedDetail(null); }}>
                  <div className="bg-[#111] border border-orange-500/30 p-6 rounded-3xl max-w-5xl w-full flex flex-col shadow-2xl max-h-[90vh] overflow-y-auto" onClick={e => e.stopPropagation()}>
                    <div className="flex justify-between items-start mb-2">
                      <div>
                        <h2 className="text-white text-xl md:text-2xl font-black uppercase tracking-widest leading-tight">{modalItemHistory.name}</h2>
                        <p className="text-orange-400/80 text-xs font-bold uppercase tracking-widest mt-1">
                          Histórico de Preços • {modalItemHistory.history.length} {modalItemHistory.history.length === 1 ? 'registro' : 'registros'} encontrado{modalItemHistory.history.length !== 1 ? 's' : ''}
                        </p>
                      </div>
                      <button onClick={() => { setModalItemHistory(null); setExpandedDetail(null); }} className="text-gray-500 hover:text-white bg-white/5 hover:bg-white/10 rounded-full p-2 ml-4 flex-shrink-0"><X size={20}/></button>
                    </div>

                    {modalItemHistory.history.length === 1 && (
                      <div className="mb-4 px-4 py-3 bg-blue-900/20 border border-blue-500/30 rounded-xl flex items-start gap-3">
                        <span className="text-blue-400 text-lg flex-shrink-0">ℹ️</span>
                        <p className="text-blue-300 text-xs font-bold leading-relaxed">
                          Este produto foi comprado apenas <strong>1 vez</strong> no histórico disponível. A cotação no Sienge pode ter incluído múltiplos fornecedores, mas os preços comparativos dos concorrentes não estão disponíveis via API. O sistema exibe o histórico de compras realizadas.
                        </p>
                      </div>
                    )}
                    
                    <div className={`grid grid-cols-1 gap-4 ${modalItemHistory.history.length >= 3 ? 'md:grid-cols-3' : modalItemHistory.history.length === 2 ? 'md:grid-cols-2' : 'md:grid-cols-1 max-w-sm mx-auto w-full'}`}>
                      {modalItemHistory.history.length === 0 ? (
                        <div className="col-span-3 text-center text-gray-500 py-10 font-bold tracking-widest uppercase">
                          Nenhum histórico disponível para exibir
                        </div>
                      ) : (() => {
                        const sorted = [...modalItemHistory.history].sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());
                        const championIdx = sorted.length > 1 ? 0 : -1;
                        const championPrice = sorted[0]?.price;
                        const prevPrice = sorted[1]?.price;
                        const championIsExpensive = championPrice !== undefined && prevPrice !== undefined && championPrice > prevPrice;
                        return sorted.slice(0, 3).map((hist, i) => {
                           const isChampion = i === championIdx;
                           const priceColor = isChampion
                             ? (championIsExpensive ? 'text-red-400' : 'text-green-400')
                             : 'text-white';
                           const champBg = isChampion
                             ? (championIsExpensive ? 'bg-red-600/10 border-red-500/50 shadow-[0_0_30px_rgba(239,68,68,0.15)] scale-[1.02]' : 'bg-emerald-600/10 border-emerald-500/50 shadow-[0_0_30px_rgba(16,185,129,0.15)] scale-[1.02]')
                             : 'bg-black/40 border-white/10';
                           const champTagBg = isChampion
                             ? (championIsExpensive ? 'bg-gradient-to-bl from-red-600 to-rose-500' : 'bg-gradient-to-bl from-emerald-500 to-teal-500')
                             : '';
                           const buyerName = users.find(u => String(u.id) === hist.buyerId)?.name || hist.buyerId || '—';
                           const supplierName = creditorMap[hist.creditorId || ''] || hist.creditorId || '—';
                           const labelText = sorted.length === 1 ? 'Última Compra' : `Compra ${i + 1} de ${sorted.length > 3 ? '3+' : sorted.length}`;
                           return (
                            <div key={i} className={cn('border rounded-2xl p-6 flex flex-col relative overflow-hidden transition-all', champBg)}>
                              {isChampion && (
                                <div className={cn('absolute top-0 right-0 text-white font-black text-[10px] tracking-widest uppercase px-4 py-1.5 rounded-bl-xl shadow-lg', champTagBg)}>
                                  {championIsExpensive ? '⚠ Alta' : '✓ Mais Recente'}
                                </div>
                              )}
                              <h3 className="text-gray-400 font-bold uppercase tracking-widest text-xs mb-4 text-center border-b border-white/10 pb-4">{labelText}</h3>
                              <div className="flex flex-col items-center justify-center flex-1 py-4">
                                <h4 className={cn('text-3xl font-black mb-1', priceColor)}>
                                  {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(hist.price)}
                                </h4>
                                <p className="text-gray-400 text-sm font-bold mt-1">{supplierName}</p>
                                <p className="text-gray-500 text-xs font-bold uppercase tracking-widest mt-1">{safeFormat(hist.date)}</p>
                              </div>
                              <div className="mt-4 pt-4 border-t border-white/5 flex flex-col text-center gap-2">
                                <button
                                  onClick={() => setExpandedDetail(expandedDetail === i ? null : i)}
                                  className="text-[10px] text-orange-400 hover:text-orange-300 uppercase font-black tracking-widest py-1.5 rounded-md border border-orange-500/20 bg-orange-500/5 hover:bg-orange-500/10 transition-all"
                                >
                                  {expandedDetail === i ? '▲ Fechar Detalhes' : `▼ Ver Detalhes  #${hist.orderId || 'S/N'}`}
                                </button>
                                {expandedDetail === i && (
                                  <div className="text-left space-y-2 bg-black/30 rounded-xl p-3 border border-white/5 mt-1">
                                    <div className="flex flex-col">
                                      <span className="text-[9px] text-gray-500 uppercase font-black tracking-widest">Comprador</span>
                                      <span className="text-xs text-white font-bold">{buyerName}</span>
                                    </div>
                                    <div className="flex flex-col">
                                      <span className="text-[9px] text-gray-500 uppercase font-black tracking-widest">Fornecedor</span>
                                      <span className="text-xs text-white font-bold">{supplierName}</span>
                                    </div>
                                    <div className="flex flex-col">
                                      <span className="text-[9px] text-gray-500 uppercase font-black tracking-widest">Data / Hora</span>
                                      <span className="text-xs text-white font-bold">{safeFormat(hist.date, 'dd/MM/yyyy HH:mm')}</span>
                                    </div>
                                  </div>
                                )}
                              </div>
                            </div>
                           );
                        });
                      })()}
                    </div>
                  </div>
                </div>
              )}


              <Card className="bg-[#161618] border-white/5 shadow-2xl mt-10">
                <CardHeader className="print:hidden">
                  <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Relatório / Alertas de Itens</CardTitle>
                </CardHeader>
                <CardContent className="p-0 overflow-x-auto overflow-y-auto max-h-[500px] sm:max-h-[600px] custom-scrollbar print:overflow-visible print:max-h-none">
                  <Table className="print:text-black relative">
                    <TableHeader className="bg-black/80 sticky top-0 z-10 backdrop-blur-md print:bg-gray-100 print:relative border-b border-white/10">
                      <TableRow className="border-none print:border-gray-200">
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Item e Código</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black cursor-pointer group hover:text-white transition-colors" onClick={() => toggleSort('date')}>
                          <div className="flex items-center">Data {renderSortIcon('date')}</div>
                        </TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Status</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Solicitante</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Comprador</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Prazos</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-center print:text-black">Qtd</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black cursor-pointer group hover:text-white transition-colors" onClick={() => toggleSort('vlrUnit')}>
                          <div className="flex items-center justify-end">Vlr Unit {renderSortIcon('vlrUnit')}</div>
                        </TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black cursor-pointer group hover:text-white transition-colors" onClick={() => toggleSort('vlrAtual')}>
                          <div className="flex items-center justify-end">Vlr Atual {renderSortIcon('vlrAtual')}</div>
                        </TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black cursor-pointer group hover:text-white transition-colors" onClick={() => toggleSort('valorTotal')}>
                          <div className="flex items-center justify-end">Valor Total {renderSortIcon('valorTotal')}</div>
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                      <TableBody>
                        {orders.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={9} className="text-center py-10 text-gray-500 font-bold">
                              Nenhum pedido ou alerta registrado para o período e filtros selecionados.
                            </TableCell>
                          </TableRow>
                        ) : (() => {
                          let flatItems: any[] = [];
                          orders.forEach(o => {
                            const itemsList = itemsDetailsMap[o.id];
                            const comprador = o.buyerId || "N/A";
                            const solicitante = o.requesterId && o.requesterId !== '0' ? String(o.requesterId).replace(/^Comprador\\s+/i, '').trim() : String(o.createdBy || 'N/A').replace(/^Comprador\\s+/i, '').trim();
                            
                            if (!itemsList || itemsList.length === 0) {
                              flatItems.push({
                                o, idx: 0, isFallback: true, desc: `Cod. ${o.id} (Carregando...)`,
                                comprador, solicitante, qty: 0, vlrBase: 0, vlrAtual: 0, totalAmount: o.totalAmount, dateNumeric: o.dateNumeric || 0
                              });
                              return;
                            }
                            
                            itemsList.forEach((item: any, idx: number) => {
                              if (!item) return;
                              const qty = Number(item.quantity || item.quantidade || 1);
                              const realUnitValue = Number(item.netPrice || item.unitPrice || item.valorUnitario || 0);
                              const totalAmount = qty * realUnitValue;
                              const desc = item.resourceDescription || item.descricao || `Item ${idx+1}`;
                              const vlrAtual = Number(latestPricesMap[desc]) || 0;
                              const vlrBase = Number(baselinePricesMap[desc]) || realUnitValue;
                              
                              flatItems.push({
                                o, idx, isFallback: false, desc, comprador, solicitante,
                                qty, vlrBase, vlrAtual, totalAmount, dateNumeric: o.dateNumeric || 0
                              });
                            });
                          });
                          
                          if (alertSortConfig) {
                            flatItems.sort((a,b) => {
                              let valA = 0, valB = 0;
                              if (alertSortConfig.key === 'date') { valA = a.dateNumeric; valB = b.dateNumeric; }
                              if (alertSortConfig.key === 'vlrUnit') { valA = a.vlrBase; valB = b.vlrBase; }
                              if (alertSortConfig.key === 'vlrAtual') { valA = a.vlrAtual; valB = b.vlrAtual; }
                              if (alertSortConfig.key === 'valorTotal') { valA = a.totalAmount; valB = b.totalAmount; }
                              
                              return alertSortConfig.direction === 'asc' ? valA - valB : valB - valA;
                            });
                          } else {
                            flatItems.sort((a,b) => b.dateNumeric - a.dateNumeric);
                          }
                          
                          return flatItems.slice(0, isPrinting ? 999999 : 100).map((flat, i) => {
                            const { o, isFallback, desc, comprador, solicitante, qty, vlrBase, vlrAtual, totalAmount } = flat;
                            if (isFallback) {
                              return (
                                <TableRow key={`alert-${o.id}-fallback-${i}`} className="border-white/5 hover:bg-white/5 transition-colors">
                                  <TableCell className="font-bold text-orange-500 whitespace-nowrap">{desc}</TableCell>
                                  <TableCell className="text-xs text-gray-500">{safeFormat(o.date)}</TableCell>
                                  <TableCell><Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">{translateStatusLabel(o.status)}</Badge></TableCell>
                                  <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{solicitante}</TableCell>
                                  <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{comprador}</TableCell>
                                  <TableCell className="text-xs text-gray-400">{o.paymentCondition || "N/A"}</TableCell>
                                  <TableCell className="text-xs font-mono text-gray-500 text-center">-</TableCell>
                                  <TableCell className="text-xs text-gray-400 font-mono text-right">-</TableCell>
                                  <TableCell className="text-xs text-gray-400 font-mono text-right">-</TableCell>
                                  <TableCell className="text-right font-black text-white whitespace-nowrap">R$ {totalAmount.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}</TableCell>
                                </TableRow>
                              );
                            }
                            
                            const isCheaper = vlrAtual > 0 && vlrAtual < vlrBase;
                            const isExpensive = vlrAtual > vlrBase;
                            const colorClass = isCheaper ? "text-green-500 font-black drop-shadow-[0_0_6px_rgba(34,197,94,0.75)]" : isExpensive ? "text-red-500 font-black drop-shadow-[0_0_6px_rgba(239,68,68,0.75)]" : "text-gray-400";
                            
                            return (
                              <TableRow
                                key={`alert-${o.id}-${flat.idx}-${i}`}
                                onClick={() => {
                                  const history = globalItemHistory[desc] ? globalItemHistory[desc].sort((a,b)=> new Date(b.date).getTime() - new Date(a.date).getTime()) : [];
                                  setModalItemHistory({ name: desc, history });
                                }}
                                className={cn(
                                  "border-white/5 hover:bg-white/10 transition-colors border-l-2 cursor-pointer",
                                  isCheaper && "border-l-green-500 shadow-[inset_4px_0_0_rgba(34,197,94,0.95)] bg-[linear-gradient(90deg,rgba(34,197,94,0.12),transparent_18%)]",
                                  isExpensive && "border-l-red-500 shadow-[inset_4px_0_0_rgba(239,68,68,0.95)] bg-[linear-gradient(90deg,rgba(239,68,68,0.12),transparent_18%)]"
                                )}
                              >
                                <TableCell className="font-bold text-orange-500" title={desc}>
                                  <div className="max-w-[200px] truncate">{desc}</div>
                                </TableCell>
                                <TableCell className="text-xs text-gray-500">{safeFormat(o.date)}</TableCell>
                                <TableCell>
                                  <Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">{translateStatusLabel(o.status)}</Badge>
                                </TableCell>
                                <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{solicitante}</TableCell>
                                <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{comprador}</TableCell>
                                <TableCell className="text-xs text-gray-400">{o.paymentCondition || "N/A"}</TableCell>
                                <TableCell className="text-xs font-mono text-gray-500 text-center">{qty}</TableCell>
                                <TableCell className="text-xs text-gray-400 font-mono text-right" title="Valor Anterior da Data Inicial">
                                  R$ {vlrBase.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}
                                </TableCell>
                                <TableCell className={`text-xs font-mono text-right ${colorClass}`}>
                                  {vlrAtual > 0 ? `R$ ${vlrAtual.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}` : '-'}
                                </TableCell>
                                <TableCell className="text-right font-black text-white whitespace-nowrap">
                                  R$ {totalAmount.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}
                                </TableCell>
                              </TableRow>
                            );
                          });
                        })()}
                      </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </motion.div>
          )}

          {(activeTab === 'finance' || activeTab === 'obras-valores') && (
            <motion.div
              key="finance"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-8"
            >
              {(() => {
                const openPayables = financialTitles.filter(t => t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO').sort((a,b) => (a.dueDateNumeric || 0) - (b.dueDateNumeric || 0));
                const openReceivables = receivableTitles.filter(t => t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO').sort((a,b) => (a.dueDateNumeric || 0) - (b.dueDateNumeric || 0));
                const paidPayables = financialTitles.filter(t => t.status === 'BAIXADO' || t.status === 'PAGO' || t.status === 'LIQUIDADO').sort((a,b) => (b.paymentDateNumeric || b.dueDateNumeric || 0) - (a.paymentDateNumeric || a.dueDateNumeric || 0));

                const totalPayable = openPayables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                const totalReceivable = openReceivables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                return (
                  <>
                    {/* Demonstração de Resultados (DRE) Projetada */}
                    <Card className="bg-[#161618] border-white/5 shadow-2xl mb-6">
                      <CardHeader className="border-b border-white/5 bg-black/20 pb-4">
                        <div className="flex items-center justify-between">
                          <CardTitle className="text-xl font-black uppercase tracking-tight text-white flex items-center gap-2">
                            <TrendingUp className="text-orange-500" size={24} />
                            Demonstrativo de Resultado (DRE Projetado Sienge)
                          </CardTitle>
                          <span className="text-xs font-bold bg-orange-600/20 text-orange-500 px-3 py-1 rounded-full border border-orange-500/20">
                            Dinâmico
                          </span>
                        </div>
                        <CardDescription className="text-gray-400 mt-2">
                          Cálculo analítico baseado nos títulos financeiros recebidos e pagos, utilizando matriz de proporção do Sienge.
                        </CardDescription>
                      </CardHeader>
                      <CardContent className="pt-6">
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                          {/* Receitas */}
                          <div className="space-y-4">
                            <h4 className="text-sm font-black text-green-500 uppercase tracking-wider mb-4 border-b border-green-500/20 pb-2">1. Receitas</h4>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-300">Receita Operacional Bruta</span>
                              <span className="font-mono text-white">R$ {dreStats.receitaBruta.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Deduções e Impostos</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.deducoes.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center font-bold text-green-400 bg-green-500/10 p-2 rounded">
                              <span>(=) Receita Líquida (ROL)</span>
                              <span className="font-mono">R$ {dreStats.rol.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                          </div>

                          {/* Custos */}
                          <div className="space-y-4">
                            <h4 className="text-sm font-black text-red-500 uppercase tracking-wider mb-4 border-b border-red-500/20 pb-2">2. Custos (CSP)</h4>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Mão-de-Obra</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.custos.maoDeObra.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Materiais e Insumos</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.custos.materiais.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Serviços</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.custos.servicos.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center font-bold text-red-400 bg-red-500/10 p-2 rounded">
                              <span>Total CSP</span>
                              <span className="font-mono">-R$ {dreStats.custos.total.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                          </div>

                          {/* Despesas e Resultado */}
                          <div className="space-y-4">
                            <h4 className="text-sm font-black text-orange-500 uppercase tracking-wider mb-4 border-b border-orange-500/20 pb-2">3. Despesas e Resultado</h4>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Despesas Gerais/Adm</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.despesas.gerais.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Pré-Labore & Trib.</span>
                              <span className="font-mono text-red-400">-R$ {(dreStats.despesas.preLabore + dreStats.despesas.tributarias).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Desp. Financeiras + IR/CSLL</span>
                              <span className="font-mono text-red-400">-R$ {(dreStats.despFinanceiras + dreStats.irCsll).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className={cn("flex justify-between items-center font-black p-3 rounded text-lg", dreStats.resultadoLiquido >= 0 ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400")}>
                              <span>(=) RESULTADO LÍQUIDO</span>
                              <span className="font-mono">R$ {dreStats.resultadoLiquido.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                          </div>
                        </div>
                      </CardContent>
                    </Card>

                    <div className="flex items-center justify-end gap-4 bg-[#161618] border border-white/5 p-4 rounded-xl shadow-2xl mb-2">
                       <div className="ml-auto flex items-end gap-6">
                           <div className="flex flex-col text-right">
                              <span className="text-[9px] text-gray-400 font-bold uppercase tracking-widest">Saldo Bancário (Sienge API)</span>
                              {saldoBancario !== null ? (
                                <span className={cn("text-xl font-black", saldoBancario >= 0 ? "text-green-500" : "text-red-500")}>
                                  {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(saldoBancario)}
                                </span>
                              ) : (
                                <span className="text-xl font-black text-emerald-500 opacity-60">Em Sincronização...</span>
                              )}
                           </div>
                       </div>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 sm:gap-6">
                      <Card className="bg-[#161618] border-white/5 shadow-2xl relative group">
                        <CardHeader className="pt-4 pr-16">
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Total a Pagar</CardDescription>
                          <CardTitle className="text-2xl font-black text-white">
                            R$ {totalPayable.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </CardTitle>
                          <button onClick={() => setReportType('pagar')} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white rounded px-2 py-1 flex items-center gap-1 text-[9px] font-bold uppercase transition-colors"><FileText size={10}/> Relatório</button>
                        </CardHeader>
                      </Card>
                      <Card className="bg-[#161618] border-white/5 shadow-2xl relative group">
                        <CardHeader className="pt-4 pr-16">
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Total a Receber</CardDescription>
                          <CardTitle className="text-2xl font-black text-white">
                            R$ {totalReceivable.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </CardTitle>
                          <button onClick={() => setReportType('receber')} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white rounded px-2 py-1 flex items-center gap-1 text-[9px] font-bold uppercase transition-colors"><FileText size={10}/> Relatório</button>
                        </CardHeader>
                      </Card>
                      <Card className="bg-[#161618] border-white/5 shadow-2xl">
                        <CardHeader>
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Saldo Previsto</CardDescription>
                          <CardTitle className={cn("text-2xl font-black", (totalReceivable - totalPayable) >= 0 ? "text-green-500" : "text-red-500")}>
                            R$ {(totalReceivable - totalPayable).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </CardTitle>
                        </CardHeader>
                      </Card>
                      <Card className="bg-[#161618] border-white/5 shadow-2xl relative group">
                        <CardHeader className="pt-4 pr-16">
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Títulos em Aberto</CardDescription>
                          <CardTitle className="text-2xl font-black text-white">
                            {openPayables.length + openReceivables.length}
                          </CardTitle>
                          <button onClick={() => setReportType('abertos')} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white rounded px-2 py-1 flex items-center gap-1 text-[9px] font-bold uppercase transition-colors"><FileText size={10}/> Relatório</button>
                        </CardHeader>
                      </Card>
                    </div>

                    <div className="flex flex-col gap-6 sm:gap-8">
                      {/* CONTAS A RECEBER */}
                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[400px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-white/5">
                          <CardTitle className="text-lg font-black uppercase text-emerald-500">1. Contas a Receber</CardTitle>
                          <button onClick={() => setReportType('receber')} className="bg-white/5 hover:bg-white/10 text-white rounded-md px-3 py-1.5 flex items-center gap-2 text-xs font-bold uppercase transition-colors"><FileText size={14}/> Gerar Relatório</button>
                        </CardHeader>
                        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                          <Table>
                            <TableHeader className="bg-black/40 sticky top-0 z-10 backdrop-blur-md">
                              <TableRow className="border-white/5 hover:bg-transparent">
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-16">ID</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Cliente e Título</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-24">Previsto</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 text-right">Valor</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {openReceivables.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={4} className="text-center py-10 text-gray-500 font-bold">
                                    Nenhum título a receber pendente.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                openReceivables.slice(0, financeLimit).map((title, idx) => (
                                  <TableRow key={title.id || `rec-${idx}`} className="border-white/5 hover:bg-white/5 border-l-2 border-l-emerald-500/50">
                                    <TableCell className="text-xs font-mono text-gray-500">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-emerald-400 truncate max-w-[200px]" title={title.creditorName || title.customerName}>
                                        {title.creditorName || title.customerName || "Desconhecido"}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-400">
                                      {safeFormat(title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-white whitespace-nowrap">
                                      R$ {(title.amount || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                          {openReceivables.length > financeLimit && (
                            <div className="p-4 flex justify-center">
                              <Button variant="outline" onClick={() => setFinanceLimit(prev => prev + 100)} className="text-xs bg-white/5 border-white/10 text-white hover:bg-white/10">Carregar mais</Button>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* CONTAS A PAGAR */}
                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[400px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-white/5">
                          <CardTitle className="text-lg font-black uppercase text-orange-500">2. Contas a Pagar</CardTitle>
                          <button onClick={() => setReportType('pagar')} className="bg-white/5 hover:bg-white/10 text-white rounded-md px-3 py-1.5 flex items-center gap-2 text-xs font-bold uppercase transition-colors"><FileText size={14}/> Gerar Relatório</button>
                        </CardHeader>
                        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                          <Table>
                            <TableHeader className="bg-black/40 sticky top-0 z-10 backdrop-blur-md">
                              <TableRow className="border-white/5 hover:bg-transparent">
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-16">ID</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Credor e Título</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-24">Vencimento</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 text-right">Valor</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {openPayables.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={4} className="text-center py-10 text-gray-500 font-bold">
                                    Nenhum título a pagar pendente neste período.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                openPayables.slice(0, financeLimit).map((title, idx) => (
                                  <TableRow key={title.id || `pay-${idx}`} className="border-white/5 hover:bg-white/5 border-l-2 border-l-orange-500/50">
                                    <TableCell className="text-xs font-mono text-gray-500">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-gray-300 truncate max-w-[200px]" title={title.creditorName}>
                                        {title.creditorName}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-400">
                                      {safeFormat(title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-orange-500 whitespace-nowrap">
                                      R$ {(title.amount || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                          {openPayables.length > financeLimit && (
                            <div className="p-4 flex justify-center">
                              <Button variant="outline" onClick={() => setFinanceLimit(prev => prev + 100)} className="text-xs bg-white/5 border-white/10 text-white hover:bg-white/10">Carregar mais</Button>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* CONTAS PAGAS / BAIXADAS */}
                      <Card className="bg-[#111] border-white/5 shadow-inner flex flex-col h-[400px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-emerald-900/50">
                          <CardTitle className="text-lg font-black uppercase text-gray-400">3. Contas Pagas (Baixadas)</CardTitle>
                          <button onClick={() => setReportType('pagar')} className="bg-white/5 hover:bg-white/10 text-white rounded-md px-3 py-1.5 flex items-center gap-2 text-xs font-bold uppercase transition-colors"><FileText size={14}/> Gerar Relatório</button>
                        </CardHeader>
                        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                          <Table>
                            <TableHeader className="bg-black/40 sticky top-0 z-10 backdrop-blur-md">
                              <TableRow className="border-white/5 hover:bg-transparent">
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-16">ID</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Credor e Título</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Banco Pago</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-24">Pago Em</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 text-right">Valor Pago</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {paidPayables.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={5} className="text-center py-10 text-gray-500 font-bold">
                                    Nenhuma conta paga localizada.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                paidPayables.slice(0, financeLimit).map((title, idx) => (
                                  <TableRow key={title.id || `paid-${idx}`} className="border-white/5 hover:bg-white/5 border-l-2 border-l-gray-600/50 opacity-70 hover:opacity-100">
                                    <TableCell className="text-xs font-mono text-gray-500 line-through">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-gray-400 truncate max-w-[200px]" title={title.creditorName}>
                                        {title.creditorName}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-500">
                                       <span className="bg-white/5 px-2 py-0.5 rounded uppercase text-[9px] font-bold">Sistema / Caixa</span>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-500">
                                      {safeFormat(title.paymentDate || title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-gray-300 whitespace-nowrap">
                                      R$ {(title.amount || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                          {paidPayables.length > financeLimit && (
                            <div className="p-4 flex justify-center">
                              <Button variant="outline" onClick={() => setFinanceLimit(prev => prev + 100)} className="text-xs bg-white/5 border-white/10 text-gray-300 hover:bg-white/10 hover:text-white">Carregar mais</Button>
                            </div>
                          )}
                        </CardContent>
                      </Card>
                    </div>

                  </>
                );
              })()}
            </motion.div>

          )}
          
          {/* FLUXO DE CAIXA TAB */}
          {activeTab === 'financeiro-fluxo' && (
            <motion.div
              key="financeiro-fluxo"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-6"
            >
              {/* Filtros Exclusivos do Fluxo de Caixa */}
              <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl relative z-10 flex flex-wrap gap-4 items-end">
                <div className="space-y-2 flex-1 min-w-[260px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Período</Label>
                  <div className="flex items-center gap-2 bg-black/30 border border-white/10 rounded-xl p-1 h-11">
                    <button
                      onClick={() => {
                        setFcPeriodMode('last6m');
                        setFcStartDate(undefined);
                        setFcEndDate(undefined);
                      }}
                      className={cn(
                        "h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all",
                        fcPeriodMode === 'last6m'
                          ? "bg-orange-600 text-white"
                          : "text-gray-300 hover:text-white hover:bg-white/10"
                      )}
                    >
                      Últimos 6 meses
                    </button>
                    <button
                      onClick={() => {
                        setFcPeriodMode('all');
                        setFcStartDate(undefined);
                        setFcEndDate(undefined);
                      }}
                      className={cn(
                        "h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all",
                        fcPeriodMode === 'all'
                          ? "bg-sky-600 text-white"
                          : "text-gray-300 hover:text-white hover:bg-white/10"
                      )}
                    >
                      Período total
                    </button>
                  </div>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Inicial</Label>
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className={cn("w-full bg-black/40 border-white/10 h-11 rounded-xl justify-start text-left font-bold text-white", !fcStartDate && "text-gray-400")}>
                        <CalendarIcon className="mr-2 h-4 w-4" />
                        {fcStartDate ? format(fcStartDate, "dd/MM/yyyy") : <span>Selecione...</span>}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0 bg-[#161618] border-white/10 text-white" align="start">
                      <Calendar
                        mode="single"
                        selected={fcStartDate}
                        onSelect={(date) => {
                          if (!date) return;
                          setFcPeriodMode('last6m');
                          setFcStartDate(date);
                        }}
                        initialFocus
                        locale={ptBR}
                        className="bg-[#161618]"
                      />
                    </PopoverContent>
                  </Popover>
                </div>
                
                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Final</Label>
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className={cn("w-full bg-black/40 border-white/10 h-11 rounded-xl justify-start text-left font-bold text-white", !fcEndDate && "text-gray-400")}>
                        <CalendarIcon className="mr-2 h-4 w-4" />
                        {fcEndDate ? format(fcEndDate, "dd/MM/yyyy") : <span>Selecione...</span>}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0 bg-[#161618] border-white/10 text-white" align="start">
                      <Calendar
                        mode="single"
                        selected={fcEndDate}
                        onSelect={(date) => {
                          if (!date) return;
                          setFcPeriodMode('last6m');
                          setFcEndDate(date);
                        }}
                        initialFocus
                        locale={ptBR}
                        className="bg-[#161618]"
                      />
                    </PopoverContent>
                  </Popover>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Empresa (Sienge)</Label>
                  <Select value={fcSelectedCompany} onValueChange={(value) => {
                    setFcSelectedCompany(value);
                    setFcSelectedBuilding('all');
                  }}>
                    <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                      <span className="truncate">{fcSelectedCompany === 'all' ? 'Todas as Empresas' : fcSelectedCompanyName}</span>
                    </SelectTrigger>
                    <SelectContent className="bg-[#161618] border-white/10 text-white">
                      <SelectItem value="all">Todas as Empresas</SelectItem>
                       {companies.map((c) => (
                         <SelectItem key={`fc-empresa-${c.id}`} value={String(c.id)}>
                           {c.name} (ID: {c.id})
                         </SelectItem>
                       ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obras</Label>
                  <Select value={fcSelectedBuilding} onValueChange={setFcSelectedBuilding}>
                    <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                      <span className="truncate">{fcSelectedBuilding === 'all' ? 'Todas as Obras' : (buildingMap[fcSelectedBuilding] || `Obra ${fcSelectedBuilding}`)}</span>
                    </SelectTrigger>
                    <SelectContent className="bg-[#161618] border-white/10 text-white">
                      <SelectItem value="all">Todas as Obras</SelectItem>
                      {fcBuildingOptions.map((b) => (
                        <SelectItem key={`fc-obra-${b.id}`} value={String(b.id)}>
                          {b.name} (ID: {b.id})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Transf. Internas</Label>
                  <Button 
                    variant="outline" 
                    onClick={() => setFcHideInternal(!fcHideInternal)}
                    className={cn(
                      "w-full h-11 rounded-xl justify-center font-bold transition-all",
                      fcHideInternal 
                        ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/20" 
                        : "bg-black/40 border-white/10 text-gray-400 hover:bg-white/5 hover:text-white"
                    )}
                  >
                    {fcHideInternal ? (
                      <><CheckCircle2 className="mr-2 h-4 w-4" /> Ocultas</>
                    ) : (
                      <><RefreshCw className="mr-2 h-4 w-4" /> Visíveis</>
                    )}
                  </Button>
                </div>

                <div className="flex items-end gap-2 min-w-[220px]">
                  <Button
                    variant="outline"
                    onClick={() => {
                      setFcPeriodMode('last6m');
                      setFcStartDate(undefined);
                      setFcEndDate(undefined);
                      setFcSelectedCompany('all');
                      setFcSelectedBuilding('all');
                      setFcHideInternal(false);
                    }}
                    className="h-11 rounded-xl border-white/10 bg-black/30 text-gray-300 hover:bg-white/10 hover:text-white"
                  >
                    Limpar Filtros
                  </Button>
                  <Button
                    onClick={() => setDataRevision((prev) => prev + 1)}
                    disabled={loading}
                    className={cn(
                      "h-11 rounded-xl font-bold",
                      isDark ? "bg-[#1B3C58] hover:bg-[#234b6e]" : "bg-[#102A40] hover:bg-[#173A57]"
                    )}
                  >
                    <Filter size={15} className="mr-2" />
                    {loading ? 'Filtrando...' : 'Filtrar'}
                  </Button>
                </div>
              </div>

              {/* Tabela do Fluxo de Caixa */}
              <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[600px]">
                <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-white/5">
                  <div className="flex flex-col">
                    <CardTitle className="text-xl font-black uppercase text-white flex items-center gap-2">
                      <FileText className="text-orange-500" size={20} /> Fluxo de Caixa (Extrato/Razão)
                    </CardTitle>
                    <CardDescription className="text-gray-400 text-xs mt-1">
                      Cruzamento das Contas a Pagar e Receber projetando o saldo cumulativo
                    </CardDescription>
                  </div>
                </CardHeader>
                <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                  <Table>
                    <TableHeader className="bg-black/60 sticky top-0 z-10 backdrop-blur-md">
                      <TableRow className="border-white/5 hover:bg-transparent">
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 w-24">Data</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 w-28">Tit/Parc</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 w-12">Orig.</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 w-32">Conta</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 w-28">Tp. Lanç.</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Cliente/Fornecedor</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right w-32">Entradas (R$)</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right w-32">Saídas (R$)</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right w-36">Saldo (R$)</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {fluxoDeCaixaData.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={9} className="text-center py-10 text-gray-500 font-bold">
                            Nenhuma movimentação encontrada para o período e empresa selecionados. Use "Limpar Filtros" para exibir todo o extrato.
                          </TableCell>
                        </TableRow>
                      ) : (
                        fluxoDeCaixaData.map((row, idx) => (
                          <TableRow
                            key={`fc-${idx}-${row.id}`}
                            className={cn(
                              "border-white/5 hover:bg-white/5 transition-colors",
                              row.entrada > 0 ? "border-l-2 border-l-emerald-600/30" : "border-l-2 border-l-red-600/20"
                            )}
                          >
                            <TableCell className="text-xs text-gray-400 whitespace-nowrap">
                              {safeFormat(row.data, 'dd/MM/yyyy')}
                            </TableCell>
                            <TableCell className="text-xs font-mono text-blue-300 whitespace-nowrap" title={(row as any).statementType}>
                              {(row as any).titParc || row.documento}
                            </TableCell>
                            <TableCell>
                              <Badge variant="outline" className={cn(
                                "text-[9px] uppercase border-white/10 px-1 py-0",
                                (row as any).origem === 'CX' ? "text-yellow-400 bg-yellow-500/10" :
                                (row as any).origem === 'BC' ? "text-sky-400 bg-sky-500/10" :
                                (row as any).origem === 'GE' ? "text-purple-400 bg-purple-500/10" :
                                (row as any).origem === 'CP' ? "text-orange-400 bg-orange-500/10" :
                                (row as any).origem === 'AC' ? "text-pink-400 bg-pink-500/10" :
                                "text-gray-400 bg-white/5"
                              )}>
                                {(row as any).origem || '—'}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-xs text-gray-300 whitespace-nowrap truncate max-w-[120px]" title={(row as any).bankAccount}>
                              {(row as any).bankAccount || '—'}
                            </TableCell>
                            <TableCell className="text-[10px] text-gray-400 whitespace-nowrap truncate max-w-[120px]" title={(row as any).statementType}>
                              {(row as any).statementType}
                            </TableCell>
                            <TableCell className="text-xs font-bold text-gray-200 truncate max-w-[220px]" title={row.pessoa}>
                              {row.pessoa}
                            </TableCell>
                            <TableCell className={cn(
                              'text-right font-mono whitespace-nowrap',
                              row.entrada > 0 ? 'text-emerald-400' :
                              row.entrada < 0 ? 'text-orange-400' : 'text-gray-600'
                            )}>
                              {row.entrada !== 0
                                ? (row.entrada < 0 ? '-' : '') + Math.abs(row.entrada).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})
                                : <span className="text-gray-600">—</span>}
                            </TableCell>
                            <TableCell className="text-right font-mono text-red-400 whitespace-nowrap">
                              {row.saida > 0
                                ? row.saida.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})
                                : <span className="text-gray-600">—</span>}
                            </TableCell>
                            <TableCell className={cn(
                              "text-right font-black font-mono whitespace-nowrap",
                              row.saldo >= 0 ? "text-emerald-400" : "text-red-400"
                            )}>
                              {row.saldo < 0 ? '-' : ''}{Math.abs(row.saldo).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                            </TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </CardContent>
                {/* Resumo Rodapé */}
                <div className="bg-black/40 border-t border-white/5 p-4 flex justify-between items-center text-sm">
                   <div className="flex gap-6">
                      <div className="flex flex-col">
                        <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Saldo Anterior</span>
                        <span className={cn('font-mono font-black', fluxoDeCaixaSaldoAnterior >= 0 ? 'text-sky-400' : 'text-orange-400')}>
                          {fluxoDeCaixaSaldoAnterior < 0 ? '-' : ''}R$ {Math.abs(fluxoDeCaixaSaldoAnterior).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                        </span>
                      </div>
                      <div className="flex flex-col">
                        <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Total Entradas</span>
                        {/* Soma com sinal: estornos negativos reduzem total (igual ao PDF Sienge) */}
                        <span className="font-mono text-emerald-400 font-black">R$ {fluxoDeCaixaData.reduce((acc, r) => acc + r.entrada, 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>
                      </div>
                      <div className="flex flex-col">
                        <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Total Saídas</span>
                        <span className="font-mono text-red-400 font-black">R$ {fluxoDeCaixaData.reduce((acc, r) => acc + r.saida, 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>
                      </div>
                      <div className="flex flex-col">
                        <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Registros</span>
                        <span className="font-mono text-gray-300 font-black">{fluxoDeCaixaData.length.toLocaleString('pt-BR')}</span>
                      </div>
                   </div>
                   <div className="flex flex-col text-right">
                      <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Saldo Acumulado do Período</span>
                      {(() => {
                        const lastSaldo = fluxoDeCaixaData.length > 0 ? fluxoDeCaixaData[fluxoDeCaixaData.length - 1].saldo : fluxoDeCaixaSaldoAnterior;
                        return (
                          <span className={cn('text-xl font-mono font-black', lastSaldo >= 0 ? 'text-emerald-500' : 'text-red-500')}>
                            {lastSaldo < 0 ? '-' : ''}R$ {Math.abs(lastSaldo).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </span>
                        );
                      })()}
                   </div>
                </div>
              </Card>
            </motion.div>
          )}

          {/* LEANDRO TAB */}
          {activeTab === 'financeiro-leandro' && (
            <LeandroTab
              isDark={isDark}
              allFinancialTitles={allFinancialTitles}
              allReceivableTitles={allReceivableTitles}
              orders={orders}
              buildings={buildings}
              companies={companies}
              syncing={syncing}
              syncSienge={syncSienge}
            />
          )}

          {activeTab === 'obras-diario' && (
            <motion.div
              key="obras-diario"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="w-full min-h-[400px]"
            >
              <FluxoProjection
                allFinancialTitles={allFinancialTitles}
                allReceivableTitles={allReceivableTitles}
                buildings={buildings}
                companies={companies}
                syncing={syncing}
                syncSienge={syncSienge}
              />
            </motion.div>
          )}

          {activeTab === 'map' && (
            <motion.div
              key="map"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="grid grid-cols-1 lg:grid-cols-4 gap-4 sm:gap-6 min-h-[400px] sm:h-[600px]"
            >
              {/* 1. Lista de Obras */}
              <Card className="lg:col-span-1 bg-[#161618] border-white/5 shadow-2xl flex flex-col h-full">
                <CardHeader className="pb-4">
                  <CardTitle className="text-white font-black uppercase text-sm tracking-tight">Obras Ativas</CardTitle>
                  <CardDescription className="text-xs">
                    {(buildingOptions.filter(b => b.name.toLowerCase().includes(buildingSearch.toLowerCase()) || String(b.id).includes(buildingSearch)) || []).length} encontradas
                  </CardDescription>
                  <div className="mt-3 relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={14} />
                    <input 
                      type="text" 
                      placeholder="Pesquisar obra..." 
                      className="w-full bg-black/40 border border-white/10 rounded-lg py-2 pl-9 pr-3 text-xs text-white placeholder:text-gray-600 focus:outline-none focus:border-orange-500/50"
                      value={buildingSearch}
                      onChange={(e) => setBuildingSearch(e.target.value)}
                    />
                  </div>
                </CardHeader>
                <CardContent className="flex-1 overflow-y-auto px-2 pb-4 space-y-1 custom-scrollbar">
                  {(() => {
                    const filtered = buildingOptions.filter(b =>
                      b.name.toLowerCase().includes(buildingSearch.toLowerCase()) ||
                      String(b.id).includes(buildingSearch)
                    );
                    // Compute volume per building from filtered orders for sorting
                    const buildingVolume: Record<string, number> = {};
                    orders.forEach(o => {
                      const key = String(o.buildingId);
                      buildingVolume[key] = (buildingVolume[key] || 0) + (o.totalAmount || 0);
                    });
                    return filtered
                      .sort((a, b) => (buildingVolume[String(b.id)] || 0) - (buildingVolume[String(a.id)] || 0))
                      .map(b => {
                        const vol = buildingVolume[String(b.id)] || 0;
                        return (
                          <button
                            key={b.id}
                            onClick={() => { setSelectedMapBuilding(b.id); setEditingEngineer(false); }}
                            className={cn(
                              "w-full text-left p-3 rounded-xl transition-all border text-xs font-bold",
                              selectedMapBuilding === b.id
                                ? "bg-orange-600/20 border-orange-500/50 text-orange-500"
                                : "bg-black/20 border-white/5 text-gray-400 hover:bg-white/5 hover:text-white"
                            )}
                          >
                            <div className="truncate mb-1 text-sm">{b.name}</div>
                            <div className="flex items-center justify-between">
                              <div className="text-[9px] text-gray-500 uppercase flex items-center gap-1">
                                <MapIcon size={10} /> ID: {b.id}
                              </div>
                              {vol > 0 && (
                                <div className="text-[9px] font-black text-orange-500/70">
                                  R$&nbsp;{vol.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}
                                </div>
                              )}
                            </div>
                          </button>
                        );
                    });
                  })()}
                </CardContent>
              </Card>

              {/* 2. Mapa Google */}
              <Card className="lg:col-span-2 bg-[#161618] border-white/5 shadow-2xl relative overflow-hidden p-0 h-full">
                {(() => {
                  const currentBuilding = buildings.find(b => b.id === selectedMapBuilding);
                  if (!currentBuilding) {
                    return (
                      <div className="flex flex-col items-center justify-center h-full text-gray-500 bg-[#0a0a0b]">
                        <MapIcon size={48} className="mb-4 opacity-20" />
                        <p className="font-bold text-sm">Selecione uma obra na lista para visualizar o mapa</p>
                      </div>
                    );
                  }

                  const query = currentBuilding.address || currentBuilding.name;

                  return (
                    <iframe 
                      width="100%" 
                      height="100%" 
                      frameBorder="0" 
                      scrolling="no" 
                      marginHeight={0} 
                      marginWidth={0} 
                      src={`https://maps.google.com/maps?q=${encodeURIComponent(query)}&t=m&z=14&output=embed`}
                      style={{ filter: "invert(90%) hue-rotate(180deg) brightness(80%) contrast(120%)" }}
                      title="Google Maps"
                    ></iframe>
                  );
                })()}
              </Card>

              {/* 3. Resumo Financeiro */}
              <Card className="lg:col-span-1 bg-[#161618] border-white/5 shadow-2xl h-full overflow-y-auto">
                <CardHeader className="pb-4">
                  <CardTitle className="text-white font-black uppercase text-sm tracking-tight leading-tight">
                    {selectedMapBuilding
                      ? buildings.find(b => b.id === selectedMapBuilding)?.name
                      : "Resumo da Obra"}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {selectedMapBuilding ? (
                    <div className="space-y-5">
                      {(() => {
                        const currentBuilding = buildings.find(b => b.id === selectedMapBuilding);
                        // Use ALL orders (no date filter) for accurate building totals
                        const buildingOrders = orders.filter(o => String(o.buildingId) === String(selectedMapBuilding));
                        // Financial titles sem vínculo de obra podem vir zerados; aqui somamos apenas os vinculados.
                        const buildingPayable = financialTitles.filter(f => {
                          if (String(f.buildingId) === String(selectedMapBuilding)) return true;
                          return false;
                        });
                        const openPayableBuilding = buildingPayable.filter(
                          f => f.status !== 'BAIXADO' && f.status !== 'PAGO' && f.status !== 'LIQUIDADO'
                        );

                        const totalOrders = buildingOrders.reduce((acc, curr) => acc + (curr.totalAmount || 0), 0);
                        const totalPayable = openPayableBuilding.reduce((acc, curr) => acc + (curr.amount || 0), 0);

                        const saveEngineer = async () => {
                          if (!currentBuilding) return;
                          setSavingEngineer(true);
                          try {
                            await api.post('/obras/meta', { id: currentBuilding.id, engineer: engineerDraft });
                            // Update local state
                            setBuildings(prev => prev.map(b =>
                              b.id === currentBuilding.id ? { ...b, engineer: engineerDraft } : b
                            ));
                            setEditingEngineer(false);
                          } catch (e) {
                            console.error('Erro ao salvar engenheiro', e);
                          } finally {
                            setSavingEngineer(false);
                          }
                        };

                        return (
                          <>
                            {/* Responsável Técnico */}
                            <div className="bg-black/20 p-4 rounded-xl border border-white/5">
                              <div className="flex items-start gap-3">
                                <div className="w-9 h-9 rounded-full bg-orange-500/20 flex items-center justify-center shrink-0 mt-0.5">
                                  <UserIcon size={16} className="text-orange-500" />
                                </div>
                                <div className="min-w-0 flex-1">
                                  <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Responsável Técnico</p>
                                  {editingEngineer ? (
                                    <div className="flex flex-col gap-2">
                                      <input
                                        autoFocus
                                        value={engineerDraft}
                                        onChange={e => setEngineerDraft(e.target.value)}
                                        onKeyDown={e => { if(e.key === 'Enter') saveEngineer(); if(e.key === 'Escape') setEditingEngineer(false); }}
                                        className="bg-black/60 border border-orange-500/40 rounded-lg px-3 py-1.5 text-sm text-white w-full focus:outline-none focus:border-orange-500"
                                        placeholder="Nome do responsável"
                                      />
                                      <div className="flex gap-2">
                                        <button
                                          onClick={saveEngineer}
                                          disabled={savingEngineer}
                                          className="flex-1 bg-orange-600 hover:bg-orange-700 text-white text-xs font-black py-1.5 rounded-lg transition-colors disabled:opacity-50"
                                        >
                                          {savingEngineer ? 'Salvando...' : 'Salvar'}
                                        </button>
                                        <button
                                          onClick={() => setEditingEngineer(false)}
                                          className="flex-1 bg-white/5 hover:bg-white/10 text-gray-400 text-xs font-bold py-1.5 rounded-lg transition-colors"
                                        >
                                          Cancelar
                                        </button>
                                      </div>
                                    </div>
                                  ) : (
                                    <div className="flex items-center justify-between gap-2">
                                      <p className="text-sm font-bold text-white leading-tight truncate">
                                        {currentBuilding?.engineer || 'Não definido'}
                                      </p>
                                      {isAdmin && (
                                        <button
                                          onClick={() => { setEngineerDraft(currentBuilding?.engineer || ''); setEditingEngineer(true); }}
                                          className="shrink-0 text-[9px] font-black uppercase text-orange-500/70 hover:text-orange-500 border border-orange-500/20 hover:border-orange-500/50 px-2 py-1 rounded-md transition-colors"
                                        >
                                          Editar
                                        </button>
                                      )}
                                    </div>
                                  )}
                                </div>
                              </div>
                            </div>

                            <div className="space-y-3">
                              <div className="bg-black/20 p-4 rounded-xl border border-white/5">
                                <p className="text-[10px] font-black text-gray-500 uppercase mb-1">Volume de Compras</p>
                                <p className="text-2xl font-black text-orange-500 leading-tight">R$ {totalOrders.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</p>
                                <p className="text-[10px] text-gray-600 mt-1">{buildingOrders.length} pedidos em todo o histórico</p>
                              </div>

                              <div className="bg-black/20 p-4 rounded-xl border border-white/5">
                                <p className="text-[10px] font-black text-gray-500 uppercase mb-1">Pendente a Pagar</p>
                                <p className="text-2xl font-black text-red-500 leading-tight">R$ {totalPayable.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</p>
                                {buildingPayable.length === 0 && (
                                  <p className="text-[9px] text-gray-600 mt-1">Títulos financeiros sem vínculo de obra</p>
                                )}
                              </div>
                            </div>
                          </>
                        );
                      })()}
                    </div>
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-600 text-xs py-10">
                      Nenhuma obra selecionada
                    </div>
                  )}
                </CardContent>
              </Card>
            </motion.div>
          )}

          {activeTab === 'logistics' && (
            <motion.div
              key="logistics"
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full"
            >
              <LogisticsTab buildings={buildings} readOnly={isRestrictedUser} />
            </motion.div>
          )}

          {activeTab === 'access' && (
            <motion.div
              key="access"
              initial={{ opacity: 0, scale: 0.95 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="w-full"
            >
              <AccessControlTab />
            </motion.div>
          )}

        </AnimatePresence>
      </main>

      <footer className="mt-20 border-t border-white/5 bg-[#161618] py-12">
        <div className="container mx-auto px-6 flex flex-col md:flex-row justify-between items-center gap-8">
          <div className="flex items-center gap-4">
            <div className="w-10 h-10 bg-white/5 rounded-xl flex items-center justify-center">
              <Building2 size={20} className="text-gray-500" />
            </div>
            <div>
              <p className="text-sm font-black text-white uppercase tracking-tighter">Dinamica Dashboard</p>
              <p className="text-[10px] font-bold text-gray-600 uppercase tracking-widest">Sienge ERP Integration v2.0</p>
            </div>
          </div>
          <div className="flex gap-8 items-center">
            <div className="text-right">
              <p className="text-[10px] font-black text-gray-600 uppercase mb-1">Última Sincronização</p>
              <p className="text-xs font-bold text-gray-400">{format(lastUpdate, "HH:mm:ss")}</p>
              <p className="text-[10px] font-bold text-green-500 uppercase tracking-widest mt-1">
                {syncInfo?.status === 'success' ? 'Sincronizado' : syncing ? 'Sincronizando' : 'Aguardando sync'}
              </p>
            </div>
            <div className="h-10 w-px bg-white/5" />
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                className={cn(
                  "h-12 px-6 rounded-xl border-white/5 font-bold transition-all",
                  apiStatus === 'online' ? "bg-green-500/10 text-green-500 border-green-500/20" : 
                  apiStatus === 'offline' ? "bg-red-500/10 text-red-500 border-red-500/20" :
                  "bg-orange-500/10 text-orange-500 border-orange-500/20"
                )}
              >
                {apiStatus === 'online' ? <Wifi size={18} className="mr-2" /> : 
                 apiStatus === 'offline' ? <WifiOff size={18} className="mr-2" /> :
                 <RefreshCw size={18} className="mr-2 animate-spin" />}
                {apiStatus === 'online' ? "Sienge Conectado" : 
                 apiStatus === 'offline' ? "Sienge Desconectado" : 
                 "Verificando..."}
              </Button>

              {apiStatus === 'online' && (
                <Button
                  onClick={downloadData}
                  className="h-12 px-6 bg-white text-black hover:bg-gray-200 font-bold rounded-xl flex items-center gap-2"
                >
                  <Download size={18} />
                  Baixar Dados
                </Button>
              )}
            </div>
          </div>
        </div>
      </footer>

      {/* Global New Order Alert Popup */}
      <AnimatePresence>
        {newOrderAlert && (
          <motion.div
            initial={{ opacity: 0, y: 50, x: 50 }}
            animate={{ opacity: 1, y: 0, x: 0 }}
            exit={{ opacity: 0, scale: 0.9, y: 50, x: 50 }}
            className="fixed bottom-6 right-6 z-[9999] bg-gradient-to-br from-orange-600 to-orange-800 p-6 rounded-2xl shadow-[0_20px_50px_rgba(234,88,12,0.3)] border border-white/10 flex items-start gap-4 max-w-sm cursor-pointer"
            onClick={() => setSelectedAlertOrder(newOrderAlert)}
          >
            <div className="bg-white/20 p-3 rounded-xl shadow-inner shrink-0">
              <Package className="text-white" size={28} />
            </div>
            <div className="pr-4">
              <h4 className="text-white font-black tracking-wide">NOVA COMPRA REGISTRADA</h4>
              <p className="text-orange-100 text-sm mt-1 leading-snug">Pedido <span className="font-bold">#{newOrderAlert.id}</span> processado no valor de <span className="font-bold">R$ {newOrderAlert.totalAmount.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</span>.</p>
              <p className="text-orange-300 text-xs mt-2 font-bold uppercase tracking-wider">{buildings.find(b => b.id === newOrderAlert.buildingId)?.name || 'Obra não identificada'}</p>
            </div>
            <button onClick={(e) => { e.stopPropagation(); setNewOrderAlert(null); }} className="absolute top-4 right-4 text-white/50 hover:text-white transition-colors">
              &times;
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </div>

    {reportType && (
      <div className="fixed inset-0 z-[100] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
        <div className="bg-[#161618] rounded-2xl border border-white/10 shadow-2xl w-full max-w-5xl h-[90vh] flex flex-col print:h-auto print:max-h-none print:shadow-none print:bg-white print:border-none print:w-full print:max-w-none">
          <div className="flex justify-between items-center p-6 border-b border-white/5 print:hidden">
            <h2 className="text-xl font-black text-white uppercase">
              {reportType === 'pagar' && 'Relatório: Total a Pagar (Filtrado)'}
              {reportType === 'receber' && 'Relatório: Total a Receber (Filtrado)'}
              {reportType === 'abertos' && 'Relatório: Títulos em Aberto (Filtrado)'}
            </h2>
            <div className="flex items-center gap-4">
              <Button onClick={() => window.print()} className="bg-orange-600 hover:bg-orange-700 text-white gap-2 font-bold focus:ring-0">
                <Printer size={16} /> Imprimir
              </Button>
              <button onClick={() => setReportType(null)} className="text-gray-400 hover:text-white p-2">
                <X size={24} />
              </button>
            </div>
          </div>
          
          <div className="flex-1 overflow-y-auto p-6 custom-scrollbar print:p-0 print:overflow-visible">
            {/* Imprimindo a tabela formatada do Modal */}
            <div className="print:block print:w-full">
              <h2 className="hidden print:block text-2xl font-black mb-6 text-black uppercase">
                 {reportType === 'pagar' && 'Relatório: Total a Pagar'}
                 {reportType === 'receber' && 'Relatório: Total a Receber'}
                 {reportType === 'abertos' && 'Relatório: Títulos em Aberto'}
              </h2>
              <Table className="print:text-black">
                <TableHeader className="bg-black/20 print:bg-gray-100 print:relative">
                  <TableRow className="border-white/5 print:border-gray-300">
                    <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black w-20">ID</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Tipo / Referência</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Pessoa Envolvida</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black w-24">Vencimento</TableHead>
                    <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black">Valor</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(() => {
                    const cMap: Record<string, string> = {};
                    creditors.forEach(c => cMap[c.id] = c.name);
                    const listPagar = financialTitles.filter(t => t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO');
                    const listReceber = receivableTitles.filter(t => t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO');
                    
                    let items: any[] = [];
                    if (reportType === 'pagar') items = listPagar.map(i => ({...i, _kind: 'pagar'}));
                    if (reportType === 'receber') items = listReceber.map(i => ({...i, _kind: 'receber'}));
                    if (reportType === 'abertos') {
                      items = [
                        ...listPagar.map(i => ({...i, _kind: 'pagar'})),
                        ...listReceber.map(i => ({...i, _kind: 'receber'}))
                      ];
                    }
                    
                    items.sort((a,b) => (b.dueDateNumeric || 0) - (a.dueDateNumeric || 0));

                    if (items.length === 0) {
                      return (
                        <TableRow>
                          <TableCell colSpan={5} className="text-center py-10 font-bold text-gray-500">Nenhum título encontrado.</TableCell>
                        </TableRow>
                      );
                    }

                    return items.map((item, idx) => (
                      <TableRow key={`rep-${idx}`} className="border-white/5 print:border-gray-200">
                        <TableCell className="font-mono text-xs">{item.id}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className={item._kind === 'pagar' ? 'text-orange-500 border-orange-500/20 mr-2 text-[9px]' : 'text-green-500 border-green-500/20 mr-2 text-[9px]'}>
                            {item._kind === 'pagar' ? 'PAGAR' : 'RECEBER'}
                          </Badge>
                          <span className="text-xs text-gray-300 print:text-gray-800">{item.description}</span>
                        </TableCell>
                        <TableCell className="font-bold text-xs truncate max-w-[200px]">
                          {item._kind === 'pagar' ? (item.creditorName || cMap[item.id] || "N/A") : (item.clientName || "N/A")}
                        </TableCell>
                        <TableCell className="text-xs">{safeFormat(item.dueDate)}</TableCell>
                        <TableCell className="text-right font-black whitespace-nowrap">
                          R$ {(item.amount || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                        </TableCell>
                      </TableRow>
                    ));
                  })()}
                </TableBody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    )}
    {selectedAlertOrder && (
      <div className="fixed inset-0 z-[110] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4" onClick={() => setSelectedAlertOrder(null)}>
        <div className="bg-[#161618] rounded-2xl border border-white/10 shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
          <div className="flex items-center justify-between p-6 border-b border-white/5">
            <div>
              <h2 className="text-xl font-black text-white uppercase">Detalhes da Compra</h2>
              <p className="text-sm text-gray-400 mt-1">Pedido #{selectedAlertOrder.id}</p>
            </div>
            <button onClick={() => setSelectedAlertOrder(null)} className="text-gray-400 hover:text-white p-2">
              <X size={24} />
            </button>
          </div>
          <div className="p-6 space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Obra</p>
                <p className="text-white font-bold">{resolveBuildingName(selectedAlertOrder)}</p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Fornecedor</p>
                <p className="text-white font-bold">{resolveCreditorName(selectedAlertOrder)}</p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Comprador</p>
                <p className="text-white font-bold">
                  {resolveUserName(selectedAlertOrder.buyerId, selectedAlertOrder.createdBy || 'N/A')}
                </p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Solicitante</p>
                <p className="text-white font-bold">
                  {selectedAlertOrder.requesterId && selectedAlertOrder.requesterId !== '0'
                    ? selectedAlertOrder.requesterId
                    : selectedAlertOrder.createdBy || 'N/A'}
                </p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Data</p>
                <p className="text-white font-bold">{safeFormat(selectedAlertOrder.date)}</p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Entrega</p>
                <p className="text-white font-bold">{safeFormat(selectedAlertOrder.deliveryDate)}</p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Status</p>
                <p className="text-white font-bold">{selectedAlertOrder.status || 'N/A'}</p>
              </div>
              <div className="bg-black/20 rounded-xl border border-white/5 p-4">
                <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Valor Total</p>
                <p className="text-orange-500 font-black text-lg">
                  {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(selectedAlertOrder.totalAmount || 0)}
                </p>
              </div>
            </div>

            <div className="bg-black/20 rounded-xl border border-white/5 p-4">
              <p className="text-[10px] font-black uppercase text-gray-500 mb-2">Condição de Pagamento</p>
              <p className="text-white font-bold">{selectedAlertOrder.paymentCondition || 'N/A'}</p>
            </div>

            <div className="bg-black/20 rounded-xl border border-white/5 p-4">
              <p className="text-[10px] font-black uppercase text-gray-500 mb-2">Observações</p>
              <p className="text-gray-300 leading-relaxed">
                {selectedAlertOrder.internalNotes || 'Nenhuma observação informada.'}
              </p>
            </div>
          </div>
        </div>
      </div>
    )}
    </>
  );
}



