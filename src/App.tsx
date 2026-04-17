import { useState, useEffect, useMemo, useCallback, useRef, Fragment } from 'react';
import { 
  LayoutDashboard, Bell, Filter, Download, TrendingUp, TrendingDown, 
  DollarSign, Package, Calendar as CalendarIcon, RefreshCw, 
  User as UserIcon, Building2, ChevronRight, Search, Map as MapIcon,
  Wifi, WifiOff, CheckCircle2, AlertCircle, FileText, Printer, X,
  Menu, ChevronDown, SlidersHorizontal, Truck, LogOut
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { LogisticsTab } from './components/LogisticsTab';
import { AccessControlTab } from './components/AccessControl';
import { LoginScreen } from './components/LoginScreen';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button, buttonVariants } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { parseISO, format, addDays } from 'date-fns';
import { ptBR } from 'date-fns/locale';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  AreaChart, Area, Cell, PieChart, Pie, LineChart, Line, Legend
} from 'recharts';
import { api, Building, User, Creditor, PurchaseOrder, PriceAlert, type AuthUser } from './lib/api';
import { cn } from './lib/utils';
import { fixText } from './lib/text';

export default function App() {
  const [sessionUser, setSessionUser] = useState<AuthUser | null>(null);
  const [authReady, setAuthReady] = useState(false);
  const [activeTab, setActiveTab] = useState<'dashboard' | 'alerts' | 'map' | 'finance' | 'logistics' | 'access'>('dashboard');
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [apiStatus, setApiStatus] = useState<'online' | 'offline' | 'checking'>('checking');
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  const [startDate, setStartDate] = useState<Date | undefined>();
  const [endDate, setEndDate] = useState<Date | undefined>(new Date());
  const [isPrinting, setIsPrinting] = useState(false);
  const [newOrderAlert, setNewOrderAlert] = useState<PurchaseOrder | null>(null);
  const [selectedAlertOrder, setSelectedAlertOrder] = useState<PurchaseOrder | null>(null);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const knownOrderIdsRef = useRef<Set<number>>(new Set());

  type ReportType = 'pagar' | 'receber' | 'abertos' | null;
  const [reportType, setReportType] = useState<ReportType>(null);
  
  const [alertSortConfig, setAlertSortConfig] = useState<{ key: 'date' | 'vlrUnit' | 'vlrAtual' | 'valorTotal', direction: 'asc' | 'desc' } | null>(null);
  const isRestrictedUser = sessionUser?.role === 'user';

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
  const [latestPricesMap, setLatestPricesMap] = useState<Record<string, number>>({});
  const [baselinePricesMap, setBaselinePricesMap] = useState<Record<string, number>>({});
  const requestedItemsRef = useRef<Set<string>>(new Set());
  
  // Reactivity: Auto-update price alerts whenever itemsDetailsMap or orders change
  useEffect(() => {
    if (orders.length > 1) {
      const alerts: PriceAlert[] = [];
      const itemHistory: Record<string, { price: number, date: string }[]> = {};
      
      orders.forEach(order => {
        const actualItems = itemsDetailsMap[order.id] || order.items;
        if (actualItems) {
          actualItems.forEach((item: any) => {
            const name = item.description || item.resourceDescription || item.descricao;
            const price = Number(item.unitPrice || item.valorUnitario || item.netPrice || 0);
            if (name && price > 0) {
              if (!itemHistory[name]) itemHistory[name] = [];
              itemHistory[name].push({ price, date: order.date });
            }
          });
        }
      });

      Object.keys(itemHistory).forEach(name => {
        const history = itemHistory[name].sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
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
  }, [orders, itemsDetailsMap]);
  
  // Selection State for Map
  const [selectedMapBuilding, setSelectedMapBuilding] = useState<number | null>(null);
  const [buildingSearch, setBuildingSearch] = useState('');
  const [editingEngineer, setEditingEngineer] = useState(false);
  const [engineerDraft, setEngineerDraft] = useState('');
  const [savingEngineer, setSavingEngineer] = useState(false);

  const isAdmin = sessionUser?.role === 'developer' || sessionUser?.role === 'admin';

  // Filter State
  const [selectedBuilding, setSelectedBuilding] = useState<string>('all');
  const [selectedUser, setSelectedUser] = useState<string>('all');
  const [selectedRequester, setSelectedRequester] = useState<string>('all');

  const safeFormat = (dateStr: string | undefined, formatStr: string = 'dd/MM/yyyy') => {
    if (!dateStr || dateStr === '---') return '---';
    try {
      const d = parseISO(dateStr);
      if (isNaN(d.getTime())) return '---';
      return format(d, formatStr);
    } catch {
      return '---';
    }
  };

  const checkConnection = useCallback(async () => {
    try {
      const response = await api.get('/test');
      const hasCache = Boolean(response.data?.ok || response.data?.cache?.pedidos || response.data?.cache?.financeiro || response.data?.cache?.receber);
      setApiStatus(hasCache ? 'online' : 'offline');
      return hasCache;
    } catch (error) {
      console.error('Connection test failed:', error);
      setApiStatus('offline');
      return false;
    }
  }, []);

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

    setBuildings(bData);
    setUsers(uData);
    setCreditors(cData);
    setCompanies(compDataRaw);

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
        description: fixText(f.descricao || f.historico || f.tipoDocumento || f.notes || f.observacao || 'Título a Pagar'),
        creditorName: fixText(f.nomeCredor || f.creditorName || f.nomeFantasiaCredor || f.fornecedor || f.credor || 'Credor sem nome'),
        _rawCreditorId: String(f.creditorId || f.debtorId || ''),
        dueDate: dStr,
        dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
        amount: parseFloat(f.totalInvoiceAmount || f.valor || f.amount || f.valorTotal || f.valorLiquido || f.valorBruto) || 0,
        status: f.situacao || f.status || 'Pendente',
      };
    });

    const allRData = rDataRaw.map((r: any) => {
      const dStr = r.data || r.date || r.dataVencimento || r.dataEmissao || r.issueDate || r?.dataVencimentoProjetado || '---';
      const d = parseISO(dStr);
      return {
        id: r.id || r.numero || r.numeroTitulo || r.codigoTitulo || r.documentNumber || 0,
        buildingId: r.idObra || r.codigoObra || r.buildingId || 0,
        description: fixText(r.descricao || r.historico || r.observacao || r.notes || r.description || 'Título a Receber'),
        clientName: fixText(r.nomeCliente || r.nomeFantasiaCliente || r.cliente || r.clientName || 'Extrato/Cliente'),
        dueDate: dStr,
        dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
        amount: parseFloat(r.value || r.valor || r.valorSaldo || r.totalInvoiceAmount || r.valorTotal || r.amount) || 0,
        status: String(r.situacao || r.status || 'ABERTO').toUpperCase(),
      };
    });

    setItemsDetailsMap(payload?.itensPedidos || {});
    setAllOrders(allOData);
    setAllFinancialTitles(allFData);
    setAllReceivableTitles(allRData);
    setLastUpdate(new Date());
    setApiStatus('online');
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
      applyBootstrapData(response.data);
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
  }, [applyBootstrapData, checkConnection]);

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
      await api.post('/sync');
      await refreshData();
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
      applyBootstrapData(response.data);
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

  const dateRange = useMemo(() => {
    const start = startDate ? toStartOfDay(startDate) : null;
    const effectiveEndDate = endDate || startDate || null;
    const endExclusive = effectiveEndDate ? addDays(new Date(
      effectiveEndDate.getFullYear(),
      effectiveEndDate.getMonth(),
      effectiveEndDate.getDate()
    ), 1).getTime() : null;
    return { start, endExclusive };
  }, [endDate, startDate, toStartOfDay]);

  const matchesDateRange = useCallback((numericValue?: number) => {
    if (!dateRange.start && !dateRange.endExclusive) return true;
    if (!numericValue || numericValue === 0) return false;
    if (dateRange.start !== null && numericValue < dateRange.start) return false;
    if (dateRange.endExclusive !== null && numericValue >= dateRange.endExclusive) return false;
    return true;
  }, [dateRange]);

  const selectedBuildingAliases = useMemo(() => {
    if (selectedBuilding === 'all') return new Set<string>();
    const selected = buildings.find((building) => String(building.id) === selectedBuilding || String(building.code) === selectedBuilding);
    return new Set(
      [selectedBuilding, selected?.id != null ? String(selected.id) : '', selected?.code ? String(selected.code) : '']
        .filter(Boolean)
    );
  }, [buildings, selectedBuilding]);

  const matchesBuildingFilter = useCallback((buildingId?: string | number) => {
    if (selectedBuilding === 'all') return true;
    return selectedBuildingAliases.has(String(buildingId ?? ''));
  }, [selectedBuilding, selectedBuildingAliases]);

  const buildingOptions = useMemo(() => {
    if (!dateRange.start && !dateRange.endExclusive) return buildings;

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
      const inBuilding = matchesBuildingFilter(o.buildingId);
      const inRequester = selectedRequester === 'all' || String(o.requesterId) === selectedRequester || o.requesterId === selectedRequester;
      return inDate && inBuilding && inRequester;
    });
  }, [allOrders, matchesBuildingFilter, matchesDateRange, selectedRequester]);

  const ordersForRequesterOptions = useMemo(() => {
    return allOrders.filter(o => {
      const inDate = matchesDateRange(o.dateNumeric);
      const inBuilding = matchesBuildingFilter(o.buildingId);
      const inUser = selectedUser === 'all' || String(o.buyerId) === selectedUser;
      return inDate && inBuilding && inUser;
    });
  }, [allOrders, matchesBuildingFilter, matchesDateRange, selectedUser]);

  const availableUsers = useMemo(() => {
    const seen = new Map<string, User>();
    ordersForUserOptions.forEach((o) => {
      const id = String(o.buyerId || '');
      if (!id || id === '0' || id === 'undefined') return;
      seen.set(id, { id, name: userMap[id] || (o as any).nomeComprador || (o as any).buyerName || `Comprador ${id}` });
    });
    return Array.from(seen.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [ordersForUserOptions, userMap]);

  const availableRequesters = useMemo(() => {
    const seen = new Map<string, User>();
    ordersForRequesterOptions.forEach((o) => {
      const id = String(o.requesterId || (o as any).solicitante || '');
      if (!id || id === '0' || id === 'undefined') return;
      const requesterName = fixText(String((o as any).solicitante || (o as any).nomeSolicitante || userMap[id] || id)).replace(/^Comprador\\s+/i, '').trim();
      seen.set(id, { id, name: requesterName || id });
    });
    return Array.from(seen.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [ordersForRequesterOptions, userMap]);

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
    if (selectedBuilding !== 'all' && !buildingOptions.some((b) => String(b.id) === selectedBuilding)) {
      setSelectedBuilding('all');
    }
  }, [buildingOptions, selectedBuilding]);

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
    const filteredOrders = allOrders.filter((o) => {
      const inDate = matchesDateRange(o.dateNumeric);
      const inBuilding = matchesBuildingFilter(o.buildingId);
      const inUser = selectedUser === 'all' || String(o.buyerId) === selectedUser;
      const inRequester = selectedRequester === 'all' || String(o.requesterId) === selectedRequester || o.requesterId === selectedRequester;
      return inDate && inBuilding && inUser && inRequester;
    }).sort((a, b) => (b.dateNumeric || 0) - (a.dateNumeric || 0));

    const filteredFinancial = allFinancialTitles.filter((f) => {
      const inDate = matchesDateRange(f.dueDateNumeric);
      const inBuilding = matchesBuildingFilter(f.buildingId);
      return inDate && inBuilding;
    });

    const filteredReceivable = allReceivableTitles.filter((r) => {
      const inDate = matchesDateRange(r.dueDateNumeric);
      const inBuilding = matchesBuildingFilter(r.buildingId);
      return inDate && inBuilding;
    });

    setOrders(filteredOrders);
    setFinancialTitles(filteredFinancial);
    setReceivableTitles(filteredReceivable);
  }, [
    allFinancialTitles,
    allOrders,
    allReceivableTitles,
    matchesBuildingFilter,
    matchesDateRange,
    selectedBuilding,
    selectedRequester,
    selectedUser
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

  const handlePrint = () => {
    setIsPrinting(true);
    setTimeout(() => {
      window.print();
    }, 500); // 500ms window para renderizar 100% dos dados na VDOM
  };

  const toMoney = (value: unknown) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
  };

  const normalizeStatus = useCallback((value: unknown) => (
    fixText(String(value || ''))
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase()
  ), []);

  const isSettledFinancialStatus = useCallback((value: unknown) => {
    const status = normalizeStatus(value);
    return ['S', 'BAIXADO', 'BAIXADA', 'PAGO', 'PAGA', 'LIQUIDADO', 'LIQUIDADA', 'QUITADO', 'QUITADA'].includes(status);
  }, [normalizeStatus]);

  const activeBuildingCount = useMemo(() => {
    const ids = new Set<string>();

    orders.forEach((o) => {
      if (o?.buildingId) ids.add(String(o.buildingId));
    });
    financialTitles.forEach((f) => {
      if (f?.buildingId) ids.add(String(f.buildingId));
    });
    receivableTitles.forEach((r) => {
      if (r?.buildingId) ids.add(String(r.buildingId));
    });

    return ids.size || buildings.length;
  }, [buildings.length, financialTitles, orders, receivableTitles]);

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

  const historicalStats = useMemo(() => {
    const historicalOrders = Array.isArray(allOrders) ? allOrders : [];
    const historicalFinancial = Array.isArray(allFinancialTitles) ? allFinancialTitles : [];

    const totalPurchases = historicalOrders.reduce((acc, curr) => acc + toMoney(curr.totalAmount), 0);
    const totalPaid = historicalFinancial
      .filter((title) => isSettledFinancialStatus(title.status))
      .reduce((acc, curr) => acc + toMoney(curr.amount), 0);

    return { totalPurchases, totalPaid };
  }, [allFinancialTitles, allOrders, isSettledFinancialStatus]);


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
      return `${o.id};"${obra}";"${user}";${safeFormat(o.date)};${valorStr};${o.status}`;
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
      csvRows.push(`Pedido;${o.id};"${obra}";"${user}";${safeFormat(o.date)};${valorStr};${o.status}`);
    });

    financialTitles.forEach(f => {
      const obra = bMap[f.buildingId] || String(f.buildingId);
      const credor = f.creditorName || "S/N";
      const valorStr = String(f.amount || 0).replace('.', ',');
      csvRows.push(`A Pagar;${f.id};"${obra}";"${credor}";${safeFormat(f.dueDate)};${valorStr};${f.status}`);
    });

    receivableTitles.forEach(r => {
      const obra = bMap[r.buildingId] || String(r.buildingId);
      const cliente = r.clientName || "S/N";
      const valorStr = String(r.amount || 0).replace('.', ',');
      csvRows.push(`A Receber;${r.id};"${obra}";"${cliente}";${safeFormat(r.dueDate)};${valorStr};${r.status}`);
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
    return <div className="min-h-screen bg-[#0F0F10]" />;
  }

  if (!sessionUser) {
    return <LoginScreen onLogin={handleLogin} />;
  }

  return (
    <>
    <div className={cn("min-h-screen overflow-x-hidden bg-[#0F0F10] text-gray-100 font-sans selection:bg-orange-500/30", reportType ? "print:hidden" : "")}>
      {/* Header */}
      <header className="border-b border-white/5 bg-[#161618]/80 backdrop-blur-xl sticky top-0 z-50 print:hidden">
        <div className="tablet-safe-wrap w-full max-w-[98%] 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 h-16 sm:h-20 flex items-center justify-between gap-3">
          {/* Logo */}
          <div className="flex items-center gap-3 shrink-0">
            <div className="w-9 h-9 sm:w-12 sm:h-12 bg-gradient-to-br from-orange-500 to-orange-700 rounded-xl sm:rounded-2xl flex items-center justify-center shadow-lg shadow-orange-500/20">
              <Building2 className="text-white" size={20} />
            </div>
            <div>
              <h1 className="text-lg sm:text-2xl font-black tracking-tighter text-white uppercase">Dinamica</h1>
              <div className="flex items-center gap-2">
                <p className="hidden sm:block text-[10px] font-bold tracking-[0.2em] text-orange-500 uppercase opacity-80">Dashboard Financeiro</p>
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
          <nav className="hidden xl:flex items-center bg-black/40 p-1 rounded-xl border border-white/5">
            {availableTabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={cn(
                  "flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-bold transition-all",
                  activeTab === tab.id 
                    ? "bg-orange-600 text-white shadow-lg shadow-orange-600/20" 
                    : "text-gray-400 hover:text-white hover:bg-white/5"
                )}
              >
                <tab.icon size={16} />
                <span>{tab.label}</span>
              </button>
            ))}
          </nav>

          {/* Desktop Actions */}
          <div className="hidden xl:flex items-center gap-2 2xl:gap-3">
            <div className="flex flex-col rounded-xl border border-white/5 bg-white/5 px-3 py-2 text-xs font-bold text-gray-300 min-w-[128px] max-w-[150px]">
              <div className="flex items-center gap-2">
                <UserIcon size={14} className="text-orange-500" />
                <span className="truncate">{sessionUser.name}</span>
              </div>
              <Button
                onClick={handleLogout}
                variant="outline"
                className="mt-2 h-9 border-white/10 bg-transparent text-white hover:bg-white/10 font-bold rounded-lg px-3 gap-2"
              >
                <LogOut size={14} />
                <span>Sair</span>
              </Button>
            </div>
            <Button 
              onClick={downloadData}
              variant="outline"
              className="bg-orange-600/10 text-orange-500 border-orange-600/20 hover:bg-orange-600 hover:text-white font-bold rounded-xl h-11 px-3 2xl:px-4 gap-2"
            >
              <Download size={16} />
              <span className="hidden 2xl:inline">Baixar Dados</span>
            </Button>
            <Button 
              onClick={syncSienge} 
              disabled={syncing}
              className="bg-white text-black hover:bg-gray-200 font-bold rounded-xl h-11 px-3 2xl:px-4 gap-2 shrink-0"
            >
              <RefreshCw size={16} className={cn(syncing && "animate-spin")} />
              <span className="hidden 2xl:inline">{syncing ? "Sincronizando..." : "Sincronizar"}</span>
            </Button>
          </div>

          {/* Mobile Action Buttons */}
          <div className="flex xl:hidden items-center gap-2 ml-auto">
            <button
              onClick={syncSienge}
              disabled={syncing}
              className="w-9 h-9 flex items-center justify-center rounded-xl bg-white/10 text-white"
            >
              <RefreshCw size={16} className={cn(syncing && "animate-spin")} />
            </button>
            <button
              onClick={downloadData}
              className="w-9 h-9 flex items-center justify-center rounded-xl bg-orange-600/20 text-orange-500"
            >
              <Download size={16} />
            </button>
            <button
              onClick={handleLogout}
              className="w-9 h-9 flex items-center justify-center rounded-xl bg-white/10 text-white"
            >
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </header>

      {/* Mobile Bottom Navigation */}
      <nav className="xl:hidden fixed bottom-0 left-0 right-0 z-50 bg-[#161618]/95 backdrop-blur-xl border-t border-white/5 flex flex-wrap print:hidden">
        {availableTabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id as any)}
            className={cn(
              "flex-1 flex flex-col items-center justify-center py-2 gap-1 text-[10px] font-bold transition-all",
              activeTab === tab.id ? "text-orange-500" : "text-gray-500"
            )}
          >
            <tab.icon size={20} />
            <span>{tab.label}</span>
          </button>
        ))}
      </nav>

      <main className="w-full max-w-full 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 py-6 sm:py-10 pb-24 xl:pb-10">
        {/* Global Date Filter - Mobile Collapsible */}
        {activeTab !== 'logistics' && activeTab !== 'access' && (
          <div className="mb-6 sm:mb-10 bg-[#161618] rounded-2xl border border-white/5 shadow-xl print:hidden overflow-hidden">
            {/* Filter Header - Mobile Toggle */}
          <button
            onClick={() => setMobileFiltersOpen(!mobileFiltersOpen)}
            className="w-full flex items-center justify-between p-4 sm:p-6 md:cursor-default"
          >
            <div className="flex items-center gap-3">
              <SlidersHorizontal size={16} className="text-orange-500" />
              <span className="text-sm font-black uppercase tracking-widest text-orange-500">Filtros</span>
              {(selectedBuilding !== 'all' || selectedUser !== 'all' || selectedRequester !== 'all' || startDate) && (
                <span className="bg-orange-600 text-white text-[9px] font-black px-2 py-0.5 rounded-full uppercase">Ativo</span>
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
                      onSelect={(date: any) => date && setStartDate(date)}
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
                      onSelect={(date: any) => date && setEndDate(date)}
                      className="text-white"
                    />
                  </PopoverContent>
                </Popover>
              </div>

              <div className="space-y-2 flex-1 sm:flex-none">
                <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obra</Label>
                <Select value={selectedBuilding} onValueChange={setSelectedBuilding}>
                  <SelectTrigger className="w-full sm:w-[180px] bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                    <SelectValue placeholder="Todas" />
                  </SelectTrigger>
                  <SelectContent className="bg-[#161618] border-white/10 text-white">
                    <SelectItem value="all">Todas as Obras</SelectItem>
                     {buildingOptions.map(b => (
                       <SelectItem key={b.id} value={String(b.id)}>{b.name}</SelectItem>
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
          {activeTab === 'dashboard' && (
            <motion.div
              key="dashboard"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-8"
            >
              {/* KPI Grid */}
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-6">
                {[
                  { label: 'COMPRAS EFETUADAS', value: `R$ ${stats.total.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`, description: `${orders.length} pedidos processados`, icon: TrendingUp, color: 'orange' },
                  { label: 'Saldo Financeiro (R-P)', value: `R$ ${stats.balance.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`, description: 'Receber - Pagar', icon: DollarSign, color: stats.balance >= 0 ? 'green' : 'red' },
                  { label: 'Obras Ativas (Filtro)', value: activeBuildingCount, description: 'Com atividade no período', icon: Building2, color: 'orange' },
                  { label: 'Pedidos Solicitados', value: orders.length, description: 'Pedidos processados no período', icon: Package, color: 'orange' },
                ].map((kpi, i) => (
                  <Card key={i} className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative group">
                    <div className="absolute top-0 right-0 p-3 opacity-10 group-hover:opacity-20 transition-opacity">
                      <kpi.icon size={40} className="text-orange-500" />
                    </div>
                    <CardHeader className="pb-2 p-4 sm:p-6">
                      <CardDescription className="text-[9px] sm:text-[10px] font-black uppercase tracking-widest text-orange-500/70 leading-tight">{kpi.label}</CardDescription>
                      <CardTitle className={cn("text-xl sm:text-3xl font-black tracking-tighter mt-1", kpi.color === 'red' ? 'text-red-500' : kpi.color === 'green' ? 'text-green-500' : 'text-white')}>
                        {kpi.value}
                      </CardTitle>
                      {kpi.description && (
                         <div className="hidden sm:block text-xs text-gray-400 mt-2 font-bold">{kpi.description}</div>
                      )}
                    </CardHeader>
                    <div className="h-1 w-full bg-orange-600/20">
                      <div className="h-full bg-orange-600 w-1/3" />
                    </div>
                  </Card>
                ))}
              </div>

              {/* Financial Quick Summary */}
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 sm:gap-6">
                <Card className="bg-gradient-to-br from-orange-600 to-orange-800 border-none shadow-2xl shadow-orange-900/20">
                  <CardContent className="p-5 sm:p-8 flex items-center justify-between">
                    <div>
                      <p className="text-orange-200 text-[10px] sm:text-xs font-black uppercase tracking-widest mb-1 sm:mb-2">Volume de Compras</p>
                      <h3 className="text-2xl sm:text-4xl font-black text-white">R$ {historicalStats.totalPurchases.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</h3>
                    </div>
                    <div className="bg-white/20 p-3 sm:p-4 rounded-xl sm:rounded-2xl">
                      <Package className="text-white" size={24} />
                    </div>
                  </CardContent>
                </Card>
                
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-5 sm:p-8 flex items-center justify-between">
                    <div>
                      <p className="text-gray-500 text-[10px] sm:text-xs font-black uppercase tracking-widest mb-1 sm:mb-2">Contas Pagas</p>
                      <h3 className="text-2xl sm:text-4xl font-black text-red-500">R$ {historicalStats.totalPaid.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</h3>
                    </div>
                    <div className="bg-red-500/10 p-3 sm:p-4 rounded-xl sm:rounded-2xl">
                      <TrendingDown className="text-red-500" size={24} />
                    </div>
                  </CardContent>
                </Card>

                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-5 sm:p-8 flex items-center justify-between">
                    <div>
                      <p className="text-gray-500 text-[10px] sm:text-xs font-black uppercase tracking-widest mb-1 sm:mb-2">Contas a Receber</p>
                      <h3 className="text-2xl sm:text-4xl font-black text-green-500">R$ {stats.rTotal.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</h3>
                    </div>
                    <div className="bg-green-500/10 p-3 sm:p-4 rounded-xl sm:rounded-2xl">
                      <TrendingUp className="text-green-500" size={24} />
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Main Charts Row */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 sm:gap-8">
                <Card className="lg:col-span-2 bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader className="flex flex-row items-center justify-between">
                    <div>
                      <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Evolução Mensal de Faturamento</CardTitle>
                      <CardDescription className="text-gray-500">Comparativo de performance por período</CardDescription>
                    </div>
                    <div className="flex gap-2">
                      <Badge className="bg-orange-600/10 text-orange-500 border-none">2026</Badge>
                    </div>
                  </CardHeader>
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
                        <Tooltip 
                          formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)}
                          contentStyle={{ backgroundColor: '#161618', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px' }}
                          itemStyle={{ fontWeight: 'bold' }}
                        />
                        <Legend />
                        <Area type="monotone" dataKey="valor" name="Compras" stroke="#f97316" strokeWidth={4} fillOpacity={1} fill="url(#colorVal)" />
                        <Area type="monotone" dataKey="financeiro" name="Contas a Pagar" stroke="#3b82f6" strokeWidth={4} fillOpacity={0.3} fill="#3b82f6" />
                      </AreaChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>

                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader>
                    <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Forma de Pagamento</CardTitle>
                    <CardDescription className="text-gray-500">Distribuição por modalidade</CardDescription>
                  </CardHeader>
                  <CardContent className="h-[220px] sm:h-[350px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie
                          data={paymentMethodData}
                          cx="50%"
                          cy="50%"
                          innerRadius={60}
                          outerRadius={80}
                          paddingAngle={5}
                          dataKey="value"
                        >
                          {paymentMethodData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={['#f97316', '#3b82f6', '#10b981', '#f59e0b', '#6366f1'][index % 5]} />
                          ))}
                        </Pie>
                        <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none', borderRadius: '8px' }} />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>

              {/* Bottom Row */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-8">
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader>
                    <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Faturamento por Fornecedor</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[220px] sm:h-[300px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={supplierData} layout="vertical">
                        <XAxis type="number" hide />
                        <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{fill: '#fff', fontSize: 10}} width={120} />
                        <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} cursor={{fill: '#ffffff05'}} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                        <Bar dataKey="value" fill="#f97316" radius={[0, 4, 4, 0]} barSize={20} />
                      </BarChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>

                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader className="flex flex-row items-center justify-between">
                    <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Últimos Pedidos</CardTitle>
                    <Button variant="ghost" size="sm" className="text-orange-500 font-bold" onClick={downloadCSV}>
                      <Download size={14} className="mr-2" /> Exportar CSV
                    </Button>
                  </CardHeader>
                  <CardContent className="p-0">
                    <Table>
                      <TableHeader className="bg-black/20">
                        <TableRow className="border-white/5 hover:bg-transparent">
                          <TableHead className="text-[10px] font-black uppercase text-gray-500">Obra</TableHead>
                          <TableHead className="text-[10px] font-black uppercase text-gray-500">Fornecedor</TableHead>
                          <TableHead className="text-[10px] font-black uppercase text-gray-500">Data</TableHead>
                          <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right">Valor</TableHead>
                        </TableRow>
                      </TableHeader>
                          <TableBody>
                            {orders.length === 0 ? (
                              <TableRow>
                                <TableCell colSpan={4} className="text-center py-10 text-gray-500 font-bold">
                                  Nenhum pedido encontrado no período.
                                </TableCell>
                              </TableRow>
                            ) : (
                              orders.slice(0, 6).map((order, idx) => (
                                <TableRow key={order.id || `order-${idx}`} className="border-white/5 hover:bg-white/5 transition-colors">
                                <TableCell className="font-bold text-sm text-gray-300">
                                    {resolveBuildingName(order)}
                                  </TableCell>
                                  <TableCell className="text-xs text-gray-400">
                                    {resolveCreditorName(order)}
                                  </TableCell>
                                  <TableCell className="text-xs text-gray-500">
                                    {safeFormat(order.date, 'dd/MM/yy')}
                                  </TableCell>
                                  <TableCell className="text-right font-black text-orange-500">
                                    R$ {(order.totalAmount || 0).toLocaleString('pt-BR')}
                                  </TableCell>
                                </TableRow>
                              ))
                            )}
                          </TableBody>
                    </Table>
                  </CardContent>
                </Card>
              </div>
            </motion.div>
          )}

          {activeTab === 'alerts' && (
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
                  <Button 
                    onClick={handlePrint}
                    className="bg-white text-black hover:bg-gray-200 font-black tracking-tight rounded-xl print:hidden text-sm h-9"
                  >
                    <Printer size={14} className="mr-2" />
                    PDF
                  </Button>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3 sm:gap-4">
                {priceAlerts.slice(0, 8).map((alert, idx) => (
                  <Card key={idx} className="bg-gradient-to-br from-orange-600/20 to-transparent border-orange-500/20 shadow-none overflow-hidden">
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
                          <div className="mt-4 h-[50px] sm:h-16 w-full opacity-60 hover:opacity-100 transition-opacity">
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
                                  <TableCell><Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">{o.status}</Badge></TableCell>
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
                                className={cn(
                                  "border-white/5 hover:bg-white/5 transition-colors border-l-2",
                                  isCheaper && "border-l-green-500 shadow-[inset_4px_0_0_rgba(34,197,94,0.95)] bg-[linear-gradient(90deg,rgba(34,197,94,0.12),transparent_18%)]",
                                  isExpensive && "border-l-red-500 shadow-[inset_4px_0_0_rgba(239,68,68,0.95)] bg-[linear-gradient(90deg,rgba(239,68,68,0.12),transparent_18%)]"
                                )}
                              >
                                <TableCell className="font-bold text-orange-500" title={desc}>
                                  <div className="max-w-[200px] truncate">{desc}</div>
                                </TableCell>
                                <TableCell className="text-xs text-gray-500">{safeFormat(o.date)}</TableCell>
                                <TableCell>
                                  <Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">{o.status}</Badge>
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

          {activeTab === 'finance' && (
            <motion.div
              key="finance"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-8"
            >
              {(() => {
                const openPayables = financialTitles.filter(t => t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO').sort((a,b) => (b.dueDateNumeric || 0) - (a.dueDateNumeric || 0));
                const openReceivables = receivableTitles.filter(t => t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO').sort((a,b) => (b.dueDateNumeric || 0) - (a.dueDateNumeric || 0));

                const totalPayable = openPayables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                const totalReceivable = openReceivables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                
                return (
                  <>
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

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-8">
                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[400px] sm:h-[500px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between">
                          <CardTitle className="text-lg font-black uppercase text-white">Contas a Pagar</CardTitle>
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
                                openPayables.map((title, idx) => (
                                  <TableRow key={title.id || `pay-${idx}`} className="border-white/5 hover:bg-white/5">
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
                        </CardContent>
                      </Card>

                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[400px] sm:h-[500px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between">
                          <CardTitle className="text-lg font-black uppercase text-white">Contas a Receber</CardTitle>
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
                                    Nenhum recebimento previsto neste período.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                openReceivables.map((title, idx) => (
                                  <TableRow key={title.id || `rec-${idx}`} className="border-white/5 hover:bg-white/5">
                                    <TableCell className="text-xs font-mono text-gray-500">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-gray-300 truncate max-w-[200px]" title={title.clientName}>
                                        {title.clientName}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-400">
                                      {safeFormat(title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-green-500 whitespace-nowrap">
                                      R$ {(title.amount || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                        </CardContent>
                      </Card>
                    </div>
                  </>
                );
              })()}
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





