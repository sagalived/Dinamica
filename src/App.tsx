import { useState, useEffect, useMemo, useCallback, useRef, Fragment } from 'react';
import { 
  LayoutDashboard, Bell, Filter, Download, TrendingUp, TrendingDown, 
  DollarSign, Package, Calendar as CalendarIcon, RefreshCw, 
  User as UserIcon, Building2, ChevronRight, Search, Map as MapIcon,
  Wifi, WifiOff, CheckCircle2, AlertCircle, FileText, Printer, X
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button, buttonVariants } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { format, subDays, addDays, isWithinInterval, startOfYear, endOfYear, parseISO } from 'date-fns';
import { ptBR } from 'date-fns/locale';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  AreaChart, Area, Cell, PieChart, Pie, LineChart, Line, Legend
} from 'recharts';
import { api, Building, User, Creditor, PurchaseOrder, PriceAlert } from './lib/api';
import { cn } from './lib/utils';

export default function App() {
  const [activeTab, setActiveTab] = useState<'dashboard' | 'alerts' | 'map' | 'finance'>('dashboard');
  const [loading, setLoading] = useState(false);
  const [apiStatus, setApiStatus] = useState<'online' | 'offline' | 'checking'>('checking');
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  const [startDate, setStartDate] = useState<Date | undefined>(); // Sem data inicial por padrão para carregar todo o histórico do Sienge
  const [endDate, setEndDate] = useState<Date | undefined>(new Date());
  const [isPrinting, setIsPrinting] = useState(false);
  const [newOrderAlert, setNewOrderAlert] = useState<PurchaseOrder | null>(null);
  const knownOrderIdsRef = useRef<Set<number>>(new Set());

  type ReportType = 'pagar' | 'receber' | 'abertos' | null;
  const [reportType, setReportType] = useState<ReportType>(null);

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
      await api.get('/test');
      setApiStatus('online');
      return true;
    } catch (error) {
      console.error("Connection test failed:", error);
      setApiStatus('offline');
      return false;
    }
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
      const [bRes, uRes, cRes, compRes] = await Promise.allSettled([
        api.get('/obras'),
        api.get('/usuarios'),
        api.get('/credores'),
        api.get('/companies')
      ]);
      
      const bDataRaw = bRes.status === 'fulfilled' && bRes.value.data ? (bRes.value.data.results || bRes.value.data) : [];
      const uDataRaw = uRes.status === 'fulfilled' && uRes.value.data ? (uRes.value.data.results || uRes.value.data) : [];
      const cDataRaw = cRes.status === 'fulfilled' && cRes.value.data ? (cRes.value.data.results || cRes.value.data) : [];
      const compDataRaw = compRes.status === 'fulfilled' && compRes.value.data ? (compRes.value.data.results || compRes.value.data) : [];

      const bData = Array.isArray(bDataRaw) ? bDataRaw.map((b: any) => ({
        id: b.id,
        name: b.nome || b.name || `Obra ${b.id}`,
        latitude: b.latitude || -23.5505 + (Math.random() - 0.5) * 0.1,
        longitude: b.longitude || -46.6333 + (Math.random() - 0.5) * 0.1,
        address: b.endereco || b.address,
        companyId: b.idCompany,
        engineer: b.responsavel || b.nomeResponsavel || b.gerente || b.engenheiro || b.responsavelTecnico || "Aguardando Avaliação"
      })) : [];

      const uData = Array.isArray(uDataRaw) ? uDataRaw.map((u: any) => ({
        id: String(u.id),
        name: u.nome || u.name || `Usuário ${u.id}`
      })) : [];

      const cData = Array.isArray(cDataRaw) ? cDataRaw.map((c: any) => ({
        id: c.id,
        name: c.nome || c.name || `Credor ${c.id}`,
        cnpj: c.cnpj || c.cpfCnpj
      })) : [];

      setBuildings(bData);
      setUsers(uData);
      setCreditors(cData);
      setCompanies(Array.isArray(compDataRaw) ? compDataRaw : []);
      
      // Only set online if we actually got some arrays back
      if (Array.isArray(bDataRaw) || Array.isArray(uDataRaw) || Array.isArray(cDataRaw)) {
        setApiStatus('online');
      } else {
        setApiStatus('offline');
      }
      
      const itemsRes = await api.get('/itens-pedidos').catch(() => null);
      if (itemsRes && itemsRes.data) {
        setItemsDetailsMap(itemsRes.data);
      }
      
      await refreshData();
    } catch (error) {
      console.error("Error fetching initial data:", error);
      setApiStatus('offline');
      setBuildings([]);
      setUsers([]);
      setCreditors([]);
      await refreshData();
    } finally {
      setLoading(false);
    }
  }, [startDate, endDate, selectedBuilding, selectedUser]);

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
    if (allOrders.length === 0) return;
    const historyMap: Record<string, any[]> = {};
    allOrders.forEach(o => {
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
      
      const beforeFilter = purchases.filter(p => {
        const d = parseISO(p.date);
        return isNaN(d.getTime()) ? false : (startDate ? d <= addDays(startDate, 1) : true);
      });
      if (beforeFilter.length > 0) {
        baseMap[desc] = beforeFilter[beforeFilter.length - 1].price;
      } else {
        baseMap[desc] = purchases[0].price;
      }
    });
    setLatestPricesMap(pricesMap);
    setBaselinePricesMap(baseMap);
  }, [allOrders, itemsDetailsMap, startDate]);

  const syncSienge = async () => {
    setLoading(true);
    setApiStatus('checking');
    try {
      await api.post('/sync');
      await refreshData();
      setApiStatus('online');
    } catch (e) {
      console.error("Sync error:", e);
      setApiStatus('offline');
    } finally {
      setLoading(false);
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
      const [ordersRes, financeRes, receivableRes] = await Promise.allSettled([
        api.get('/pedidos-compra', { params: { limit: 5000 } }),
        api.get('/financeiro', { params: { limit: 5000 } }),
        api.get('/financeiro/receber', { params: { limit: 5000 } })
      ]);

      let oDataRaw = ordersRes.status === 'fulfilled' && ordersRes.value.data ? (ordersRes.value.data.results || ordersRes.value.data) : [];
      let fDataRaw = financeRes.status === 'fulfilled' && financeRes.value.data ? (financeRes.value.data.results || financeRes.value.data) : [];
      let rDataRaw = receivableRes.status === 'fulfilled' && receivableRes.value.data ? (receivableRes.value.data.results || receivableRes.value.data) : [];

      const rawOrdersArray = Array.isArray(oDataRaw) ? oDataRaw : [];
      
      // Auto-reconstruir catálogos caso a Sienge esteja bloqueando endpoints /obras ou /usuarios
      if (rawOrdersArray.length > 0) {
        const uniqueBuildings = new Map<string, Building>();
        const uniqueUsers = new Map<string, User>();
        const uniqueRequesters = new Map<string, User>();
        
        rawOrdersArray.forEach((o: any) => {
          const bId = Number(o.idObra || o.codigoVisivelObra || o.buildingId);
          if (bId && !uniqueBuildings.has(String(bId))) {
            uniqueBuildings.set(String(bId), { id: bId, name: o.nomeObra || `Obra ${bId}`, latitude: 0, longitude: 0, address: '', companyId: 0, engineer: '' });
          }
          const uId = String(o.idComprador || o.codigoComprador || o.buyerId || "0");
          if (uId && uId !== "0" && !uniqueUsers.has(uId)) {
            uniqueUsers.set(uId, { id: uId, name: o.nomeComprador || o.nomeUsuario || o.buyerName || `Comprador ${uId}` });
          }
          // Prioridade: solicitante (requesterUser da Solicitação de Compra Sienge) -> fallback comprador
          const solName = o.solicitante || o.nomeSolicitante || o.createdBy || "";
          if (solName && !uniqueRequesters.has(solName)) {
            uniqueRequesters.set(solName, { id: solName, name: solName });
          }
        });
        
        if (buildings.length === 0 && uniqueBuildings.size > 0) setBuildings(Array.from(uniqueBuildings.values()));
        if (users.length === 0 && uniqueUsers.size > 0) setUsers(Array.from(uniqueUsers.values()));
        // Sempre atualiza solicitantes pois novos podem surgir do cache da Sienge
        if (uniqueRequesters.size > 0) setRequesters(Array.from(uniqueRequesters.values()));
      }

      // Mapping Raw Data First (All-Time Data)
      const allOData: PurchaseOrder[] = rawOrdersArray.map((o: any) => {
        const dStr = o.dataEmissao || o.data || o.date || "---";
        const d = parseISO(dStr);
        return {
          id: o.id || o.numero || 0,
          buildingId: o.idObra || o.codigoVisivelObra || o.buildingId || 0,
          buyerId: o.idComprador ? String(o.idComprador) : (o.codigoComprador ? String(o.codigoComprador) : (o.buyerId ? String(o.buyerId) : "0")),
          date: dStr,
          dateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
          totalAmount: parseFloat(o.valorTotal || o.totalAmount) || 0,
          supplierId: o.codigoFornecedor || o.supplierId,
          status: o.situacao || o.status || 'N/A',
          paymentCondition: o.condicaoPagamento || o.paymentMethod || 'A Prazo',
          deliveryDate: o.dataEntrega || o.prazoEntrega || '---',
          internalNotes: o.internalNotes || o.observacao || "",
          createdBy: o.createdBy || o.criadoPor || "",
          // 'solicitante' é preenchido pelo server.ts via requesterUser da Solicitação de Compra
          // Se não existir (compra direta sem solicitação), cai no createdBy (comprador)
          requesterId: o.solicitante || o.requesterId || o.createdBy || "0"
        };
      });

      // --- LOGIC FOR NEW ORDER POPUP ---
      if (knownOrderIdsRef.current.size > 0) {
        const newOrders = allOData.filter(o => !knownOrderIdsRef.current.has(o.id));
        if (newOrders.length > 0) {
          setNewOrderAlert(newOrders[0]); 
        }
      }
      allOData.forEach(o => knownOrderIdsRef.current.add(o.id));
      // ---------------------------------

      const allFData = (Array.isArray(fDataRaw) ? fDataRaw : []).map((f: any) => {
        const dStr = f.dataVencimento || f.issueDate || f.dataVencimentoProjetado || f.dataEmissao || f.dataContabil || "---";
        const d = parseISO(dStr);
        return {
          id: f.id || f.numero || f.codigoTitulo || 0,
          buildingId: f.idObra || f.debtorId || f.codigoObra || 0,
          description: f.descricao || f.historico || f.tipoDocumento || f.observacao || 'Título a Pagar',
          creditorName: f.nomeCredor || f.nomeFantasiaCredor || f.fornecedor || f.credor || 'S/N',
          dueDate: dStr,
          dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
          amount: parseFloat(f.valor || f.amount || f.valorTotal || f.valorLiquido || f.valorBruto) || 0,
          status: f.situacao || f.status || 'Pendente',
        };
      });

      const allRData = (Array.isArray(rDataRaw) ? rDataRaw : []).map((r: any) => {
        const dStr = r.dataVencimento || r.dataEmissao || r.issueDate || r?.dataVencimentoProjetado || "---";
        const d = parseISO(dStr);
        return {
          id: r.id || r.numero || r.numeroTitulo || r.codigoTitulo || 0,
          buildingId: r.idObra || r.codigoObra || 0,
          description: r.descricao || r.historico || r.observacao || r.notes || 'Título a Receber',
          clientName: r.nomeCliente || r.nomeFantasiaCliente || r.cliente || 'S/N',
          dueDate: dStr,
          dueDateNumeric: isNaN(d.getTime()) ? 0 : d.getTime(),
          amount: parseFloat(r.valor || r.valorSaldo || r.totalInvoiceAmount || r.valorTotal) || 0,
          status: String(r.situacao || r.status || 'ABERTO').toUpperCase(),
        };
      });

      // Apply Local Filters for Dashboard Tables (Date/Building Selection)
      const startDateNum = startDate ? startDate.getTime() : null;
      const endDateNum = startDate ? addDays(endDate || new Date(), 1).getTime() : null;

      const filteredOData = allOData.filter((o) => {
        const inDate = startDateNum ? (o.dateNumeric !== 0 && o.dateNumeric! >= startDateNum && o.dateNumeric! <= endDateNum!) : true;
        const inBuilding = selectedBuilding === 'all' || String(o.buildingId) === selectedBuilding;
        const inUser = selectedUser === 'all' || String(o.buyerId) === selectedUser;
        const inRequester = selectedRequester === 'all' || String(o.requesterId) === selectedRequester || o.requesterId === selectedRequester;
        return inDate && inBuilding && inUser && inRequester;
      }).sort((a, b) => (b.dateNumeric || 0) - (a.dateNumeric || 0));

      const filteredFData = allFData.filter((f) => {
        const inDate = startDateNum ? (f.dueDateNumeric !== 0 && f.dueDateNumeric >= startDateNum && f.dueDateNumeric <= endDateNum!) : true;
        const inBuilding = selectedBuilding === 'all' || String(f.buildingId) === selectedBuilding;
        return inDate && inBuilding;
      });

      const filteredRData = allRData.filter((r) => {
        const inDate = startDateNum ? (r.dueDateNumeric !== 0 && r.dueDateNumeric >= startDateNum && r.dueDateNumeric <= endDateNum!) : true;
        const inBuilding = selectedBuilding === 'all' || String(r.buildingId) === selectedBuilding;
        return inDate && inBuilding;
      });

      setAllOrders(allOData);
      setAllFinancialTitles(allFData);
      
      setOrders(filteredOData);
      setFinancialTitles(filteredFData);
      setReceivableTitles(filteredRData);

      // Fetch items for the most recent orders (limit to 15 for performance)
      const ordersWithItems = [...filteredOData];
      const ordersToFetchItems = ordersWithItems.slice(0, 15);
      
      await Promise.all(ordersToFetchItems.map(async (order) => {
        try {
          const itemsRes = await api.get(`/pedidos-compra/${order.id}/itens`);
          const itemsRaw = itemsRes.data.results || itemsRes.data;
          if (Array.isArray(itemsRaw)) {
            order.items = itemsRaw.map((item: any) => ({
              id: item.id,
              description: item.descricao || item.itemNome || 'Item',
              quantity: item.quantidade || 0,
              unitPrice: item.valorUnitario || 0,
              totalPrice: item.valorTotal || 0,
              unit: item.unidadeMedidaSigla || 'UN'
            }));
          }
        } catch (e) {
          console.warn(`Could not fetch items for order ${order.id}`);
        }
      }));

      setOrders(ordersWithItems);
      
      if (Array.isArray(oDataRaw) || Array.isArray(fDataRaw) || Array.isArray(rDataRaw)) {
        setLastUpdate(new Date());
        setApiStatus('online');
      } else {
        throw new Error("API returned non-array data");
      }
    } catch (error) {
      console.error("Error refreshing data:", error);
      setOrders([]);
      setFinancialTitles([]);
      setReceivableTitles([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchInitialData();
    
    // Agora o Client Puxa passivamente o cache em background a cada 20min (o servidor atualiza pesado autonomamente)
    const syncInterval = setInterval(() => {
      fetchInitialData();
    }, 20 * 60 * 1000);

    const handleAfterPrint = () => setIsPrinting(false);
    window.addEventListener('afterprint', handleAfterPrint);

    return () => {
      clearInterval(syncInterval);
      window.removeEventListener('afterprint', handleAfterPrint);
    };
  }, []);

  const handlePrint = () => {
    setIsPrinting(true);
    setTimeout(() => {
      window.print();
    }, 500); // 500ms window para renderizar 100% dos dados na VDOM
  };

  // Analytics Calculations
  const stats = useMemo(() => {
    const ordersArray = Array.isArray(orders) ? orders : [];
    const total = ordersArray.reduce((acc, curr) => acc + (curr.totalAmount || 0), 0);
    const avg = ordersArray.length > 0 ? total / ordersArray.length : 0;
    
    const fTotal = financialTitles.reduce((acc, curr) => acc + (curr.amount || 0), 0);
    const rTotal = receivableTitles.reduce((acc, curr) => acc + (curr.amount || 0), 0);
    const balance = rTotal - fTotal;

    return { total, avg, fTotal, rTotal, balance };
  }, [orders, financialTitles, receivableTitles]);

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
    const map: Record<number, number> = {};
    const ordersArray = Array.isArray(orders) ? orders : [];
    ordersArray.forEach(o => {
      if (o.supplierId) {
        map[o.supplierId] = (map[o.supplierId] || 0) + (o.totalAmount || 0);
      }
    });
    return Object.entries(map)
      .map(([id, val]) => ({ 
        name: creditors.find(c => c.id === Number(id))?.name || `Fornecedor ${id}`, 
        value: val 
      }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 5);
  }, [orders, creditors]);

  const paymentMethodData = useMemo(() => {
    const map: Record<string, number> = {};
    const ordersArray = Array.isArray(orders) ? orders : [];
    
    ordersArray.forEach(order => {
      const method = order.paymentCondition || 'Não Informado';
      map[method] = (map[method] || 0) + (order.totalAmount || 0);
    });

    const result = Object.entries(map)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);
      
    return result.length > 0 ? result : [
      { name: 'Boleto', value: 45000 },
      { name: 'PIX', value: 30000 },
      { name: 'Cartão', value: 15000 },
      { name: 'Transferência', value: 10000 },
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
    // Adicionar BOM (\uFEFF) para forçar o Excel a reconhecer UTF-8
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
  return (
    <>
    <div className={cn("min-h-screen bg-[#0F0F10] text-gray-100 font-sans selection:bg-orange-500/30", reportType ? "print:hidden" : "")}>
      {/* Header */}
      <header className="border-b border-white/5 bg-[#161618]/80 backdrop-blur-xl sticky top-0 z-50 print:hidden">
        <div className="w-full max-w-[98%] 2xl:max-w-[1800px] mx-auto px-6 h-20 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 bg-gradient-to-br from-orange-500 to-orange-700 rounded-2xl flex items-center justify-center shadow-lg shadow-orange-500/20">
              <Building2 className="text-white" size={28} />
            </div>
            <div>
              <h1 className="text-2xl font-black tracking-tighter text-white uppercase">Dinamica</h1>
              <div className="flex items-center gap-2">
                <p className="text-[10px] font-bold tracking-[0.2em] text-orange-500 uppercase opacity-80">Dashboard Financeiro</p>
                {apiStatus === 'online' && (
                  <span className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-[9px] font-bold text-emerald-500 uppercase tracking-wider">
                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                    Sienge
                  </span>
                )}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <nav className="flex items-center bg-black/40 p-1 rounded-xl border border-white/5 mr-4">
              {[
                { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
                { id: 'finance', label: 'Financeiro', icon: DollarSign },
                { id: 'alerts', label: 'Alertas', icon: Bell },
                { id: 'map', label: 'Mapa de Obras', icon: MapIcon },
              ].map((tab) => (
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
                  <span className="hidden md:inline">{tab.label}</span>
                </button>
              ))}
            </nav>
            <Button 
              onClick={downloadData}
              variant="outline"
              className="bg-orange-600/10 text-orange-500 border-orange-600/20 hover:bg-orange-600 hover:text-white font-bold rounded-xl h-11 px-6 gap-2"
            >
              <Download size={18} />
              Baixar Dados
            </Button>
            <Button 
              onClick={syncSienge} 
              disabled={loading}
              className="bg-white text-black hover:bg-gray-200 font-bold rounded-xl h-11 px-6 gap-2"
            >
              <RefreshCw size={18} className={cn(loading && "animate-spin")} />
              {loading ? "Sincronizando..." : "Sincronizar API"}
            </Button>
          </div>
        </div>
      </header>

      <main className="w-full max-w-[98%] 2xl:max-w-[1800px] mx-auto px-6 py-10">
        {/* Global Date Filter */}
        <div className="mb-10 flex flex-wrap items-end gap-6 bg-[#161618] p-6 rounded-2xl border border-white/5 shadow-xl print:hidden">
          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Inicial</Label>
            <Popover>
              <PopoverTrigger className={cn(buttonVariants({ variant: "outline" }), "w-[180px] h-12 justify-start bg-black/40 border-white/10 rounded-xl text-white font-bold")}>
                <CalendarIcon className="mr-3 h-5 w-5 text-orange-500" />
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

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Final</Label>
            <Popover>
              <PopoverTrigger className={cn(buttonVariants({ variant: "outline" }), "w-[180px] h-12 justify-start bg-black/40 border-white/10 rounded-xl text-white font-bold")}>
                <CalendarIcon className="mr-3 h-5 w-5 text-orange-500" />
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

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obra</Label>
            <Select value={selectedBuilding} onValueChange={setSelectedBuilding}>
              <SelectTrigger className="w-[200px] bg-black/40 border-white/10 h-12 rounded-xl text-white font-bold">
                <SelectValue placeholder="Todas" />
              </SelectTrigger>
              <SelectContent className="bg-[#161618] border-white/10 text-white">
                <SelectItem value="all">Todas as Obras</SelectItem>
                {buildings.map(b => (
                  <SelectItem key={b.id} value={String(b.id)}>{b.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Comprador</Label>
            <Select value={selectedUser} onValueChange={setSelectedUser}>
              <SelectTrigger className="w-[200px] bg-black/40 border-white/10 h-12 rounded-xl text-white font-bold">
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent className="bg-[#161618] border-white/10 text-white">
                <SelectItem value="all">Todos os Compradores</SelectItem>
                {users.map(u => (
                  <SelectItem key={u.id} value={String(u.id)}>{u.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Solicitante</Label>
            <Select value={selectedRequester} onValueChange={setSelectedRequester}>
              <SelectTrigger className="w-[200px] bg-black/40 border-white/10 h-12 rounded-xl text-white font-bold">
                <SelectValue placeholder="Todos" />
              </SelectTrigger>
              <SelectContent className="bg-[#161618] border-white/10 text-white">
                <SelectItem value="all">Todos os Solicitantes</SelectItem>
                {requesters.map(r => (
                  <SelectItem key={`req-${r.id}`} value={String(r.id)}>{r.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <Button 
            onClick={refreshData} 
            className="h-12 px-8 bg-orange-600 hover:bg-orange-700 text-white font-black rounded-xl shadow-lg shadow-orange-600/20"
          >
            Filtrar Dados
          </Button>
        </div>

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
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                {[
                  { label: 'COMPRAS EFETUADAS', value: `R$ ${stats.total.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`, description: `${orders.length} pedidos processados`, icon: TrendingUp, color: 'orange' },
                  { label: 'Saldo Financeiro (R-P)', value: `R$ ${stats.balance.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`, description: 'Receber - Pagar', icon: DollarSign, color: stats.balance >= 0 ? 'green' : 'red' },
                  { label: 'Obras Ativas (Filtro)', value: new Set([...orders.map(o => o.buildingId), ...financialTitles.map(f => f.buildingId)]).size || buildings.length, description: 'Com atividade no período', icon: Building2, color: 'orange' },
                  { label: 'Pedidos Solicitados', value: orders.length, description: 'Pedidos processados no período', icon: Package, color: 'orange' },
                ].map((kpi, i) => (
                  <Card key={i} className="bg-[#161618] border-white/5 shadow-2xl overflow-hidden relative group">
                    <div className="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                      <kpi.icon size={64} className="text-orange-500" />
                    </div>
                    <CardHeader className="pb-2">
                      <CardDescription className="text-[10px] font-black uppercase tracking-widest text-orange-500/70">{kpi.label}</CardDescription>
                      <CardTitle className={cn("text-3xl font-black tracking-tighter", kpi.color === 'red' ? 'text-red-500' : kpi.color === 'green' ? 'text-green-500' : 'text-white')}>
                        {kpi.value}
                      </CardTitle>
                      {kpi.description && (
                         <div className="text-xs text-gray-400 mt-2 font-bold">{kpi.description}</div>
                      )}
                    </CardHeader>
                    <div className="h-1 w-full bg-orange-600/20">
                      <div className="h-full bg-orange-600 w-1/3" />
                    </div>
                  </Card>
                ))}
              </div>

              {/* Financial Quick Summary */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <Card className="bg-gradient-to-br from-orange-600 to-orange-800 border-none shadow-2xl shadow-orange-900/20">
                  <CardContent className="p-8 flex items-center justify-between">
                    <div>
                      <p className="text-orange-200 text-xs font-black uppercase tracking-widest mb-2">Volume de Compras</p>
                      <h3 className="text-4xl font-black text-white">R$ {stats.total.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</h3>
                    </div>
                    <div className="bg-white/20 p-4 rounded-2xl">
                      <Package className="text-white" size={32} />
                    </div>
                  </CardContent>
                </Card>
                
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-8 flex items-center justify-between">
                    <div>
                      <p className="text-gray-500 text-xs font-black uppercase tracking-widest mb-2">Contas a Pagar</p>
                      <h3 className="text-4xl font-black text-red-500">R$ {stats.fTotal.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</h3>
                    </div>
                    <div className="bg-red-500/10 p-4 rounded-2xl">
                      <TrendingDown className="text-red-500" size={32} />
                    </div>
                  </CardContent>
                </Card>

                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardContent className="p-8 flex items-center justify-between">
                    <div>
                      <p className="text-gray-500 text-xs font-black uppercase tracking-widest mb-2">Contas a Receber</p>
                      <h3 className="text-4xl font-black text-green-500">R$ {stats.rTotal.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}</h3>
                    </div>
                    <div className="bg-green-500/10 p-4 rounded-2xl">
                      <TrendingUp className="text-green-500" size={32} />
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Main Charts Row */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
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
                  <CardContent className="h-[350px] pt-4">
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
                  <CardContent className="h-[350px]">
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
                        <Tooltip contentStyle={{ backgroundColor: '#161618', border: 'none', borderRadius: '8px' }} />
                        <Legend />
                      </PieChart>
                    </ResponsiveContainer>
                  </CardContent>
                </Card>
              </div>

              {/* Bottom Row */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                <Card className="bg-[#161618] border-white/5 shadow-2xl">
                  <CardHeader>
                    <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Faturamento por Fornecedor</CardTitle>
                  </CardHeader>
                  <CardContent className="h-[300px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={supplierData} layout="vertical">
                        <XAxis type="number" hide />
                        <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{fill: '#fff', fontSize: 10}} width={120} />
                        <Tooltip cursor={{fill: '#ffffff05'}} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
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
                                    {buildings.find(b => b.id === order.buildingId)?.name || order.buildingId}
                                  </TableCell>
                                  <TableCell className="text-xs text-gray-400">
                                    {creditors.find(c => c.id === order.supplierId)?.name || order.supplierId}
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
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-2xl font-black text-white flex items-center gap-3">
                  <Bell className="text-orange-500" size={28} />
                  Variações de Preço Detectadas
                </h3>
                <div className="flex items-center gap-4">
                  <Badge className="bg-orange-600 text-white font-black px-4 py-1 print:hidden">
                    {priceAlerts.length} {priceAlerts.length === 1 ? 'ALERTA ATIVO' : 'ALERTAS ATIVOS'}
                  </Badge>
                  <Button 
                    onClick={handlePrint}
                    className="bg-white text-black hover:bg-gray-200 font-black tracking-tight rounded-xl print:hidden"
                  >
                    Gerar PDF / Imprimir Lista
                  </Button>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {priceAlerts.slice(0, 8).map((alert, i) => (
                  <Card key={i} className="bg-[#161618] border-white/5 shadow-lg overflow-hidden relative">
                    <div className="absolute top-0 left-0 w-1.5 h-full bg-orange-600" />
                    <CardContent className="p-5 flex flex-col justify-between">
                      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
                        <div className="space-y-1">
                          <h4 className="text-sm font-black text-white leading-tight max-w-[250px] line-clamp-2" title={alert.item}>{alert.item}</h4>
                          <div className="flex items-center gap-2 text-[10px] text-gray-500">
                            <span>{safeFormat(alert.oldDate)}</span>
                            <ChevronRight size={10} />
                            <span className="font-bold text-orange-500/80">{safeFormat(alert.newDate)}</span>
                          </div>
                        </div>
                        
                        <div className="flex items-center gap-3 bg-black/40 p-3 rounded-xl border border-white/5 shrink-0">
                          <div className="text-center">
                            <p className="text-[9px] font-black uppercase text-gray-500 mb-0.5">Anterior</p>
                            <p className="text-xs font-bold text-gray-400">R$ {alert.oldPrice.toFixed(2)}</p>
                          </div>
                          <div className="w-px h-6 bg-white/10" />
                          <div className="text-center">
                            <p className="text-[9px] font-black uppercase text-orange-500 mb-0.5">Atual</p>
                            <p className="text-base font-black text-white pr-1">R$ {alert.newPrice.toFixed(2)}</p>
                          </div>
                          <div className="flex flex-col items-center bg-orange-600/10 px-3 py-1 rounded-lg border border-orange-600/20 min-w-[55px]">
                            <span className="text-[11px] font-black text-orange-500 whitespace-nowrap">
                              {alert.diff > 0 ? '+' : ''}{((alert.diff/alert.oldPrice)*100).toFixed(1)}%
                            </span>
                          </div>
                        </div>
                      </div>
                      {alert.history && alert.history.length > 0 && (
                        <div className="mt-4 h-16 w-full opacity-60 hover:opacity-100 transition-opacity">
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
                <CardContent className="p-0 overflow-x-auto overflow-y-auto max-h-[600px] custom-scrollbar print:overflow-visible print:max-h-none">
                  <Table className="print:text-black relative">
                    <TableHeader className="bg-black/80 sticky top-0 z-10 backdrop-blur-md print:bg-gray-100 print:relative border-b border-white/10">
                      <TableRow className="border-none print:border-gray-200">
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Item e Código</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Data</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Status</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Solicitante</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Comprador</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Prazos</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-center print:text-black">Qtd</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black">Vlr Unit</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black">Vlr Atual</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black">Valor Total</TableHead>
                      </TableRow>
                    </TableHeader>
                      <TableBody>
                        {allOrders.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={9} className="text-center py-10 text-gray-500 font-bold">
                              Nenhum pedido ou alerta registrado para o período e filtros selecionados.
                            </TableCell>
                          </TableRow>
                        ) : (() => {
                          return [...orders].sort((a,b) => (b.dateNumeric || 0) - (a.dateNumeric || 0))
                          .slice(0, isPrinting ? 999999 : 100).map((o, i) => {
                            // buyerId pode ser código numérico ou username — exibe direto
                            const comprador = o.buyerId || "N/A";
                            // requesterId agora é o requesterUser real da Solicitação (ex: "GEAN", "RAFAEL")
                            // Se não houver solicitação vinculada, mostra o createdBy (comprador direto)
                            const solicitante = o.requesterId && o.requesterId !== "0" ? o.requesterId : (o.createdBy || "N/A");
                            const itemsList = itemsDetailsMap[o.id];
                            
                            if (!itemsList || itemsList.length === 0) {
                              return (
                                <TableRow key={`alert-${o.id}-fallback`} className="border-white/5 hover:bg-white/5 transition-colors">
                                  <TableCell className="font-bold text-orange-500 whitespace-nowrap">Cod. {o.id} {(!itemsList) ? "(Carregando...)" : ""}</TableCell>
                                  <TableCell className="text-xs text-gray-500">{safeFormat(o.date)}</TableCell>
                                  <TableCell><Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">{o.status}</Badge></TableCell>
                                  <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{solicitante}</TableCell>
                                  <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{comprador}</TableCell>
                                  <TableCell className="text-xs text-gray-400">{o.paymentCondition || "N/A"}</TableCell>
                                  <TableCell className="text-xs font-mono text-gray-500 text-center">-</TableCell>
                                  <TableCell className="text-xs text-gray-400 font-mono text-right">-</TableCell>
                                  <TableCell className="text-xs text-gray-400 font-mono text-right">-</TableCell>
                                  <TableCell className="text-right font-black text-white whitespace-nowrap">R$ {o.totalAmount.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}</TableCell>
                                </TableRow>
                              );
                            }

                            return (
                              <Fragment key={`frag-${o.id}`}>
                                {itemsList.map((item: any, idx: number) => {
                                  if (!item) return null;
                                  const qty = Number(item.quantity || item.quantidade || 1);
                                  const realUnitValue = Number(item.netPrice || item.unitPrice || item.valorUnitario || 0);
                                  const totalAmount = qty * realUnitValue;
                                  
                                  const desc = item.resourceDescription || item.descricao;
                                  const vlrAtual = Number(latestPricesMap[desc]) || 0;
                                  const vlrBase = Number(baselinePricesMap[desc]) || realUnitValue;
                                  // Na tabela, só coramos de alerta se o valor subiu no novo orçamento!
                                  const isDiff = vlrAtual > vlrBase;
                                  
                                  return (
                                  <TableRow key={`alert-${o.id}-${idx}`} className="border-white/5 hover:bg-white/5 transition-colors">
                                    <TableCell className="font-bold text-orange-500" title={desc}>
                                      <div className="max-w-[200px] truncate">{desc || `Item ${idx+1}`}</div>
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
                                    <TableCell className={`text-xs font-mono text-right ${isDiff ? "text-orange-500 font-black" : "text-gray-400"}`}>
                                      {vlrAtual > 0 ? `R$ ${vlrAtual.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}` : '-'}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-white whitespace-nowrap">
                                      R$ {totalAmount.toLocaleString('pt-BR', {minimumFractionDigits:2, maximumFractionDigits:2})}
                                    </TableCell>
                                  </TableRow>
                                )})}
                              </Fragment>
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
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
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

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[500px]">
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

                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[500px]">
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
              className="grid grid-cols-1 lg:grid-cols-4 gap-6 h-[600px]"
            >
              {/* 1. Lista de Obras */}
              <Card className="lg:col-span-1 bg-[#161618] border-white/5 shadow-2xl flex flex-col h-full">
                <CardHeader className="pb-4">
                  <CardTitle className="text-white font-black uppercase text-sm tracking-tight">Obras Ativas</CardTitle>
                  <CardDescription className="text-xs">
                    {(buildings.filter(b => b.name.toLowerCase().includes(buildingSearch.toLowerCase()) || String(b.id).includes(buildingSearch)) || []).length} encontradas
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
                  {buildings.filter(b => b.name.toLowerCase().includes(buildingSearch.toLowerCase()) || String(b.id).includes(buildingSearch)).map(b => (
                    <button
                      key={b.id}
                      onClick={() => setSelectedMapBuilding(b.id)}
                      className={cn(
                        "w-full text-left p-3 rounded-xl transition-all border text-xs font-bold",
                        selectedMapBuilding === b.id 
                          ? "bg-orange-600/20 border-orange-500/50 text-orange-500" 
                          : "bg-black/20 border-white/5 text-gray-400 hover:bg-white/5 hover:text-white"
                      )}
                    >
                      <div className="truncate mb-1">{b.name}</div>
                      <div className="text-[9px] text-gray-500 uppercase flex items-center gap-1">
                        <MapIcon size={10} /> ID: {b.id}
                      </div>
                    </button>
                  ))}
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
                    <div className="space-y-6">
                      {(() => {
                        const currentBuilding = buildings.find(b => b.id === selectedMapBuilding);
                        const buildingOrders = allOrders.filter(o => String(o.buildingId) === String(selectedMapBuilding));
                        const buildingPayable = allFinancialTitles.filter(f => String(f.buildingId) === String(selectedMapBuilding));
                        
                        const totalOrders = buildingOrders.reduce((acc, curr) => acc + (curr.totalAmount || 0), 0);
                        const totalPayable = buildingPayable.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                        
                        return (
                          <>
                            <div className="flex items-center gap-3">
                              <div className="w-10 h-10 rounded-full bg-orange-500/20 flex items-center justify-center shrink-0">
                                <UserIcon size={20} className="text-orange-500" />
                              </div>
                              <div className="min-w-0">
                                <p className="text-[10px] font-black uppercase text-gray-500 text-left">Responsável Técnico</p>
                                <p className="text-sm font-bold text-white leading-tight text-left truncate">{currentBuilding?.engineer || "N/A"}</p>
                              </div>
                            </div>

                            <div className="space-y-4">
                              <div className="bg-black/20 p-4 rounded-xl border border-white/5">
                                <p className="text-[10px] font-black text-gray-500 uppercase mb-1">Volume de Compras</p>
                                <p className="text-2xl font-black text-orange-500 leading-tight">R$ {totalOrders.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</p>
                                <p className="text-[10px] text-gray-600 mt-2">{buildingOrders.length} pedidos em todo o histórico</p>
                              </div>
                              
                              <div className="bg-black/20 p-4 rounded-xl border border-white/5">
                                <p className="text-[10px] font-black text-gray-500 uppercase mb-1">Pendente a Pagar</p>
                                <p className="text-2xl font-black text-red-500 leading-tight">R$ {totalPayable.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</p>
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
            className="fixed bottom-6 right-6 z-[9999] bg-gradient-to-br from-orange-600 to-orange-800 p-6 rounded-2xl shadow-[0_20px_50px_rgba(234,88,12,0.3)] border border-white/10 flex items-start gap-4 max-w-sm"
          >
            <div className="bg-white/20 p-3 rounded-xl shadow-inner shrink-0">
              <Package className="text-white" size={28} />
            </div>
            <div className="pr-4">
              <h4 className="text-white font-black tracking-wide">NOVA COMPRA REGISTRADA</h4>
              <p className="text-orange-100 text-sm mt-1 leading-snug">Pedido <span className="font-bold">#{newOrderAlert.id}</span> processado no valor de <span className="font-bold">R$ {newOrderAlert.totalAmount.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</span>.</p>
              <p className="text-orange-300 text-xs mt-2 font-bold uppercase tracking-wider">{buildings.find(b => b.id === newOrderAlert.buildingId)?.name || 'Obra não identificada'}</p>
            </div>
            <button onClick={() => setNewOrderAlert(null)} className="absolute top-4 right-4 text-white/50 hover:text-white transition-colors">
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
    </>
  );
}
