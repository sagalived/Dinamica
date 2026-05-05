import React, { useMemo, useState, useEffect } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useSienge } from '../contexts/SiengeContext';
import { useAuth } from '../contexts/AuthContext';
import { useTheme } from '../contexts/ThemeContext';
import { NavigationMenu } from './NavigationMenu';
import { cn } from '../lib/utils';
import { SlidersHorizontal, ChevronDown, RefreshCw, Sun, Moon, LogOut, User as UserIcon, Download, Calendar as CalendarIcon, Truck, LayoutDashboard, DollarSign, Bell, Map as MapIcon } from 'lucide-react';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { format } from 'date-fns';
import { Button } from './ui/button';
import logoWordmark from '../assets/dinamica-wordmark.svg';
import logoWordmarkDark from '../assets/dinamica-wordmark-dark.svg';

export function LayoutPrincipal() {
  const { isDark, toggleThemeMode, themeMode } = useTheme();
  const { sessionUser, logout, isRestrictedUser } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const path = location.pathname;
  const isDashboardGeral = path === '/';

  const {
    syncSienge, syncing, syncProgress, apiStatus,
    applyFilters,
    globalPeriodMode, setGlobalPeriodMode,
    startDate, setStartDate, endDate, setEndDate,
    selectedCompany, setSelectedCompany,
    selectedUser, setSelectedUser,
    selectedRequester, setSelectedRequester,
    fcSelectedBuilding, setFcSelectedBuilding,
    companies, users, requesters, buildings
  } = useSienge();

  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  useEffect(() => {
    // Na aba Dashboard/Geral esses filtros não são usados.
    if (!isDashboardGeral) return;
    if (selectedUser !== 'all') setSelectedUser('all');
    if (selectedRequester !== 'all') setSelectedRequester('all');
  }, [isDashboardGeral, selectedRequester, selectedUser, setSelectedRequester, setSelectedUser]);

  const availableTabs = useMemo(() => (
    isRestrictedUser
      ? [{ id: '/logistica', label: 'Logística', icon: Truck }]
      : [
          { id: '/', label: 'Dashboard', icon: LayoutDashboard },
          { id: '/financeiro', label: 'Financeiro', icon: DollarSign },
          { id: '/financeiro/alerta', label: 'Alertas', icon: Bell },
          { id: '/obras/mapa', label: 'Mapa de Obras', icon: MapIcon },
          { id: '/logistica', label: 'Logística', icon: Truck },
          { id: '/acessos', label: 'Acessos', icon: UserIcon },
        ]
  ), [isRestrictedUser]);

  // Map route to activeTab for NavigationMenu
  const activeTab = path.startsWith('/financeiro') ? 'finance' : 
                    path.startsWith('/logistica') ? 'logistics' :
                    path.startsWith('/acessos') ? 'access' :
                    path.startsWith('/obras') ? 'map' : 'dashboard';

  const setActiveTab = (id: string) => {
    const routeMap: Record<string, string> = {
      'dashboard': '/',
      'finance': '/financeiro',
      'alerts': '/financeiro/alerta',
      'map': '/obras/mapa',
      'logistics': '/logistica',
      'access': '/acessos',
      'obras-diario': '/obras/diario',
      'financeiro-fluxo': '/financeiro/fluxo',
      'financeiro-leandro': '/financeiro/leandro'
    };
    if (routeMap[id]) navigate(routeMap[id]);
  };

  const showFilters = !['/logistica', '/acessos', '/obras/diario', '/financeiro/fluxo', '/financeiro/leandro'].includes(path);

  const buildingFilterOptions = useMemo(() => {
    if (selectedCompany === 'all') return buildings;
    return (buildings || []).filter((b: any) => String(b?.companyId) === String(selectedCompany));
  }, [buildings, selectedCompany]);

  useEffect(() => {
    if (!showFilters) return;
    if (selectedCompany === 'all') return;
    if (fcSelectedBuilding === 'all') return;
    const selected = (buildings || []).find((b: any) => String(b?.id) === String(fcSelectedBuilding));
    if (selected && String(selected?.companyId) !== String(selectedCompany)) {
      setFcSelectedBuilding('all');
    }
  }, [buildings, fcSelectedBuilding, selectedCompany, setFcSelectedBuilding, showFilters]);

  const downloadData = () => {
    // Moved to Context or we can re-implement here later
    alert("Função de download movida temporariamente");
  };

  if (!sessionUser) return null;

  return (
    <div className={cn(
      "min-h-screen overflow-x-hidden font-sans transition-colors duration-300", 
      isDark ? "bg-[#0F1115] text-slate-100" : "bg-[#F3F5F7] text-[#102A40]"
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
          <NavigationMenu activeTab={activeTab} setActiveTab={setActiveTab as any} isRestrictedUser={isRestrictedUser} />

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
                onClick={logout}
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
                isDark ? "border-slate-700 bg-slate-900 text-slate-100 hover:bg-slate-800" : "border-slate-200 bg-white text-slate-700 hover:bg-slate-100"
              )}
            >
              {isDark ? <Sun size={16} /> : <Moon size={16} />}
              <span>{isDark ? 'Dia' : 'Noite'}</span>
            </Button>
            
            <Button
              onClick={syncSienge}
              disabled={syncing}
              className={cn(
                "relative overflow-hidden text-white font-bold rounded-xl h-11 px-3 2xl:px-4 gap-2 shrink-0 min-w-[176px]",
                isDark ? "bg-[#1B3C58] hover:bg-[#234b6e]" : "bg-[#102A40] hover:bg-[#173A57]"
              )}
            >
              {syncing && (
                <span
                  aria-hidden
                  className={cn(
                    "absolute inset-y-0 left-0",
                    isDark ? "bg-white/10" : "bg-white/20"
                  )}
                  style={{ width: `${Math.max(0, Math.min(100, syncProgress || 0))}%` }}
                />
              )}
              <span className="relative z-10 flex items-center gap-2">
                <RefreshCw size={16} className={cn(syncing && "animate-spin")} />
                <span>{syncing ? "Atualizando..." : "Atualizar Dados"}</span>
              </span>
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

          {/* Mobile Actions (Menu Toggle) */}
          <div className="flex xl:hidden items-center gap-2">
            <button
              onClick={syncSienge}
              disabled={syncing}
              className={cn("w-9 h-9 flex items-center justify-center rounded-xl text-white", isDark ? "bg-[#1B3C58]" : "bg-[#102A40]")}
            >
              <RefreshCw size={16} className={cn(syncing && "animate-spin")} />
            </button>
            <button
              onClick={toggleThemeMode}
              className={cn("w-9 h-9 flex items-center justify-center rounded-xl", isDark ? "bg-slate-900 text-slate-100" : "bg-white text-slate-700 border border-slate-200")}
            >
              {isDark ? <Sun size={15} /> : <Moon size={15} />}
            </button>
            <button
              onClick={logout}
              className={cn("w-9 h-9 flex items-center justify-center rounded-xl", isDark ? "bg-slate-800 text-slate-200" : "bg-slate-200 text-slate-700")}
            >
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </header>

      <main className="w-full max-w-full 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 py-6 sm:py-10 pb-24 xl:pb-10 pt-24 xl:pt-10">
        
        {/* Filters Topbar */}
        {showFilters && (
          <div className="mb-6 sm:mb-10 bg-[#161618] rounded-2xl border border-white/5 shadow-xl print:hidden overflow-hidden mt-8 xl:mt-0">
            <button onClick={() => setMobileFiltersOpen(!mobileFiltersOpen)} className="w-full flex items-center justify-between p-4 sm:p-6 md:cursor-default">
              <div className="flex items-center gap-3">
                <SlidersHorizontal size={16} className="text-orange-500" />
                <span className="text-sm font-black uppercase tracking-widest text-orange-500">Filtros</span>
              </div>
              <ChevronDown size={16} className={cn("text-gray-500 transition-transform md:hidden", mobileFiltersOpen && "rotate-180")} />
            </button>
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
                      Últimos 12 meses
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
                    <PopoverTrigger asChild>
                      <Button variant="outline" className={cn("w-full sm:w-[160px] bg-black/40 border-white/10 h-11 rounded-xl justify-start text-left font-bold text-white", !startDate && "text-gray-400")}>
                        <CalendarIcon className="mr-2 h-4 w-4 text-orange-500" />
                        {startDate ? format(startDate, "dd/MM/yyyy") : <span>Início</span>}
                      </Button>
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
                    <PopoverTrigger asChild>
                      <Button variant="outline" className={cn("w-full sm:w-[160px] bg-black/40 border-white/10 h-11 rounded-xl justify-start text-left font-bold text-white", !endDate && "text-gray-400")}>
                        <CalendarIcon className="mr-2 h-4 w-4 text-orange-500" />
                        {endDate ? format(endDate, "dd/MM/yyyy") : <span>Fim</span>}
                      </Button>
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

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Empresa (Sienge)</Label>
                  <Select value={selectedCompany} onValueChange={setSelectedCompany}>
                    <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                      <span className="truncate">{selectedCompany === 'all' ? 'Todas as Empresas' : companies?.find((c: any) => String(c.id) === selectedCompany)?.name || 'Todas as Empresas'}</span>
                    </SelectTrigger>
                    <SelectContent className="bg-[#161618] border-white/10 text-white">
                      <SelectItem value="all">Todas as Empresas</SelectItem>
                      {companies?.map((c: any) => (
                        <SelectItem key={`empresa-${c.id}`} value={String(c.id)}>{c.name || `Empresa ${c.id}`}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obra</Label>
                  <Select value={fcSelectedBuilding} onValueChange={setFcSelectedBuilding}>
                    <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                      <span className="truncate">{fcSelectedBuilding === 'all' ? 'Todas as Obras' : buildings?.find((b: any) => String(b.id) === fcSelectedBuilding)?.name || `Obra ${fcSelectedBuilding}`}</span>
                    </SelectTrigger>
                    <SelectContent className="bg-[#161618] border-white/10 text-white">
                      <SelectItem value="all">Todas as Obras</SelectItem>
                      {buildingFilterOptions?.map((b: any) => (
                        <SelectItem key={`obra-${b.id}`} value={String(b.id)}>{b.name || `Obra ${b.id}`}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {!isDashboardGeral && (
                  <>
                    <div className="space-y-2 flex-1 min-w-[180px]">
                      <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Comprador</Label>
                      <Select value={selectedUser} onValueChange={setSelectedUser}>
                        <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                          <span className="truncate">{selectedUser === 'all' ? 'Todos' : users?.find((u: any) => String(u.id) === selectedUser)?.name || 'Todos'}</span>
                        </SelectTrigger>
                        <SelectContent className="bg-[#161618] border-white/10 text-white">
                          <SelectItem value="all">Todos os Compradores</SelectItem>
                          {users?.map((u: any) => (
                            <SelectItem key={`user-${u.id}`} value={String(u.id)}>{u.name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2 flex-1 min-w-[180px]">
                      <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Solicitante</Label>
                      <Select value={selectedRequester} onValueChange={setSelectedRequester}>
                        <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                          <span className="truncate">{selectedRequester === 'all' ? 'Todos' : requesters?.find((r: any) => String(r.id) === selectedRequester)?.name || 'Todos'}</span>
                        </SelectTrigger>
                        <SelectContent className="bg-[#161618] border-white/10 text-white">
                          <SelectItem value="all">Todos os Solicitantes</SelectItem>
                          {requesters?.map((r: any) => (
                            <SelectItem key={`requester-${r.id}`} value={String(r.id)}>{r.name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </>
                )}

                <Button 
                  onClick={() => {
                    applyFilters();
                    setMobileFiltersOpen(false);
                  }} 
                  className="h-11 px-6 bg-orange-600 hover:bg-orange-700 text-white font-black rounded-xl shadow-lg shadow-orange-600/20 w-full sm:w-auto"
                >
                  Confirmar Filtro
                </Button>
              </div>
            </div>
          </div>
        )}

        <Outlet />

      </main>
    </div>
  );
}
