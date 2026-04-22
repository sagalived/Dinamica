import { cn } from '../lib/utils';
import { LayoutDashboard, DollarSign, Building2, Truck, Bell } from 'lucide-react';

interface NavProps {
  activeTab: string;
  setActiveTab: (val: string) => void;
  isRestrictedUser?: boolean;
}

export function NavigationMenu({ activeTab, setActiveTab, isRestrictedUser }: NavProps) {
  if (isRestrictedUser) {
    return (
      <nav className="hidden xl:flex items-center gap-2">
        <button
          onClick={() => setActiveTab('logistics')}
          className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-bold uppercase transition-all tracking-wider",
            activeTab === 'logistics' ? "bg-orange-600 text-white" : "text-gray-400"
          )}
        >
          LOGÍSTICA
        </button>
      </nav>
    );
  }

  const menuConfig = [
    {
      id: 'dashboard',
      label: 'DASHBOARD',
      options: [
        { id: 'dashboard', label: 'GERAL' },
        { id: 'dashboard-financeiro', label: 'FINANCEIROS' },
        { id: 'dashboard-obras', label: 'OBRAS' },
        { id: 'dashboard-logistica', label: 'LOGISTICA' }
      ]
    },
    {
      id: 'financeiro',
      label: 'FINANCEIRO',
      options: [
        { id: 'finance', label: 'VALORES' },
        { id: 'financeiro-fluxo', label: 'FLUXO DE CAIXA' },
        { id: 'alerts', label: 'ALERTA' }
      ]
    },
    {
      id: 'obras',
      label: 'OBRAS',
      options: [
        { id: 'obras-diario', label: 'DIARIO DE OBRAS' },
        { id: 'map', label: 'VALORES' },
        { id: 'obras-alerta', label: 'ALERTA' }
      ]
    },
    {
      id: 'logistica',
      label: 'LOGISTICA',
      options: [
        { id: 'logistics', label: 'ACOMPANHAMENTO' }
      ]
    }
  ];

  return (
    <nav className="hidden xl:flex items-center gap-2">
      {menuConfig.map(menu => {
        const isActiveContext = activeTab.startsWith(menu.id) || menu.options.some(o => o.id === activeTab);
        return (
          <div key={menu.id} className="relative group">
            <button 
              onClick={() => setActiveTab(menu.options[0].id)}
              className={cn(
                "flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-bold uppercase transition-all tracking-widest",
                isActiveContext
                  ? "bg-orange-600 text-white shadow-lg shadow-orange-600/20"
                  : "text-gray-400 hover:text-white hover:bg-white/5"
              )}
            >
              <span>{menu.label}</span>
            </button>
            
            <div className="absolute left-0 top-full pt-2 opacity-0 translate-y-2 pointer-events-none group-hover:opacity-100 group-hover:translate-y-0 group-hover:pointer-events-auto transition-all duration-200 z-50">
              <div className="flex flex-col bg-[#161618] border border-white/10 rounded-xl shadow-2xl p-2 min-w-[220px] gap-1">
                {menu.options.map(opt => (
                  <button
                    key={opt.id}
                    onClick={() => setActiveTab(opt.id)}
                    className={cn(
                      "text-left px-4 py-2.5 rounded-lg text-sm font-bold uppercase transition-all tracking-wider",
                      activeTab === opt.id 
                        ? "bg-orange-600/20 text-orange-500" 
                        : "text-gray-400 hover:bg-white/5 hover:text-white"
                    )}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>
          </div>
        );
      })}
    </nav>
  );
}
