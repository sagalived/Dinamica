import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { ThemeProvider } from './contexts/ThemeContext';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import { SiengeProvider } from './contexts/SiengeContext';
import { LayoutPrincipal } from './components/LayoutPrincipal';
import { LoginScreen } from './components/LoginScreen';

// Import das abas
import { DashboardGeral } from './tabs/dashboard/Geral';
import { LogisticsTab } from './tabs/logistica/LogisticaTab';
import { DiarioObras as ObrasTab } from './tabs/obras/ObrasTab';
import { MapaTab } from './tabs/obras/MapaTab';
import { ObrasAlerta } from './tabs/obras/Alerta';
import { FinanceiroValores } from './tabs/financeiro/Valores';
import { FinanceiroFluxoTab } from './tabs/financeiro/FluxoCaixa';
import { CentroDeCustoTab } from './tabs/financeiro/CentroDeCusto';
import { AccessControlTab } from './components/AccessControl';
import { ContratosTab } from './tabs/juridico/ContratosTab.tsx';
import { cn } from './lib/utils';
import { useTheme } from './contexts/ThemeContext';

function AppInner() {
  const { authReady, sessionUser, login, isRestrictedUser } = useAuth();
  const { themeMode, toggleThemeMode, isDark } = useTheme();

  if (!authReady) {
    return <div className={cn("min-h-screen", isDark ? "bg-[#0F1115]" : "bg-[#F3F5F7]")} />;
  }

  if (!sessionUser) {
    return <LoginScreen onLogin={login} themeMode={themeMode} onToggleTheme={toggleThemeMode} />;
  }

  return (
    <Router>
      <Routes>
        <Route path="/" element={<LayoutPrincipal />}>
          {isRestrictedUser ? (
            // Restricted User Routes
            <>
              <Route index element={<Navigate to="/logistica" replace />} />
              <Route path="logistica" element={<LogisticsTab />} />
              <Route path="*" element={<Navigate to="/logistica" replace />} />
            </>
          ) : (
            // Normal User Routes
            <>
              <Route index element={<DashboardGeral />} />
              
              <Route path="financeiro">
                <Route index element={<FinanceiroValores />} />
                <Route path="alerta" element={<Navigate to="/obras/alerta" replace />} />
                <Route path="fluxo" element={<FinanceiroFluxoTab />} />
                <Route path="centro-custo" element={<CentroDeCustoTab />} />
              </Route>

              <Route path="obras">
                <Route path="mapa" element={<MapaTab />} />
                <Route path="diario" element={<ObrasTab />} />
                <Route path="alerta" element={<ObrasAlerta />} />
              </Route>

              <Route path="logistica" element={<LogisticsTab />} />

              <Route path="juridico">
                <Route index element={<Navigate to="/juridico/contratos" replace />} />
                <Route path="contratos" element={<ContratosTab />} />
              </Route>

              <Route path="acessos" element={<AccessControlTab />} />
              
              <Route path="*" element={<Navigate to="/" replace />} />
            </>
          )}
        </Route>
      </Routes>
    </Router>
  );
}

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
        <SiengeProvider>
          <AppInner />
        </SiengeProvider>
      </AuthProvider>
    </ThemeProvider>
  );
}
