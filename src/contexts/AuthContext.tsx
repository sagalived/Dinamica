import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';
import {
  api,
  clearAuthToken,
  clearSessionUser as clearStoredSessionUser,
  getAuthToken,
  getSessionUser as getStoredSessionUser,
  setAuthToken,
  setSessionUser as setStoredSessionUser,
} from '../lib/api';

import type { AuthUser } from '../lib/types';

interface AuthContextData {
  sessionUser: AuthUser | null;
  authReady: boolean;
  isAdmin: boolean;
  isRestrictedUser: boolean;
  login: (user: AuthUser, token: string) => void;
  logout: () => void;
  checkAuth: () => Promise<void>;
}

const AuthContext = createContext<AuthContextData | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [sessionUser, setSessionUser] = useState<AuthUser | null>(null);
  const [authReady, setAuthReady] = useState(false);

  const isAdmin = sessionUser?.role === 'developer' || sessionUser?.role === 'admin';
  const isRestrictedUser = sessionUser?.role === 'user';

  const checkAuth = useCallback(async () => {
    const safeLogout = () => {
      clearAuthToken();
      clearStoredSessionUser();
      setSessionUser(null);
    };

    try {
      const token = getAuthToken();
      const storedUser = getStoredSessionUser();

      if (token && storedUser) {
        try {
          // Valida o token no backend (se inválido, faz logout)
          const response = await api.get('/auth/me');
          const user = (response.data && response.data.user) ? (response.data.user as AuthUser) : null;
          if (user) {
            setSessionUser({ ...user, name: (user as any).full_name || (user as any).name });
          } else {
            safeLogout();
          }
        } catch (error) {
          console.error('Erro ao verificar sessão:', error);
          safeLogout();
        }
      } else {
        safeLogout();
      }
    } catch (error) {
      console.error('Falha ao restaurar sessão:', error);
      safeLogout();
    } finally {
      setAuthReady(true);
    }
  }, []);

  useEffect(() => {
    checkAuth();
  }, [checkAuth]);

  const login = useCallback((user: AuthUser, token: string) => {
    setAuthToken(token);
    const enrichedUser: AuthUser = { ...user, name: (user as any).full_name || (user as any).name };
    setStoredSessionUser(enrichedUser);
    setSessionUser(enrichedUser);
  }, []);

  const logout = useCallback(() => {
    clearAuthToken();
    clearStoredSessionUser();
    setSessionUser(null);
  }, []);

  return (
    <AuthContext.Provider value={{ sessionUser, authReady, isAdmin, isRestrictedUser, login, logout, checkAuth }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
