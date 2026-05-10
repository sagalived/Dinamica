import axios from 'axios';

import type { AuthUser } from './types';

// API instances
export const api = axios.create({
  baseURL: '/api',
  timeout: 15000,
});

export const authApi = axios.create({
  baseURL: '/api/auth',
  timeout: 15000,
});

export const kanbanApi = axios.create({
  baseURL: '/api/kanban',
  timeout: 15000,
});

export const sienge = axios.create({
  baseURL: '/api/sienge',
  timeout: 120000, // 2 min — filtered sem data pode retornar muitos registros
});

// Token management
const AUTH_TOKEN_KEY = 'dinamica_token';
const SESSION_KEY = 'dinamica_session';

export function setAuthToken(token: string) {
  localStorage.setItem(AUTH_TOKEN_KEY, token);
  // Apply token to all axios instances
  const authHeader = `Bearer ${token}`;
  api.defaults.headers.common['Authorization'] = authHeader;
  kanbanApi.defaults.headers.common['Authorization'] = authHeader;
  sienge.defaults.headers.common['Authorization'] = authHeader;
}

export function getAuthToken() {
  return localStorage.getItem(AUTH_TOKEN_KEY);
}

export function clearAuthToken() {
  localStorage.removeItem(AUTH_TOKEN_KEY);
  // Remove from all axios instances
  delete api.defaults.headers.common['Authorization'];
  delete kanbanApi.defaults.headers.common['Authorization'];
  delete sienge.defaults.headers.common['Authorization'];
}

export function setSessionUser(user: AuthUser) {
  localStorage.setItem(SESSION_KEY, JSON.stringify(user));
}

export function getSessionUser(): AuthUser | null {
  try {
    const stored = localStorage.getItem(SESSION_KEY);
    if (!stored) return null;
    return JSON.parse(stored);
  } catch {
    // Sessao corrompida em localStorage nao deve derrubar a aplicacao.
    localStorage.removeItem(SESSION_KEY);
    return null;
  }
}

export function clearSessionUser() {
  localStorage.removeItem(SESSION_KEY);
}

// Request interceptor: add Authorization header
function attachTokenInterceptor(instance: any) {
  instance.interceptors.request.use((config: any) => {
    const token = getAuthToken();
    if (token) {
      config.headers = config.headers || {};
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  }, (error: any) => Promise.reject(error));
}

// Apply interceptors to all instances
[api, authApi, kanbanApi, sienge].forEach(attachTokenInterceptor);

// Response interceptor: handle 401 globally (optional)
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      clearAuthToken();
      clearSessionUser();
      // Trigger logout event - can be caught by App component
      window.dispatchEvent(new CustomEvent('logout'));
    }
    return Promise.reject(error);
  }
);
