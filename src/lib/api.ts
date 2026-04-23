import axios from 'axios';

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
  timeout: 15000,
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
  const stored = localStorage.getItem(SESSION_KEY);
  return stored ? JSON.parse(stored) : null;
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

// ========== TYPE DEFINITIONS ==========

export interface LogisticsLocation {
  id: number;
  code: string;
  name: string;
  address?: string;
  latitude?: number;
  longitude?: number;
  type: string;
  source: string;
}

export interface Building {
  id: number;
  name: string;
  code?: string;
  latitude?: number;
  longitude?: number;
  address?: string;
  companyId?: number;
  company_id?: number;
  company_name?: string;
  building_type?: string;
  engineer?: string;
}

export interface DirectoryUser {
  id: string;
  name: string;
  email?: string;
  active: boolean;
}

export interface AuthUser {
  id: number;
  email: string;
  username?: string;
  full_name?: string;
  name?: string;          // alias de full_name — usado no App legado
  role: string;
  department?: string;
  is_active?: boolean;
}

export interface Creditor {
  id: number;
  name: string;
  cnpj?: string;
  city?: string;
  state?: string;
  active: boolean;
}

export interface Company {
  id: number;
  name: string;
  cnpj?: string;
  trade_name?: string;
  companyName?: string;
  legalName?: string;
}

export interface Client {
  id: number;
  name: string;
  fantasy_name?: string;
  cnpj_cpf?: string;
  city?: string;
  state?: string;
  email?: string;
  phone?: string;
  status?: string;
}

export interface SummaryCard {
  label: string;
  value: number;
}

export interface DashboardSummary {
  cards: SummaryCard[];
  companies_by_buildings: Array<{ company_name: string; total: number }>;
  creditor_states: Array<{ state: string; total: number }>;
  client_cities: Array<{ city: string; total: number }>;
  active_directory_users: number;
}

export interface User {
  id: string | number;
  name: string;
}

export interface PurchaseOrder {
  id: number;
  buildingId: number;
  companyId?: string;
  buyerId?: string;
  date: string;
  dateNumeric: number;
  totalAmount: number;
  supplierId?: string | number;
  status: string;
  paymentCondition: string;
  deliveryDate: string;
  internalNotes: string;
  createdBy: string;
  requesterId: string;
  items?: any[];
}

export interface PriceAlert {
  item: string;
  oldPrice: number;
  newPrice: number;
  diff: number;
  oldDate: string;
  newDate: string;
  history: Array<{ price: number; date: string; orderId?: number; buyerId?: string; creditorId?: string }>;
}
