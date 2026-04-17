import axios from 'axios';

export const api = axios.create({
  baseURL: '/api/sienge',
});

export const authApi = axios.create({
  baseURL: '/api/auth',
  timeout: 8000,
});

export interface Building {
  id: number;
  name: string;
  code?: string;
  latitude?: number;
  longitude?: number;
  address?: string;
  engineer?: string;
  companyId?: number;
}

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

export interface User {
  id: string;
  name: string;
}

export interface AuthUser {
  id: number;
  username: string;
  email: string;
  name: string;
  role: string;
  department?: string;
}

export interface Creditor {
  id: number;
  name: string;
  cnpj?: string;
}

export interface Company {
  id: number;
  name: string;
  cnpj?: string;
}

export interface OrderItem {
  id: number;
  description: string;
  quantity: number;
  unitPrice: number;
  totalPrice: number;
  unit: string;
}

export interface PurchaseOrder {
  id: number;
  buildingId: number;
  buyerId: string;
  date: string;
  dateNumeric?: number;
  totalAmount: number;
  supplierId: number;
  status: string;
  paymentCondition: string;
  deliveryDate?: string;
  internalNotes?: string;
  createdBy?: string;
  requesterId?: string;
  items?: OrderItem[];
}

export interface PriceAlert {
  item: string;
  oldPrice: number;
  newPrice: number;
  diff: number;
  oldDate: string;
  newDate: string;
  history?: { price: number; date: string }[];
}
