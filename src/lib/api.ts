import axios from 'axios';

export const api = axios.create({
  baseURL: '/api/sienge',
});

export interface Building {
  id: number;
  name: string;
  latitude?: number;
  longitude?: number;
  address?: string;
  engineer?: string;
  companyId?: number;
}

export interface User {
  id: string;
  name: string;
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
