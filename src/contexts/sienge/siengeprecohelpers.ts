import type { PriceAlert, PurchaseOrder } from '../../lib/types';

type HistoryEntry = {
  price: number;
  date: string;
  orderId?: number;
  buyerId?: string;
  creditorId?: string;
};

type ItemHistoryMap = Record<string, HistoryEntry[]>;

export function montarHistoricoGlobalItens({
  orders,
  itemsDetailsMap,
  quotationsMap,
}: {
  orders: PurchaseOrder[];
  itemsDetailsMap: Record<string, any>;
  quotationsMap: Record<string, any>;
}): ItemHistoryMap {
  const historyMap: ItemHistoryMap = {};

  const addEntry = (
    name: string,
    price: number,
    date: string,
    orderId?: number,
    buyerId?: string,
    creditorId?: string,
  ) => {
    if (!name || !price || price <= 0) return;
    if (!historyMap[name]) historyMap[name] = [];

    const duplicate = historyMap[name].some(
      (entry) => entry.price === price && entry.date === date && entry.creditorId === creditorId,
    );
    if (!duplicate) {
      historyMap[name].push({ price, date, orderId, buyerId, creditorId });
    }
  };

  if (orders.length === 0) return historyMap;

  orders.forEach((order) => {
    const buyerId = String(order.buyerId || '');
    const creditorId = String((order as any).supplierId || (order as any).creditorId || '');

    const actualItems = itemsDetailsMap[order.id] || order.items;
    if (actualItems) {
      actualItems.forEach((item: any) => {
        const name = item.description || item.resourceDescription || item.descricao;
        const price = Number(item.unitPrice || item.valorUnitario || item.netPrice || 0);
        addEntry(name, price, order.date, order.id, buyerId, creditorId);
      });
    }

    const quotationEntry = quotationsMap[String(order.id)];
    const quotesArray: any[] = Array.isArray(quotationEntry)
      ? quotationEntry
      : quotationEntry?.quotes && Array.isArray(quotationEntry.quotes)
      ? quotationEntry.quotes
      : [];

    quotesArray.forEach((quote: any) => {
      const quoteOrderId = quote.orderId || order.id;
      const quoteSupplierId = String(quote.supplierId || quote.creditorId || quote.idFornecedor || '');
      const quoteDate = quote.date || order.date;
      const quoteItems = quote.items || quote.itens || [];

      quoteItems.forEach((quoteItem: any) => {
        const name = quoteItem.description || quoteItem.descricao || quoteItem.resourceDescription;
        const price = Number(quoteItem.unitPrice || quoteItem.valorUnitario || quoteItem.netPrice || 0);
        addEntry(name, price, quoteDate, quoteOrderId, buyerId, quoteSupplierId);
      });
    });
  });

  return historyMap;
}

export function montarAlertasPreco(globalItemHistory: ItemHistoryMap): PriceAlert[] {
  const keys = Object.keys(globalItemHistory);
  if (keys.length === 0) return [];

  const alerts: PriceAlert[] = [];

  keys.forEach((name) => {
    const history = globalItemHistory[name].sort(
      (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
    );

    if (history.length < 2) return;

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
        history: history.slice().reverse(),
      });
    }
  });

  return alerts;
}
