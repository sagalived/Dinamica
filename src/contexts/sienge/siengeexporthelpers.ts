export function gerarCsvPedidosSienge({
  buildings,
  users,
  orders,
  safeFormat,
  translateStatusLabel,
}: {
  buildings: any[];
  users: any[];
  orders: any[];
  safeFormat: (value: any) => string;
  translateStatusLabel: (value: any) => string;
}): string {
  const bMap: Record<string, string> = {};
  buildings.forEach((b) => {
    bMap[b.id] = b.name;
  });

  const uMap: Record<string, string> = {};
  users.forEach((u) => {
    uMap[u.id] = u.name;
  });

  const headers = 'ID;Obra;Comprador;Data;Valor;Status\n';
  const rows = orders
    .map((o) => {
      const obra = bMap[o.buildingId] || o.buildingId;
      const buyerKey = String(o.buyerId || '');
      const user = uMap[buyerKey] || o.buyerId;
      const valorStr = String(o.totalAmount || 0).replace('.', ',');
      return `${o.id};"${obra}";"${user}";${safeFormat(o.date)};${valorStr};${translateStatusLabel(o.status)}`;
    })
    .join('\n');

  return `\uFEFF${headers}${rows}`;
}

export function gerarCsvRelatorioSienge({
  buildings,
  users,
  orders,
  financialTitles,
  receivableTitles,
  safeFormat,
  translateStatusLabel,
}: {
  buildings: any[];
  users: any[];
  orders: any[];
  financialTitles: any[];
  receivableTitles: any[];
  safeFormat: (value: any) => string;
  translateStatusLabel: (value: any) => string;
}): string {
  const bMap: Record<string, string> = {};
  buildings.forEach((b) => {
    bMap[b.id] = b.name;
  });

  const uMap: Record<string, string> = {};
  users.forEach((u) => {
    uMap[u.id] = u.name;
  });

  const headers = 'Tipo;ID;Obra;Comprador/Credor/Cliente;Data;Valor;Status\n';
  const csvRows: string[] = [];

  orders.forEach((o) => {
    const obra = bMap[o.buildingId] || String(o.buildingId);
    const buyerKey = String(o.buyerId || '');
    const user = uMap[buyerKey] || String(o.buyerId);
    const valorStr = String(o.totalAmount || 0).replace('.', ',');
    csvRows.push(
      `Pedido;${o.id};"${obra}";"${user}";${safeFormat(o.date)};${valorStr};${translateStatusLabel(o.status)}`,
    );
  });

  financialTitles.forEach((f) => {
    const obra = bMap[f.buildingId] || String(f.buildingId);
    const credor = f.creditorName || 'S/N';
    const valorStr = String(f.amount || 0).replace('.', ',');
    csvRows.push(
      `A Pagar;${f.id};"${obra}";"${credor}";${safeFormat(f.dueDate)};${valorStr};${translateStatusLabel(f.status)}`,
    );
  });

  receivableTitles.forEach((r) => {
    const obra = bMap[r.buildingId] || String(r.buildingId);
    const cliente = r.clientName || 'S/N';
    const valorStr = String(r.amount || 0).replace('.', ',');
    csvRows.push(
      `A Receber;${r.id};"${obra}";"${cliente}";${safeFormat(r.dueDate)};${valorStr};${translateStatusLabel(r.status)}`,
    );
  });

  return `\uFEFF${headers}${csvRows.join('\n')}`;
}

export function baixarCsv(texto: string, fileName: string): void {
  const blob = new Blob([texto], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.setAttribute('download', fileName);
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
