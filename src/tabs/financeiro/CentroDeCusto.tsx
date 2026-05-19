/**
 * Centro de Custo — consolidação (via contexto SIENGE) para o período/empresa/obra
 * selecionados na barra de filtros do LayoutPrincipal.
 */

import React, { useEffect, useMemo, useState } from 'react';

import { motion } from 'framer-motion';
import {
  ArrowDown,
  ArrowUp,
  Building2,
  Download,
  FileText,
  Wallet,
} from 'lucide-react';

import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../components/ui/table';

import { cn } from '../../lib/utils';
import { useSienge } from '../../contexts/SiengeContext';
import { calcularFluxoCaixa, INTERNAL_BANK_ACCOUNTS } from './fluxoCaixaLogic';
import {
  buildEntityLabel,
  formatDateSafe,
  getDocumentParts,
  parseDateFlexible,
  pickEntityName,
} from './centro-custo/centrodecustohelpers';
import type { CentroDeCustoPeriod, CentroDeCustoRow } from './types';

const currency = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
});

function normalizeBuildingCode(value: unknown): string {
  if (value === null || value === undefined) return '';
  const text = String(value).trim();
  if (!text) return '';

  const digits = text.replace(/\D/g, '');
  if (!digits) return '';

  return digits.replace(/^0+/, '');
}

function normalizeCompanyId(value: unknown): string {
  if (value === null || value === undefined) return '';
  return String(value).trim();
}

function resolveBuildingName(buildingIdOrCode: unknown, buildings: any[]): string {
  const needle = normalizeBuildingCode(buildingIdOrCode);
  if (!needle) return '';

  const found = buildings.find((b) => {
    const code = normalizeBuildingCode(b?.code ?? b?.id ?? b?.buildingId);
    return code === needle;
  });

  if (!found) return '';
  return String(found?.name || found?.description || found?.title || '').trim();
}

function resolveCompanyName(companyId: unknown, companies: any[]): string {
  const id = normalizeCompanyId(companyId);
  if (!id) return '';

  const found = companies.find((c) => normalizeCompanyId(c?.id ?? c?.companyId) === id);
  return String(found?.name || found?.companyName || found?.razaoSocial || '').trim();
}

function toCsvRow(values: Array<string | number>): string {
  return values
    .map((v) => {
      const text = String(v ?? '');
      if (text.includes('"')) return `"${text.replace(/"/g, '""')}"`;
      if (text.includes(';') || text.includes('\n')) return `"${text}"`;
      return text;
    })
    .join(';');
}

function downloadCsv(filename: string, header: string[], rows: Array<Array<string | number>>): void {
  const lines = [toCsvRow(header), ...rows.map(toCsvRow)].join('\n');
  const blob = new Blob([`\ufeff${lines}`], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);

  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();

  URL.revokeObjectURL(url);
}

function toMonthKey(dateStr: string): string {
  const t = parseDateFlexible(dateStr);
  if (!t) return '';
  const d = new Date(t);
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  return `${d.getFullYear()}-${mm}`;
}

function monthLabel(monthKey: string): string {
  if (!monthKey) return '';
  const [y, m] = monthKey.split('-');
  return `${m}/${y}`;
}

type CentroDeCustoRowWithCompany = CentroDeCustoRow & { companyId: string; companyName: string };

type CentroDeCustoPeriodWithCompany = CentroDeCustoPeriod & { companyId: string; companyName: string };

export function CentroDeCustoTab() {
  const DETAIL_PAGE_SIZE = 20;

  const [detailPage, setDetailPage] = useState(1);
  const [detailShowAll, setDetailShowAll] = useState(false);

  const {
    loading,
    companies,
    buildings,
    selectedCompany,
    fcSelectedBuilding,
    startDate,
    endDate,
    financialTitles,
    receivableTitles,
  } = useSienge();

  const filteredRows: CentroDeCustoRowWithCompany[] = useMemo(() => {
    const companyMap = new Map<string, string>();
    for (const c of companies || []) {
      const id = normalizeCompanyId((c as any)?.id);
      if (!id) continue;
      const name = String((c as any)?.name || (c as any)?.companyName || '').trim();
      companyMap.set(id, name || id);
    }

    const resolveCompanyFromBuilding = (buildingIdOrCode: unknown): string => {
      const needle = normalizeBuildingCode(buildingIdOrCode);
      if (!needle) return '';
      const found = (buildings || []).find((b: any) => {
        const code = normalizeBuildingCode(b?.code ?? b?.id ?? b?.buildingId);
        return code === needle;
      });
      return normalizeCompanyId(found?.companyId ?? found?.company_id);
    };

    const rows: CentroDeCustoRowWithCompany[] = [];

    // Contas a pagar (títulos) — sempre saída.
    for (const f of Array.isArray(financialTitles) ? financialTitles : []) {
      if (!f) continue;
      const obraId = String((f as any)?.buildingId ?? (f as any)?.buildingCode ?? '');
      const obra =
        String((f as any)?.buildingName || '').trim() ||
        resolveBuildingName(obraId, buildings || []) ||
        (obraId ? `Obra ${obraId}` : 'Sem Obra');

      const companyId =
        normalizeCompanyId((f as any)?.companyId) ||
        resolveCompanyFromBuilding(obraId) ||
        '';
      const companyName = companyId ? (companyMap.get(companyId) || companyId) : '';

      const saida = Number((f as any)?.amount || 0) || 0;
      const entrada = 0;

      rows.push({
        id: (f as any)?.id,
        data: String((f as any)?.dueDate || ''),
        vencto: String((f as any)?.dueDate || ''),
        baixa: String((f as any)?.paymentDate || ''),
        companyId,
        companyName,
        historico: String((f as any)?.description || 'Título a Pagar'),
        clienteFornecedor: pickEntityName(
          null,
          null,
          null,
          (f as any)?.creditorName,
          null,
          null,
        ),
        tituloParcela: String((f as any)?.id || ''),
        documento: String((f as any)?.documentNumber || ''),
        obra,
        obraId,
        entrada,
        saida: Math.abs(saida),
        saldo: entrada - Math.abs(saida),
      });
    }

    // Extrato (receivableTitles) — Income/Expense.
    for (const r of Array.isArray(receivableTitles) ? receivableTitles : []) {
      if (!r) continue;
      const obraId = String((r as any)?.buildingId ?? (r as any)?.buildingCode ?? '');
      const obra =
        String((r as any)?.buildingName || '').trim() ||
        resolveBuildingName(obraId, buildings || []) ||
        (obraId ? `Obra ${obraId}` : 'Sem Obra');

      const companyId =
        normalizeCompanyId((r as any)?.companyId) ||
        resolveCompanyFromBuilding(obraId) ||
        '';
      const companyName = companyId ? (companyMap.get(companyId) || companyId) : '';

      const rawValue = Number((r as any)?.rawValue ?? 0);
      const amount = Number((r as any)?.amount ?? 0);
      const type = String((r as any)?.type || 'Income');
      const isIncome = type === 'Income';
      const rv = rawValue !== 0 ? rawValue : (isIncome ? Math.abs(amount) : -Math.abs(amount));
      const entrada = isIncome ? rv : 0;
      const saida = !isIncome ? Math.abs(rv) : 0;

      const documentId = String((r as any)?.documentId || '').trim();
      const documentNumber = String((r as any)?.documentNumber || '').trim();
      const installment = (r as any)?.installmentNumber;
      const tituloParcela = (() => {
        const base = [documentId, documentNumber].filter(Boolean).join('.');
        if (!base) return String((r as any)?.id || '');
        if (installment == null || installment === '') return base;
        return `${base}/${String(installment)}`;
      })();

      rows.push({
        id: (r as any)?.id,
        data: String((r as any)?.dueDate || ''),
        vencto: String((r as any)?.dueDate || ''),
        baixa: String((r as any)?.paymentDate || ''),
        companyId,
        companyName,
        historico: String((r as any)?.description || (isIncome ? 'Recebimento' : 'Pagamento')),
        clienteFornecedor: pickEntityName(
          (r as any)?.clientName,
          null,
          null,
          null,
          null,
          null,
        ),
        tituloParcela,
        documento: documentId || documentNumber || String((r as any)?.id || ''),
        obra,
        obraId,
        entrada,
        saida,
        saldo: entrada - saida,
      });
    }

    rows.sort((a, b) => {
      const ta = parseDateFlexible(a.data);
      const tb = parseDateFlexible(b.data);
      return tb - ta;
    });

    return rows;
  }, [buildings, companies, financialTitles, receivableTitles]);

  useEffect(() => {
    setDetailPage(1);
  }, [detailShowAll, filteredRows.length]);

  const detailTotalPages = useMemo(() => {
    if (detailShowAll) return 1;
    return Math.max(1, Math.ceil(filteredRows.length / DETAIL_PAGE_SIZE));
  }, [DETAIL_PAGE_SIZE, detailShowAll, filteredRows.length]);

  const detailSafePage = useMemo(() => {
    return Math.min(Math.max(1, detailPage), detailTotalPages);
  }, [detailPage, detailTotalPages]);

  const detailRows = useMemo(() => {
    if (detailShowAll) return filteredRows;
    const start = (detailSafePage - 1) * DETAIL_PAGE_SIZE;
    const end = start + DETAIL_PAGE_SIZE;
    return filteredRows.slice(start, end);
  }, [DETAIL_PAGE_SIZE, detailSafePage, detailShowAll, filteredRows]);

  const detailRangeLabel = useMemo(() => {
    if (filteredRows.length === 0) return '0';
    if (detailShowAll) return `1-${filteredRows.length}`;
    const start = (detailSafePage - 1) * DETAIL_PAGE_SIZE + 1;
    const end = Math.min(filteredRows.length, start + DETAIL_PAGE_SIZE - 1);
    return `${start}-${end}`;
  }, [DETAIL_PAGE_SIZE, detailSafePage, detailShowAll, filteredRows.length]);

  const totals = useMemo(() => {
    const entrada = filteredRows.reduce((acc, r) => acc + (r.entrada || 0), 0);
    const saida = filteredRows.reduce((acc, r) => acc + (r.saida || 0), 0);
    return {
      entrada,
      saida,
      saldo: entrada - saida,
    };
  }, [filteredRows]);

  const monthlyTotals: CentroDeCustoPeriod[] = useMemo(() => {
    const m = new Map<string, { entradas: number; saidas: number }>();

    for (const r of filteredRows) {
      const key = toMonthKey(r.data);
      if (!key) continue;
      const cur = m.get(key) || { entradas: 0, saidas: 0 };
      cur.entradas += r.entrada || 0;
      cur.saidas += r.saida || 0;
      m.set(key, cur);
    }

    const rows: CentroDeCustoPeriod[] = Array.from(m.entries())
      .sort(([a], [b]) => (a > b ? -1 : 1))
      .map(([mes, v]) => ({
        mes,
        entradas: v.entradas,
        saidas: v.saidas,
        saldo: v.entradas - v.saidas,
      }));

    return rows;
  }, [filteredRows]);

  const companyMonthlyTotals: CentroDeCustoPeriodWithCompany[] = useMemo(() => {
    const m = new Map<string, { companyId: string; companyName: string; mes: string; entradas: number; saidas: number }>();

    for (const r of filteredRows) {
      const mes = toMonthKey(r.data);
      if (!mes) continue;
      const key = `${r.companyId}::${mes}`;

      const existing = m.get(key) || {
        companyId: r.companyId,
        companyName: r.companyName,
        mes,
        entradas: 0,
        saidas: 0,
      };

      existing.entradas += r.entrada || 0;
      existing.saidas += r.saida || 0;

      m.set(key, existing);
    }

    const arr = Array.from(m.values()).sort((a, b) => {
      if (a.mes !== b.mes) return a.mes > b.mes ? -1 : 1;
      return a.companyName.localeCompare(b.companyName);
    });

    return arr.map((x) => ({
      companyId: x.companyId,
      companyName: x.companyName,
      mes: x.mes,
      entradas: x.entradas,
      saidas: x.saidas,
      saldo: x.entradas - x.saidas,
    }));
  }, [filteredRows]);

  const onDownloadDetalhado = () => {
    downloadCsv(
      'centro-de-custo-detalhado.csv',
      ['Empresa', 'Obra', 'Data', 'Histórico', 'Cliente/Fornecedor', 'Título/Parcela', 'Documento', 'Entrada', 'Saída', 'Saldo'],
      filteredRows.map((r) => {
        const doc = getDocumentParts(r.documento);
        return [
          r.companyName,
          r.obra,
          formatDateSafe(r.data),
          r.historico,
          r.clienteFornecedor || '',
          r.tituloParcela || '',
          doc.treated ? `${doc.original} (${doc.treated})` : doc.original,
          r.entrada,
          r.saida,
          r.saldo,
        ];
      })
    );
  };

  const onDownloadMensal = () => {
    downloadCsv(
      'centro-de-custo-mensal.csv',
      ['Mês', 'Entradas', 'Saídas', 'Saldo'],
      monthlyTotals.map((m) => [monthLabel(m.mes), m.entradas, m.saidas, m.saldo])
    );
  };

  const onDownloadMensalEmpresa = () => {
    downloadCsv(
      'centro-de-custo-mensal-por-empresa.csv',
      ['Empresa', 'Mês', 'Entradas', 'Saídas', 'Saldo'],
      companyMonthlyTotals.map((m) => [m.companyName, monthLabel(m.mes), m.entradas, m.saidas, m.saldo])
    );
  };

  const onDownloadFluxoCaixa = () => {
    const fluxo = calcularFluxoCaixa({
      allReceivableTitles: Array.isArray(receivableTitles) ? receivableTitles : [],
      allFinancialTitles: Array.isArray(financialTitles) ? financialTitles : [],
      buildings: Array.isArray(buildings) ? buildings : [],
      fcSelectedCompany: selectedCompany || 'all',
      fcSelectedBuilding: fcSelectedBuilding || 'all',
      fcHideInternal: false,
      startNumeric: startDate ? parseInt(String(startDate.getFullYear()) + String(startDate.getMonth() + 1).padStart(2, '0') + String(startDate.getDate()).padStart(2, '0')) : null,
      endNumeric: endDate ? parseInt(String(endDate.getFullYear()) + String(endDate.getMonth() + 1).padStart(2, '0') + String(endDate.getDate()).padStart(2, '0')) : null,
    }).rows;

    downloadCsv(
      'fluxo-caixa.csv',
      ['Conta', 'Data', 'Tit/Parc', 'Origem', 'Tipo', 'Pessoa', 'Entrada', 'Saída', 'Saldo'],
      fluxo.map((r) => [
        r.bankAccount,
        formatDateSafe(r.data),
        r.titParc,
        r.origem,
        r.statementType,
        r.pessoa,
        r.entrada,
        r.saida,
        r.saldo,
      ])
    );
  };

  const onDownloadBancoInterno = () => {
    const fluxo = calcularFluxoCaixa({
      allReceivableTitles: Array.isArray(receivableTitles) ? receivableTitles : [],
      allFinancialTitles: Array.isArray(financialTitles) ? financialTitles : [],
      buildings: Array.isArray(buildings) ? buildings : [],
      fcSelectedCompany: selectedCompany || 'all',
      fcSelectedBuilding: fcSelectedBuilding || 'all',
      fcHideInternal: false,
      startNumeric: startDate ? parseInt(String(startDate.getFullYear()) + String(startDate.getMonth() + 1).padStart(2, '0') + String(startDate.getDate()).padStart(2, '0')) : null,
      endNumeric: endDate ? parseInt(String(endDate.getFullYear()) + String(endDate.getMonth() + 1).padStart(2, '0') + String(endDate.getDate()).padStart(2, '0')) : null,
    }).rows;
    const interno = fluxo.filter((r) => INTERNAL_BANK_ACCOUNTS.has(r.bankAccount));

    downloadCsv(
      'fluxo-caixa-banco-interno.csv',
      ['Conta', 'Data', 'Tit/Parc', 'Origem', 'Tipo', 'Pessoa', 'Entrada', 'Saída', 'Saldo'],
      interno.map((r) => [
        r.bankAccount,
        formatDateSafe(r.data),
        r.titParc,
        r.origem,
        r.statementType,
        r.pessoa,
        r.entrada,
        r.saida,
        r.saldo,
      ])
    );
  };

  const onDownloadResumoBanco = () => {
    const fluxo = calcularFluxoCaixa({
      allReceivableTitles: Array.isArray(receivableTitles) ? receivableTitles : [],
      allFinancialTitles: Array.isArray(financialTitles) ? financialTitles : [],
      buildings: Array.isArray(buildings) ? buildings : [],
      fcSelectedCompany: selectedCompany || 'all',
      fcSelectedBuilding: fcSelectedBuilding || 'all',
      fcHideInternal: false,
      startNumeric: startDate ? parseInt(String(startDate.getFullYear()) + String(startDate.getMonth() + 1).padStart(2, '0') + String(startDate.getDate()).padStart(2, '0')) : null,
      endNumeric: endDate ? parseInt(String(endDate.getFullYear()) + String(endDate.getMonth() + 1).padStart(2, '0') + String(endDate.getDate()).padStart(2, '0')) : null,
    }).rows;
    const interno = fluxo.filter((r) => INTERNAL_BANK_ACCOUNTS.has(r.bankAccount));

    const m = new Map<string, { conta: string; saldo: number }>();

    for (const r of interno) {
      const key = r.bankAccount;
      const cur = m.get(key) || { conta: r.bankAccount, saldo: 0 };
      cur.saldo = r.saldo;
      m.set(key, cur);
    }

    const rows = Array.from(m.values()).sort((a, b) => a.conta.localeCompare(b.conta));

    downloadCsv(
      'resumo-banco-interno.csv',
      ['Conta', 'Saldo'],
      rows.map((r) => [r.conta, r.saldo])
    );
  };

  function handlePrint() {
    window.print();
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="p-4 space-y-4"
    >
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div>
          <h2 className="text-2xl font-semibold">Centro de Custo</h2>
          <p className="text-sm text-muted-foreground">Consolidado por empresa e obra</p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button variant="outline" onClick={handlePrint}>
            <FileText className="h-4 w-4 mr-2" />
            Imprimir
          </Button>

          <Button variant="outline" onClick={onDownloadDetalhado}>
            <Download className="h-4 w-4 mr-2" />
            CSV detalhado
          </Button>

          <Button variant="outline" onClick={onDownloadMensal}>
            <Download className="h-4 w-4 mr-2" />
            CSV mensal
          </Button>

          <Button variant="outline" onClick={onDownloadMensalEmpresa}>
            <Download className="h-4 w-4 mr-2" />
            CSV mensal (empresa)
          </Button>

          <Button variant="outline" onClick={onDownloadFluxoCaixa}>
            <Wallet className="h-4 w-4 mr-2" />
            Fluxo de Caixa
          </Button>

          <Button variant="outline" onClick={onDownloadBancoInterno}>
            <Wallet className="h-4 w-4 mr-2" />
            Fluxo (banco interno)
          </Button>

          <Button variant="outline" onClick={onDownloadResumoBanco}>
            <Wallet className="h-4 w-4 mr-2" />
            Resumo (interno)
          </Button>
        </div>
      </div>

      {loading && (
        <Card>
          <CardContent className="p-4 text-sm text-muted-foreground">
            Carregando dados do SIENGE...
          </CardContent>
        </Card>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <ArrowUp className="h-4 w-4 text-green-600" /> Entradas
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-semibold">{currency.format(totals.entrada)}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <ArrowDown className="h-4 w-4 text-red-600" /> Saídas
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-semibold">{currency.format(totals.saida)}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center gap-2">
              <Wallet className="h-4 w-4" /> Saldo
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div
              className={cn(
                'text-2xl font-semibold',
                totals.saldo >= 0 ? 'text-green-600' : 'text-red-600'
              )}
            >
              {currency.format(totals.saldo)}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2">
            <Building2 className="h-4 w-4" />
            Resumo mensal
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Mês</TableHead>
                <TableHead className="text-right">Entradas</TableHead>
                <TableHead className="text-right">Saídas</TableHead>
                <TableHead className="text-right">Saldo</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {monthlyTotals.map((m) => (
                <TableRow key={m.mes}>
                  <TableCell className="font-medium">{monthLabel(m.mes)}</TableCell>
                  <TableCell className="text-right">{currency.format(m.entradas)}</TableCell>
                  <TableCell className="text-right">{currency.format(m.saidas)}</TableCell>
                  <TableCell
                    className={cn('text-right font-medium', m.saldo >= 0 ? 'text-green-600' : 'text-red-600')}
                  >
                    {currency.format(m.saldo)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2">
            <Building2 className="h-4 w-4" />
            Resumo mensal por empresa
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Empresa</TableHead>
                <TableHead>Mês</TableHead>
                <TableHead className="text-right">Entradas</TableHead>
                <TableHead className="text-right">Saídas</TableHead>
                <TableHead className="text-right">Saldo</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {companyMonthlyTotals.map((m) => (
                <TableRow key={`${m.companyId}-${m.mes}`}>
                  <TableCell className="font-medium">{m.companyName}</TableCell>
                  <TableCell>{monthLabel(m.mes)}</TableCell>
                  <TableCell className="text-right">{currency.format(m.entradas)}</TableCell>
                  <TableCell className="text-right">{currency.format(m.saidas)}</TableCell>
                  <TableCell
                    className={cn('text-right font-medium', m.saldo >= 0 ? 'text-green-600' : 'text-red-600')}
                  >
                    {currency.format(m.saldo)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2">
            <Badge variant="secondary">{filteredRows.length}</Badge>
            Lançamentos (detalhado)
          </CardTitle>
        </CardHeader>
        <CardContent className="overflow-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Empresa</TableHead>
                <TableHead>Obra</TableHead>
                <TableHead>Data</TableHead>
                <TableHead>Histórico</TableHead>
                <TableHead>Entidade</TableHead>
                <TableHead>Título/Parcela</TableHead>
                <TableHead>Documento</TableHead>
                <TableHead className="text-right">Entrada</TableHead>
                <TableHead className="text-right">Saída</TableHead>
                <TableHead className="text-right">Saldo</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {detailRows.map((row) => {
                const doc = getDocumentParts(row.documento);

                return (
                  <TableRow key={String(row.id ?? `${row.companyId}-${row.data}-${row.historico}`)}>
                    <TableCell className="whitespace-nowrap">{row.companyName}</TableCell>
                    <TableCell className="whitespace-nowrap">{row.obra}</TableCell>
                    <TableCell className="whitespace-nowrap">{formatDateSafe(row.data)}</TableCell>
                    <TableCell className="min-w-[280px]">{row.historico}</TableCell>
                    <TableCell className="min-w-[240px]">
                      {buildEntityLabel(row.clienteFornecedor, row.documento, row.entrada > 0 ? 'Cliente' : 'Credor')}
                    </TableCell>
                    <TableCell className="whitespace-nowrap">{row.tituloParcela || '—'}</TableCell>
                    <TableCell className="whitespace-nowrap">
                      <div className="flex flex-col">
                        <span>{doc.original || '—'}</span>
                        {doc.treated && (
                          <span className="text-xs text-muted-foreground">{doc.treated}</span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="text-right whitespace-nowrap">
                      {row.entrada ? currency.format(row.entrada) : '—'}
                    </TableCell>
                    <TableCell className="text-right whitespace-nowrap">
                      {row.saida ? currency.format(row.saida) : '—'}
                    </TableCell>
                    <TableCell
                      className={cn(
                        'text-right whitespace-nowrap font-medium',
                        row.saldo >= 0 ? 'text-green-600' : 'text-red-600'
                      )}
                    >
                      {currency.format(row.saldo)}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>

          <div className="mt-3 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="text-xs text-muted-foreground">
              Mostrando {detailRangeLabel} de {filteredRows.length}
              {!detailShowAll && detailTotalPages > 1 ? ` (página ${detailSafePage}/${detailTotalPages})` : ''}
            </div>

            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant={detailShowAll ? 'default' : 'outline'}
                onClick={() => setDetailShowAll((v) => !v)}
                className="h-9"
              >
                {detailShowAll ? 'Paginar' : 'Todas'}
              </Button>

              {!detailShowAll && (
                <>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setDetailPage((p) => Math.max(1, p - 1))}
                    disabled={detailSafePage <= 1}
                    className="h-9"
                  >
                    Anterior
                  </Button>

                  <Button
                    type="button"
                    onClick={() => setDetailPage((p) => Math.min(p + 1, detailTotalPages))}
                    disabled={detailSafePage >= detailTotalPages}
                    className="h-9"
                  >
                    Next
                  </Button>
                </>
              )}
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  );
}
