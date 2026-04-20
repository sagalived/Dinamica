import React, { useState } from 'react';
import { ShieldAlert, Users, Lock, Key, DatabaseBackup } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from './ui/card';
import { Button } from './ui/button';

export function AccessControlTab() {
  const [backupStatus, setBackupStatus] = useState('');

  const handleBackup = async () => {
    setBackupStatus('Iniciando envio...');
    try {
      const res = await fetch('/api/admin/backup/drive', { method: 'POST' });
      const data = await res.json();
      if (data.success) setBackupStatus(`Concluído! ID: ${data.file_id}`);
      else setBackupStatus('Erro (precisa credencial): ' + (data.error || 'Falha'));
    } catch {
      setBackupStatus('Erro de conexão do servidor.');
    }
  };
  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-6">
        <Users className="text-orange-500" size={32} />
        <div>
          <h2 className="text-2xl font-black text-white uppercase tracking-tight">Gestão de Acessos</h2>
          <p className="text-gray-500 text-sm">Cadastro de perfis e controle de permissões do sistema.</p>
        </div>
      </div>

      <Card className="bg-orange-500/10 border-orange-500/20 shadow-none">
        <CardContent className="p-8 flex flex-col items-center justify-center text-center">
          <ShieldAlert className="text-orange-500 mb-4 animate-pulse" size={48} />
          <h3 className="text-xl font-black text-white uppercase tracking-tight mb-2">Módulo em Manutenção</h3>
          <p className="text-gray-400 max-w-md">
            O painel de múltiplos níveis de acesso (Desenvolvedor, Administrador, Usuário) está sendo preparado para o lançamento.
          </p>
          <div className="flex flex-wrap gap-4 mt-6">
            <div className="bg-black/40 border border-white/5 py-2 px-4 rounded-xl flex items-center gap-2">
              <Key size={14} className="text-red-500" />
              <span className="text-xs font-bold text-gray-300">Desenvolvedor</span>
            </div>
            <div className="bg-black/40 border border-white/5 py-2 px-4 rounded-xl flex items-center gap-2">
              <Lock size={14} className="text-orange-500" />
              <span className="text-xs font-bold text-gray-300">Administrador</span>
            </div>
            <div className="bg-black/40 border border-white/5 py-2 px-4 rounded-xl flex items-center gap-2">
              <Users size={14} className="text-blue-500" />
              <span className="text-xs font-bold text-gray-300">Usuário Restrito</span>
            </div>
          </div>
          <div className="mt-8 flex flex-col items-center justify-center p-4 border border-blue-500/20 bg-blue-500/10 rounded-xl w-full">
            <h4 className="text-blue-400 font-bold text-sm mb-2 uppercase">Google Drive Backup</h4>
            <Button onClick={handleBackup} className="bg-blue-600 hover:bg-blue-700 text-white gap-2 font-bold uppercase">
              <DatabaseBackup size={16} /> Forçar Backup Agora
            </Button>
            {backupStatus && <p className="text-xs text-blue-300 mt-3 font-mono">{backupStatus}</p>}
          </div>

        </CardContent>
      </Card>

      <Card className="bg-[#161618] border-white/5 shadow-2xl opacity-50 pointer-events-none">
        <CardHeader>
          <CardTitle className="text-lg font-black uppercase tracking-tight text-white flex items-center gap-2">
            Prévia: Novo Usuário
          </CardTitle>
          <CardDescription className="text-gray-500">Formulário de cadastro bloqueado temporariamente.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="bg-black/40 h-10 w-full rounded-xl border border-white/5" />
            <div className="bg-black/40 h-10 w-full rounded-xl border border-white/5" />
          </div>
          <div className="bg-black/40 h-10 w-full rounded-xl border border-white/5" />
          <div className="bg-black/40 h-24 w-full rounded-xl border border-white/5" />
          <Button disabled className="w-full bg-gray-800 text-gray-500 font-bold uppercase tracking-widest">Aguarde Disponibilidade</Button>
        </CardContent>
      </Card>
    </div>
  );
}
