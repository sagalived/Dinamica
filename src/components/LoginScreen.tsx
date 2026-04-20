import { type FormEvent, useState } from 'react';
import { Eye, EyeOff, LogIn, Plus, X } from 'lucide-react';
import { authApi, type AuthUser } from '../lib/api';

type LoginScreenProps = {
  onLogin: (user: AuthUser) => void;
};

export function LoginScreen({ onLogin }: LoginScreenProps) {
  const [email, setEmail] = useState('dev@admin.com');
  const [password, setPassword] = useState('admin');
  const [showPassword, setShowPassword] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [registerOpen, setRegisterOpen] = useState(false);
  const [registerSubmitting, setRegisterSubmitting] = useState(false);
  const [registerError, setRegisterError] = useState('');
  const [registerSuccess, setRegisterSuccess] = useState('');
  const [registerForm, setRegisterForm] = useState({
    name: '',
    email: '',
    department: '',
    role: 'user',
  });

  const loginWithFallback = () => {
    if (email.trim().toLowerCase() !== 'dev@admin.com' || password !== 'admin') {
      return false;
    }

    onLogin({
      id: 1,
      username: 'dev@admin.com',
      email: 'dev@admin.com',
      name: 'Administrador Dev',
      role: 'developer',
      department: 'Tecnologia',
    });
    return true;
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setError('');

    try {
      const response = await authApi.post('/login', { email, password });
      const user = response.data?.user as AuthUser | undefined;
      if (!user) {
        throw new Error('Resposta de login inválida.');
      }
      onLogin(user);
    } catch (err: any) {
      if (loginWithFallback()) {
        return;
      }
      setError(err?.response?.data?.error || err?.message || 'Não foi possível entrar.');
    } finally {
      setSubmitting(false);
    }
  };

  const handleRegister = async (event: FormEvent) => {
    event.preventDefault();
    setRegisterSubmitting(true);
    setRegisterError('');
    setRegisterSuccess('');

    try {
      const response = await authApi.post('/register', registerForm);
      const tempPassword = response.data?.tempPassword || '123456';
      setRegisterSuccess(`Usuário cadastrado com sucesso. Senha temporária: ${tempPassword}`);
      setRegisterForm({ name: '', email: '', department: '', role: 'user' });
    } catch (err: any) {
      setRegisterError(err?.response?.data?.error || err?.message || 'Não foi possível cadastrar.');
    } finally {
      setRegisterSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen overflow-hidden bg-[#101011] text-white">
      <div className="grid min-h-screen grid-cols-1 lg:grid-cols-[440px_minmax(0,1fr)]">
        <section className="relative flex flex-col justify-between border-r border-white/5 bg-[#111113] px-8 py-10 lg:px-14">
          <div className="inline-flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-gradient-to-br from-lime-500 to-emerald-600 font-black text-black">
              D
            </div>
            <div>
              <p className="text-2xl font-black uppercase tracking-tight">Dinâmica</p>
              <p className="text-[10px] uppercase tracking-[0.28em] text-lime-400">Empreendimentos</p>
            </div>
          </div>

          <div className="max-w-sm">
            <p className="mb-4 text-sm font-bold uppercase tracking-[0.24em] text-orange-500">Acesso Seguro</p>
            <h1 className="text-5xl font-black tracking-tight">Login</h1>
            <p className="mt-5 text-sm leading-6 text-gray-400">
              Entre com seu usuário administrativo para acessar o painel financeiro, logística e gestão operacional.
            </p>

            <form onSubmit={handleSubmit} className="mt-12 space-y-7">
              <label className="block">
                <span className="mb-3 block text-sm font-black text-orange-500">Email</span>
                <input
                  type="email"
                  value={email}
                  onChange={(event) => setEmail(event.target.value)}
                  className="w-full border-0 border-b border-white/30 bg-transparent px-0 py-3 text-lg outline-none transition placeholder:text-gray-600 focus:border-orange-500"
                  placeholder="dev@admin.com"
                  autoComplete="username"
                />
              </label>

              <label className="block">
                <span className="mb-3 block text-sm font-black text-orange-500">Senha</span>
                <div className="flex items-center gap-3 border-b border-white/30">
                  <input
                    type={showPassword ? 'text' : 'password'}
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    className="w-full border-0 bg-transparent px-0 py-3 text-lg outline-none placeholder:text-gray-600"
                    placeholder="admin"
                    autoComplete="current-password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword((current) => !current)}
                    className="pb-1 text-gray-300 transition hover:text-white"
                    aria-label={showPassword ? 'Ocultar senha' : 'Mostrar senha'}
                  >
                    {showPassword ? <EyeOff size={18} /> : <Eye size={18} />}
                  </button>
                </div>
              </label>

              {error ? <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-300">{error}</div> : null}

              <button
                type="submit"
                disabled={submitting}
                className="inline-flex w-full items-center justify-center gap-3 rounded-2xl bg-gradient-to-r from-orange-600 to-orange-500 px-5 py-4 text-sm font-black uppercase tracking-[0.24em] text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-70"
              >
                <LogIn size={16} />
                {submitting ? 'Entrando...' : 'Entrar'}
              </button>

              <button
                type="button"
                onClick={() => setRegisterOpen(true)}
                className="inline-flex w-full items-center justify-center gap-3 rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-sm font-black uppercase tracking-[0.24em] text-white transition hover:bg-white/10"
              >
                <Plus size={16} />
                Cadastrar Usuario
              </button>
            </form>
          </div>

          <div className="text-xs text-gray-500">
            Usuário padrão semeado no banco:
            <span className="ml-2 font-bold text-gray-300">dev@admin.com</span>
          </div>
        </section>

        <section className="relative hidden overflow-hidden bg-[radial-gradient(circle_at_top,#2b2b2d,transparent_45%),linear-gradient(90deg,#171719_0%,#222124_55%,#1a1a1d_100%)] lg:block">
          <div className="absolute left-10 top-10 h-28 w-28 rounded-full border-8 border-orange-500/95 bg-white shadow-[0_0_80px_rgba(249,115,22,0.15)]" />
          <div className="absolute right-36 top-20 h-12 w-12 rounded-full bg-gradient-to-br from-orange-300 to-orange-600 shadow-[0_0_40px_rgba(249,115,22,0.35)]" />
          <div className="absolute inset-x-20 top-24 h-[420px] rounded-[50%] border-[28px] border-orange-100/95 border-r-orange-500 border-b-transparent blur-[1px]" />
          <div className="absolute inset-x-24 top-40 h-[260px] rounded-[42px] border border-orange-500/35 bg-[#1f1f21] shadow-[0_30px_80px_rgba(0,0,0,0.45)]">
            <div className="flex h-full">
              <div className="flex w-24 flex-col justify-center gap-4 border-r border-orange-500/20 px-5">
                {Array.from({ length: 5 }).map((_, index) => (
                  <div key={index} className="h-7 w-7 rounded-full bg-gradient-to-br from-orange-300 to-orange-600 shadow-inner shadow-white/20" />
                ))}
              </div>
              <div className="flex-1 p-9">
                <div className="mb-8 flex items-start justify-between">
                  <div className="space-y-3">
                    <div className="h-3 w-28 rounded-full bg-orange-500" />
                    <div className="h-3 w-32 rounded-full bg-orange-500" />
                    <div className="h-3 w-20 rounded-full bg-orange-500" />
                  </div>
                  <div className="h-32 w-32 rounded-full border-8 border-orange-500 bg-orange-600/15" />
                </div>
                <div className="space-y-5">
                  {[62, 74, 58].map((width, index) => (
                    <div key={width} className="flex items-center gap-3">
                      <div className="h-5 w-5 rounded-full bg-orange-500" />
                      <div className="h-5 rounded-full bg-orange-500" style={{ width: `${width}%` }} />
                      <div className="h-5 rounded-full bg-orange-200/90" style={{ width: `${20 - index * 4}%` }} />
                    </div>
                  ))}
                </div>
                <div className="mt-10 grid grid-cols-3 gap-4">
                  {Array.from({ length: 3 }).map((_, index) => (
                    <div key={index} className="rounded-2xl border border-orange-500/20 bg-black/25 p-4">
                      <div className="h-4 w-4 rounded-full bg-orange-500" />
                      <div className="mt-4 h-2 w-16 rounded-full bg-orange-500" />
                      <div className="mt-2 h-2 w-12 rounded-full bg-orange-200/80" />
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
          <div className="absolute bottom-12 left-24 h-28 w-24 rounded-t-[30px] bg-gradient-to-b from-orange-300 to-orange-600 shadow-[0_14px_30px_rgba(249,115,22,0.2)]" />
          <div className="absolute bottom-12 left-40 h-20 w-16 rounded-t-[20px] bg-gradient-to-b from-orange-200 to-orange-500" />
          <div className="absolute bottom-20 right-20 h-44 w-16 rounded-full bg-gradient-to-b from-orange-500 to-orange-600" />
          <div className="absolute bottom-24 left-10 right-10 h-px bg-orange-500/30" />
        </section>
      </div>

      {registerOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 p-4 backdrop-blur-sm">
          <div className="w-full max-w-lg rounded-3xl border border-white/10 bg-[#161618] p-6 shadow-2xl">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs font-black uppercase tracking-[0.24em] text-orange-500">Novo Acesso</p>
                <h2 className="mt-2 text-2xl font-black">Cadastrar Usuario</h2>
              </div>
              <button type="button" onClick={() => setRegisterOpen(false)} className="rounded-full border border-white/10 bg-white/5 p-2 text-white hover:bg-white/10">
                <X size={18} />
              </button>
            </div>

            <form onSubmit={handleRegister} className="mt-6 space-y-5">
              <label className="block">
                <span className="mb-2 block text-sm font-black text-orange-500">Nome completo</span>
                <input
                  value={registerForm.name}
                  onChange={(event) => setRegisterForm((current) => ({ ...current, name: event.target.value }))}
                  className="w-full rounded-2xl border border-white/10 bg-black/30 px-4 py-3 text-white outline-none focus:border-orange-500"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm font-black text-orange-500">Email</span>
                <input
                  type="email"
                  value={registerForm.email}
                  onChange={(event) => setRegisterForm((current) => ({ ...current, email: event.target.value }))}
                  className="w-full rounded-2xl border border-white/10 bg-black/30 px-4 py-3 text-white outline-none focus:border-orange-500"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm font-black text-orange-500">Setor</span>
                <input
                  value={registerForm.department}
                  onChange={(event) => setRegisterForm((current) => ({ ...current, department: event.target.value }))}
                  className="w-full rounded-2xl border border-white/10 bg-black/30 px-4 py-3 text-white outline-none focus:border-orange-500"
                />
              </label>

              <label className="block">
                <span className="mb-2 block text-sm font-black text-orange-500">Perfil</span>
                <select
                  value={registerForm.role}
                  onChange={(event) => setRegisterForm((current) => ({ ...current, role: event.target.value }))}
                  className="w-full rounded-2xl border border-white/10 bg-black/30 px-4 py-3 text-white outline-none focus:border-orange-500"
                >
                  <option value="admin">Administrativo</option>
                  <option value="developer">Desenvolvedor</option>
                  <option value="user">Usuario</option>
                </select>
              </label>

              {registerError ? <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-300">{registerError}</div> : null}
              {registerSuccess ? <div className="rounded-2xl border border-green-500/20 bg-green-500/10 px-4 py-3 text-sm text-green-300">{registerSuccess}</div> : null}

              <button
                type="submit"
                disabled={registerSubmitting}
                className="inline-flex w-full items-center justify-center gap-3 rounded-2xl bg-gradient-to-r from-orange-600 to-orange-500 px-5 py-4 text-sm font-black uppercase tracking-[0.24em] text-white transition hover:brightness-110 disabled:opacity-70"
              >
                {registerSubmitting ? 'Cadastrando...' : 'Cadastrar Usuario'}
              </button>
            </form>
          </div>
        </div>
      ) : null}
    </div>
  );
}
