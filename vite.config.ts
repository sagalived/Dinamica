import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import os from 'os';
import path from 'path';
import {defineConfig, loadEnv} from 'vite';

function getLanIpv4(): string {
  const nets = os.networkInterfaces();
  for (const iface of Object.values(nets)) {
    for (const addr of iface ?? []) {
      if (addr.family === 'IPv4' && !addr.internal) return addr.address;
    }
  }
  return '127.0.0.1';
}

export default defineConfig(({mode}) => {
  const env = loadEnv(mode, '.', '');
  const apiHost = getLanIpv4();
  const apiPort = Number(
    env.VITE_API_PORT ||
      process.env.VITE_API_PORT ||
      env.API_PORT ||
      process.env.API_PORT ||
      env.PORT ||
      process.env.PORT ||
      '8000',
  );
  return {
    plugins: [react(), tailwindcss()],
    define: {
      'process.env.GEMINI_API_KEY': JSON.stringify(env.GEMINI_API_KEY),
    },
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    server: {
      // Permite acessar o dev server via IP da máquina (não só localhost)
      host: true,
      proxy: {
        '/api': {
          // Evita cair em instâncias antigas presas no 127.0.0.1:8000.
          // Como o Uvicorn do dev roda em 0.0.0.0, o IP LAN sempre aponta para a instância correta.
          target: `http://${apiHost}:${apiPort}`,
          changeOrigin: true,
        },
      },
      // HMR is disabled in AI Studio via DISABLE_HMR env var.
      // Do not modify - file watching is disabled to prevent flickering during agent edits.
      hmr: false,
      watch: {
        ignored: ['**/data/**', '**/dist/**'],
      },
    },
  };
});
