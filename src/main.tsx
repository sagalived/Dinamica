import {Component, StrictMode, type ReactNode} from 'react';
import {createRoot} from 'react-dom/client';
import App from './App.tsx';
import './index.css';

type ErrorBoundaryProps = {
  children: ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
  errorMessage: string;
};

class AppErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  declare props: ErrorBoundaryProps;

  state: ErrorBoundaryState = {
    hasError: false,
    errorMessage: '',
  };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return {
      hasError: true,
      errorMessage: error.message || 'Unknown rendering error',
    };
  }

  componentDidCatch(error: Error, errorInfo: { componentStack: string }) {
    console.error('Frontend render error:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{
          minHeight: '100vh',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          background: '#0F0F10',
          color: '#F5F5F5',
          padding: '24px',
          fontFamily: 'sans-serif',
        }}>
          <div style={{
            maxWidth: '720px',
            width: '100%',
            background: '#161618',
            border: '1px solid rgba(255,255,255,0.08)',
            borderRadius: '16px',
            padding: '24px',
          }}>
            <h1 style={{ margin: 0, fontSize: '20px' }}>Erro ao renderizar a interface</h1>
            <p style={{ marginTop: '12px', color: '#D4D4D8' }}>
              Recarregue a pagina. Se continuar, veja o detalhe abaixo e me envie.
            </p>
            <pre style={{
              marginTop: '16px',
              padding: '16px',
              borderRadius: '12px',
              background: '#09090B',
              color: '#FB923C',
              overflow: 'auto',
              whiteSpace: 'pre-wrap',
            }}>
              {this.state.errorMessage}
            </pre>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AppErrorBoundary>
      <App />
    </AppErrorBoundary>
  </StrictMode>,
);
