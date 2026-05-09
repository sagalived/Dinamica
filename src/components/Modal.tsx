import React from 'react';
import { cn } from '../lib/utils';
import { useTheme } from '../contexts/ThemeContext';

interface ModalProps {
  title: string;
  onClose: () => void;
  children: React.ReactNode;
}

const Modal: React.FC<ModalProps> = ({ title, onClose, children }) => {
  const { isDark } = useTheme();

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/75 p-4">
      <div
        className={cn(
          "w-full max-w-2xl rounded-2xl border shadow-2xl overflow-hidden",
          isDark ? "border-slate-800 bg-[#11141A] text-slate-100" : "border-slate-200 bg-white text-slate-800"
        )}
      >
        <div className={cn("flex items-center justify-between p-4 border-b", isDark ? "border-slate-800" : "border-slate-200")}>
          <h2 className={cn("text-lg font-black", isDark ? "text-slate-100" : "text-slate-900")}>{title}</h2>
          <button
            onClick={onClose}
            className={cn("text-xl leading-none", isDark ? "text-slate-400 hover:text-slate-200" : "text-slate-500 hover:text-slate-800")}
          >
            ×
          </button>
        </div>
        <div className="p-4 space-y-4">{children}</div>
      </div>
    </div>
  );
};

export default Modal;