declare module './Modal' {
  import React from 'react';

  interface ModalProps {
    title: string;
    onClose: () => void;
    children: React.ReactNode;
  }

  const Modal: React.FC<ModalProps>;
  export default Modal;
}