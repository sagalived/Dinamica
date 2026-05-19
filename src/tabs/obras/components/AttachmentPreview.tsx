import React, { useState } from 'react';
import { FileText, X, ZoomIn } from 'lucide-react';

import type { Attachment } from '../types';

export function AttachmentPreview({
  att,
  onDelete,
}: {
  att: Attachment;
  onDelete: () => void;
}) {
  const [lightbox, setLightbox] = useState(false);
  const BASE = import.meta.env.VITE_API_BASE ?? '';

  return (
    <>
      <div className="group relative rounded-lg overflow-hidden border border-white/10 bg-black/30">
        {att.type === 'image' ? (
          <div
            className="w-full h-20 cursor-zoom-in relative"
            onClick={() => setLightbox(true)}
          >
            <img
              src={`${BASE}${att.url}`}
              alt={att.originalName}
              className="w-full h-full object-cover hover:scale-105 transition-transform"
            />
            <div className="absolute inset-0 bg-black/0 group-hover:bg-black/20 transition-colors flex items-center justify-center">
              <ZoomIn size={16} className="text-white opacity-0 group-hover:opacity-100 transition-opacity" />
            </div>
          </div>
        ) : att.type === 'video' ? (
          <video src={`${BASE}${att.url}`} className="w-full h-20 object-cover" controls />
        ) : (
          <a
            href={`${BASE}${att.url}`}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 p-2 hover:bg-white/5 transition-colors"
          >
            <FileText size={18} className="text-green-400 shrink-0" />
            <span className="text-xs text-gray-300 truncate">{att.originalName}</span>
          </a>
        )}
        <button
          onClick={onDelete}
          className="absolute top-1 right-1 w-5 h-5 rounded-full bg-black/70 text-red-400 hover:text-red-300 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity"
        >
          <X size={10} />
        </button>
      </div>

      {lightbox && (
        <div
          className="fixed inset-0 z-[9999] bg-black/95 flex items-center justify-center p-4"
          onClick={() => setLightbox(false)}
        >
          <img
            src={`${BASE}${att.url}`}
            alt={att.originalName}
            className="max-w-full max-h-full object-contain rounded-xl shadow-2xl"
            onClick={(event) => event.stopPropagation()}
          />
          <button
            className="absolute top-4 right-4 text-white/60 hover:text-white"
            onClick={() => setLightbox(false)}
          >
            <X size={28} />
          </button>
          <p className="absolute bottom-6 text-gray-400 text-sm">{att.originalName}</p>
        </div>
      )}
    </>
  );
}
