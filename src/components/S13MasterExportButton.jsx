import React, { useState } from 'react';

export default function S13MasterExportButton() {
  const [isExporting, setIsExporting] = useState(false);

  const handleMasterExport = async () => {
    setIsExporting(true);
    try {
      const currentYear = new Date().getFullYear();
      const response = await fetch(`/api/export-s13-master?year=${currentYear}`);
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `S-13_Master_Record_${currentYear}.pdf`;
      document.body.appendChild(a); a.click(); a.remove(); window.URL.revokeObjectURL(url);
    } catch (error) {}
    setIsExporting(false);
  };

  return (
    <button onClick={handleMasterExport} disabled={isExporting} className="bg-teal-600 text-white font-medium py-2 px-4 rounded-lg transition-all">
      {isExporting ? 'Generating Master...' : 'Export S-13 Master Record'}
    </button>
  );
}