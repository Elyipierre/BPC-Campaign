import React, { useState, useEffect } from 'react';
import { supabase } from './supabaseClient'; 

export default function AssignmentHistory({ territory }) {
  const [history, setHistory] = useState([]);
  const [conductors, setConductors] = useState([]);
  const [selectedConductor, setSelectedConductor] = useState('');
  const [loading, setLoading] = useState(false);
  const [exporting, setExporting] = useState(false);

  useEffect(() => { if (territory?.id) { fetchHistory(); fetchConductors(); } }, [territory]);

  const fetchHistory = async () => {
    const { data } = await supabase.from('assignment_history').select(`id, assigned_at, completed_at, is_active, expires_at, profiles(id, display_name, role)`).eq('territory_id', territory.id).order('assigned_at', { ascending: false });
    if (data) setHistory(data);
  };

  const fetchConductors = async () => {
    const { data } = await supabase.from('profiles').select('id, display_name').in('role', ['admin', 'conductor']).order('display_name', { ascending: true });
    if (data) setConductors(data);
  };

  const handleAssign = async () => {
    if (!selectedConductor) return;
    setLoading(true);
    await supabase.from('territories').update({ status: 'Assigned', assigned_to: selectedConductor, assigned_at: new Date() }).eq('id', territory.id);
    await supabase.from('assignment_history').insert([{ territory_id: territory.id, assigned_to: selectedConductor, is_active: true }]);
    setLoading(false);
    setSelectedConductor('');
    fetchHistory();
  };

  const handleReturn = async (historyId) => {
    setLoading(true);
    await supabase.from('assignment_history').update({ completed_at: new Date(), is_active: false }).eq('id', historyId);
    await supabase.from('territories').update({ status: 'Available', assigned_to: null, assigned_at: null }).eq('id', territory.id);
    setLoading(false);
    fetchHistory();
  };

  const handleExportS13 = async () => {
    setExporting(true);
    try {
      const response = await fetch(`/api/generate-s13?territoryId=${territory.id}`, { method: 'GET' });
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `S-13_Territory.pdf`; a.click(); a.remove();
    } catch (error) {}
    setExporting(false);
  };

  const activeAssignment = history.find(h => h.is_active);

  return (
    <div className="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 flex flex-col h-full">
      <div className="mb-6 border-b border-white/10 pb-4 flex justify-between">
        <h2 className="text-2xl font-semibold text-[#FDFBF7]">Assignment History</h2>
        <button onClick={handleExportS13} disabled={exporting || history.length === 0} className="bg-teal-600/20 text-teal-300 border border-teal-500/30 py-2 px-4 rounded-lg">{exporting ? 'Generating...' : 'Export S-13'}</button>
      </div>

      <div className="mb-8 bg-slate-900/50 p-4 rounded-xl border border-white/5">
        {activeAssignment ? (
          <div className="flex justify-between items-center">
            <div>
              <p className="text-[#FDFBF7] font-medium text-lg">{activeAssignment.profiles?.display_name}</p>
              <p className="text-xs text-slate-500">Checked out on {new Date(activeAssignment.assigned_at).toLocaleDateString()}</p>
            </div>
            <button onClick={() => handleReturn(activeAssignment.id)} disabled={loading} className="bg-slate-700 text-[#FDFBF7] py-2 px-4 rounded-lg">Return Territory</button>
          </div>
        ) : (
          <div className="flex items-end space-x-3">
            <select value={selectedConductor} onChange={(e) => setSelectedConductor(e.target.value)} className="w-full bg-slate-800 text-[#FDFBF7] rounded-lg px-3 py-2">
              <option value="">-- Select Conductor --</option>
              {conductors.map(c => (<option key={c.id} value={c.id}>{c.display_name}</option>))}
            </select>
            <button onClick={handleAssign} disabled={loading || !selectedConductor} className="bg-teal-600 text-white py-2 px-6 rounded-lg">Assign</button>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-y-auto">
        <div className="border-l-2 border-slate-700 ml-3 space-y-6">
          {history.map((record) => (
            <div key={record.id} className="relative pl-6">
              <div className={`absolute -left-[9px] top-1.5 w-4 h-4 rounded-full border-2 border-slate-900 ${record.is_active ? 'bg-teal-400' : 'bg-slate-500'}`}></div>
              <div className="bg-slate-800/40 p-3 rounded-lg border border-white/5">
                <span className="text-[#FDFBF7] text-sm">{record.profiles?.display_name}</span>
                <div className="text-xs text-slate-500">Assigned: {new Date(record.assigned_at).toLocaleDateString()}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}