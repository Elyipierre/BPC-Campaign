import React, { useState, useEffect } from 'react';
import { supabase } from './supabaseClient';

export default function DoNotCallRegistry({ territoryId }) {
  const [dncs, setDncs] = useState([]);
  const [address, setAddress] = useState('');
  const [apt, setApt] = useState('');
  const [notes, setNotes] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => { if (territoryId) fetchDNCs(); }, [territoryId]);

  const fetchDNCs = async () => {
    const { data } = await supabase.from('do_not_calls').select('*').eq('territory_id', territoryId).order('created_at', { ascending: false });
    if (data) setDncs(data);
  };

  const handleAddDNC = async (e) => {
    e.preventDefault();
    setLoading(true);
    const { data: userAuth } = await supabase.auth.getUser();
    await supabase.from('do_not_calls').insert([{ territory_id: territoryId, address_full: address, apt: apt, notes: notes, added_by: userAuth?.user?.id }]);
    setLoading(false);
    setAddress(''); setApt(''); setNotes('');
    fetchDNCs();
  };

  const handleRemoveDNC = async (id) => {
    if (!window.confirm("Remove this address?")) return;
    await supabase.from('do_not_calls').delete().eq('id', id);
    fetchDNCs();
  };

  return (
    <div className="bg-white/5 backdrop-blur-md border border-red-500/20 rounded-2xl p-6">
      <h2 className="text-2xl font-semibold text-[#FDFBF7] mb-6 border-b border-red-500/20 pb-4">Do Not Call Registry</h2>
      <form onSubmit={handleAddDNC} className="mb-8 flex space-x-3 bg-slate-900/50 p-4 rounded-xl">
        <input type="text" required value={address} onChange={(e) => setAddress(e.target.value)} placeholder="Address" className="flex-1 bg-slate-800 text-[#FDFBF7] rounded-lg px-3 py-2" />
        <input type="text" value={apt} onChange={(e) => setApt(e.target.value)} placeholder="Apt" className="w-24 bg-slate-800 text-[#FDFBF7] rounded-lg px-3 py-2" />
        <input type="text" value={notes} onChange={(e) => setNotes(e.target.value)} placeholder="Notes" className="w-1/3 bg-slate-800 text-[#FDFBF7] rounded-lg px-3 py-2" />
        <button type="submit" disabled={loading} className="bg-red-900/60 text-red-100 py-2 px-4 rounded-lg">Add DNC</button>
      </form>
      <div className="space-y-2 max-h-64 overflow-y-auto">
        {dncs.map((dnc) => (
          <div key={dnc.id} className="flex justify-between bg-slate-800/40 p-3 rounded-lg">
            <span className="text-[#FDFBF7] text-sm">{dnc.address_full} {dnc.apt}</span>
            <button onClick={() => handleRemoveDNC(dnc.id)} className="text-red-400">X</button>
          </div>
        ))}
      </div>
    </div>
  );
}