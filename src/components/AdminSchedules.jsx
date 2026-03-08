import React, { useState, useEffect } from 'react';
import { supabase } from './supabaseClient'; 

export default function AdminSchedules() {
  const [windows, setWindows] = useState([]);
  const [coEmail, setCoEmail] = useState('');
  const [coDate, setCoDate] = useState('');
  const [loading, setLoading] = useState(false);
  const daysOfWeek = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

  useEffect(() => { fetchWindows(); }, []);

  const fetchWindows = async () => {
    const { data } = await supabase.from('service_account_windows').select('*').order('day_of_week', { ascending: true });
    if (data) setWindows(data);
  };

  const handleWindowChange = (index, field, value) => {
    const updated = [...windows];
    updated[index][field] = value;
    setWindows(updated);
  };

  const saveWindows = async () => {
    setLoading(true);
    await supabase.from('service_account_windows').upsert(windows);
    setLoading(false);
    alert('Schedule saved!');
  };

  const scheduleCOVisit = async (e) => {
    e.preventDefault();
    setLoading(true);
    const { error } = await supabase.from('circuit_overseer_visits').insert([{ co_email: coEmail, visit_start_date: coDate }]);
    setLoading(false);
    if (!error) { alert('CO Visit scheduled!'); setCoEmail(''); setCoDate(''); }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 to-slate-800 p-8 text-slate-200">
      <div className="max-w-4xl mx-auto space-y-8">
        <section className="bg-white/5 backdrop-blur-md border border-white/10 rounded-2xl p-6 shadow-xl">
          <div className="mb-6 border-b border-white/10 pb-4">
            <h2 className="text-2xl font-semibold text-[#FDFBF7]">Telephone Witnessing Windows</h2>
          </div>
          <div className="space-y-4">
            {windows.map((win, idx) => (
              <div key={win.id} className="flex items-center justify-between bg-slate-900/50 p-4 rounded-lg border border-white/5">
                <div className="w-32 font-medium text-[#FDFBF7]">{daysOfWeek[win.day_of_week]}</div>
                <label className="flex items-center cursor-pointer">
                  <div className="relative">
                    <input type="checkbox" className="sr-only" checked={win.is_enabled} onChange={(e) => handleWindowChange(idx, 'is_enabled', e.target.checked)} />
                    <div className={`block w-10 h-6 rounded-full transition-colors ${win.is_enabled ? 'bg-teal-500' : 'bg-slate-700'}`}></div>
                    <div className={`dot absolute left-1 top-1 bg-white w-4 h-4 rounded-full transition-transform ${win.is_enabled ? 'transform translate-x-4' : ''}`}></div>
                  </div>
                </label>
                <div className="flex items-center space-x-2">
                  <input type="time" value={win.start_time} disabled={!win.is_enabled} onChange={(e) => handleWindowChange(idx, 'start_time', e.target.value)} className="bg-slate-800 border border-slate-700 text-[#FDFBF7] rounded px-3 py-1.5 focus:border-teal-400" />
                  <span className="text-slate-500">to</span>
                  <input type="time" value={win.end_time} disabled={!win.is_enabled} onChange={(e) => handleWindowChange(idx, 'end_time', e.target.value)} className="bg-slate-800 border border-slate-700 text-[#FDFBF7] rounded px-3 py-1.5 focus:border-teal-400" />
                </div>
              </div>
            ))}
          </div>
          <div className="mt-6 flex justify-end">
            <button onClick={saveWindows} disabled={loading} className="bg-teal-600 hover:bg-teal-500 text-white font-medium py-2 px-6 rounded-lg transition-all">{loading ? 'Saving...' : 'Save Schedule'}</button>
          </div>
        </section>

        <section className="bg-[#FDFBF7] rounded-2xl p-6 shadow-xl border border-slate-200">
          <div className="mb-6 border-b border-slate-200 pb-4">
            <h2 className="text-2xl font-semibold text-slate-900">Circuit Overseer Schedule</h2>
          </div>
          <form onSubmit={scheduleCOVisit} className="flex items-end space-x-4">
            <div className="flex-1">
              <label className="block text-sm font-medium text-slate-700 mb-1">CO's Gmail Address</label>
              <input type="email" required value={coEmail} onChange={(e) => setCoEmail(e.target.value)} className="w-full bg-white border border-slate-300 text-slate-900 rounded-lg px-4 py-2" />
            </div>
            <div className="w-1/3">
              <label className="block text-sm font-medium text-slate-700 mb-1">Visit Start Date (Sunday)</label>
              <input type="date" required value={coDate} onChange={(e) => setCoDate(e.target.value)} className="w-full bg-white border border-slate-300 text-slate-900 rounded-lg px-4 py-2" />
            </div>
            <button type="submit" disabled={loading} className="bg-slate-900 hover:bg-slate-800 text-[#FDFBF7] font-medium py-2 px-6 rounded-lg">{loading ? 'Scheduling...' : 'Schedule Visit'}</button>
          </form>
        </section>
      </div>
    </div>
  );
}