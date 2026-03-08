import express from 'express';
import { createClient } from '@supabase/supabase-js';
import { generateS13MasterRecord } from './pdf-s13-generator.js';

const router = express.Router();
const supabaseUrl = 'https://dlncebwzunuxouyxteir.supabase.co';
const supabaseKey = process.env.SUPABASE_SERVICE_KEY; 
const supabase = createClient(supabaseUrl, supabaseKey);

router.get('/api/verify-telephone-window', async (req, res) => {
    try {
        const now = new Date();
        const formatter = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour12: false, weekday: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit' });
        const parts = formatter.formatToParts(now);
        const getTimePart = (type) => parts.find(p => p.type === type)?.value;

        const days = { 'Sun': 0, 'Mon': 1, 'Tue': 2, 'Wed': 3, 'Thu': 4, 'Fri': 5, 'Sat': 6 };
        const currentDayOfWeek = days[getTimePart('weekday')];
        const currentTime = `${getTimePart('hour')}:${getTimePart('minute')}:${getTimePart('second')}`;

        const { data: windowRule } = await supabase.from('service_account_windows').select('*').eq('day_of_week', currentDayOfWeek).single();

        if (!windowRule || !windowRule.is_enabled) return res.status(200).json({ allowed: false, reason: "Window closed or disabled." });
        if (currentTime >= windowRule.start_time && currentTime <= windowRule.end_time) return res.status(200).json({ allowed: true });
        return res.status(200).json({ allowed: false, reason: "Outside allowed hours." });
    } catch (err) {
        return res.status(500).json({ error: "Server error." });
    }
});

router.get('/api/export-s13-master', async (req, res) => {
    try {
        const serviceYear = req.query.year || new Date().getFullYear().toString();
        const { data: territories } = await supabase.from('territories').select('id, territory_no').order('territory_no', { ascending: true });
        const { data: history } = await supabase.from('assignment_history').select(`id, territory_id, assigned_at, completed_at, profiles(display_name)`).order('assigned_at', { ascending: true });

        const formattedTerritories = territories.map(t => {
            const terrHistory = history.filter(h => h.territory_id === t.id);
            return {
                territory_no: t.territory_no,
                last_completed_date: '', 
                assignment_history: terrHistory.map(h => ({
                    conductor_name: h.profiles?.display_name || 'Unknown',
                    date_assigned: new Date(h.assigned_at).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: '2-digit' }),
                    date_completed: h.completed_at ? new Date(h.completed_at).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric', year: '2-digit' }) : ''
                }))
            };
        });

        const pdfBuffer = await generateS13MasterRecord(serviceYear, formattedTerritories);
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="S-13_Master_${serviceYear}.pdf"`);
        res.send(Buffer.from(pdfBuffer));
    } catch (err) {
        res.status(500).json({ error: "Failed to generate S-13." });
    }
});

export default router;