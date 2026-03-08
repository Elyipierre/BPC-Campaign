import cron from 'node-cron';
import { createClient } from '@supabase/supabase-js';
import nodemailer from 'nodemailer';

const supabaseUrl = 'https://dlncebwzunuxouyxteir.supabase.co';
const supabaseKey = process.env.SUPABASE_SERVICE_KEY; 
const supabase = createClient(supabaseUrl, supabaseKey);

const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: 'your-system-email@gmail.com', pass: 'your-app-password' }
});

async function sendSMS(carrierEmail, message) {
    try {
        await transporter.sendMail({
            from: 'your-system-email@gmail.com',
            to: carrierEmail,
            subject: 'Territory Alert',
            text: message
        });
    } catch (error) {
        console.error(`❌ Failed to send SMS:`, error);
    }
}

cron.schedule('0 8 * * *', async () => {
    const { data: assignments, error } = await supabase
        .from('assignment_history')
        .select(`id, territory_id, assigned_at, territories(territory_no), profiles(display_name, phone_carrier_email)`)
        .eq('is_active', true);

    if (error || !assignments) return;

    const now = new Date();
    assignments.forEach(async (record) => {
        const diffTime = Math.abs(now - new Date(record.assigned_at));
        const daysHeld = Math.floor(diffTime / (1000 * 60 * 60 * 24));
        const conductorEmail = record.profiles?.phone_carrier_email;
        if (!conductorEmail) return;

        const terrNo = record.territories.territory_no;
        if (daysHeld === 113) await sendSMS(conductorEmail, `Reminder: Territory ${terrNo} is due in 1 week.`);
        else if (daysHeld === 119) await sendSMS(conductorEmail, `Alert: Territory ${terrNo} is due TOMORROW.`);
        else if (daysHeld === 120) await sendSMS(conductorEmail, `Final Notice: Territory ${terrNo} is due TODAY.`);
    });
});

cron.schedule('0 12 * * *', async () => {
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const tomorrowString = tomorrow.toISOString().split('T')[0];

    const { data: schedules, error } = await supabase
        .from('conductor_schedule')
        .select(`meeting_time, location, profiles(display_name, phone_carrier_email)`)
        .eq('scheduled_date', tomorrowString);

    if (error || !schedules) return;

    schedules.forEach(async (schedule) => {
        const conductorEmail = schedule.profiles?.phone_carrier_email;
        if (!conductorEmail) return;
        await sendSMS(conductorEmail, `Reminder: You are scheduled to take the lead in the ministry tomorrow at ${schedule.meeting_time} (${schedule.location}).`);
    });
});