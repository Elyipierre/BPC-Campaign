/**
 * BPC Campaign - Master Configuration
 */

const SUPABASE_URL = "https://dlncebwzunuxouyxteir.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRsbmNlYnd6dW51eG91eXh0ZWlyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI4NDQ1NzgsImV4cCI6MjA4ODQyMDU3OH0.WyM45C1Co_XmG-p_g793p3mImAIHqVWRpdxGer_95qQ";

// Initialize Supabase
const supabaseClient = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

window.appConfig = {
    supabase: supabaseClient,
    
    // assets/app-config.js
signInWithGoogle: async () => {
    const { error } = await supabaseClient.auth.signInWithOAuth({
        provider: 'google',
        options: {
            // Must match the new filename exactly
            redirectTo: 'https://elyipierre.github.io/territory-management.html'
        }
    });
    if (error) console.error("Login failed:", error.message);
},

    signOut: async () => {
        await supabaseClient.auth.signOut();
        window.location.href = 'index.html';
    },

    // Campaign Mode Logic
    toggleCampaignMode: (isEnabled) => {
        const body = document.body;
        const labelStd = document.getElementById('campaign-label');
        const labelCmp = document.getElementById('campaign-active-label');
        const knob = document.getElementById('toggleKnob');
        const bg = document.getElementById('campaignToggle');

        if (isEnabled) {
            body.classList.add('campaign-active');
            localStorage.setItem('campaign_mode', 'true');
            if(knob) knob.style.transform = 'translateX(24px)';
            if(bg) bg.classList.replace('bg-slate-700', 'bg-teal-500');
            if(labelStd) labelStd.style.opacity = '0.5';
            if(labelCmp) labelCmp.style.opacity = '1';
        } else {
            body.classList.remove('campaign-active');
            localStorage.setItem('campaign_mode', 'false');
            if(knob) knob.style.transform = 'translateX(4px)';
            if(bg) bg.classList.replace('bg-teal-500', 'bg-slate-700');
            if(labelStd) labelStd.style.opacity = '1';
            if(labelCmp) labelCmp.style.opacity = '0.5';
        }
    }
};

// Auto-run on load to check session and mode
document.addEventListener('DOMContentLoaded', async () => {
    const isCampaign = localStorage.getItem('campaign_mode') === 'true';
    window.appConfig.toggleCampaignMode(isCampaign);
});
