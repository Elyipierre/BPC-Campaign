/**
 * BPC Campaign - Supabase & Auth Configuration
 */

const SUPABASE_URL = "https://dlncebwzunuxouyxteir.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRsbmNlYnd6dW51eG91eXh0ZWlyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI4NDQ1NzgsImV4cCI6MjA4ODQyMDU3OH0.WyM45C1Co_XmG-p_g793p3mImAIHqVWRpdxGer_95qQ";

// Initialize Client
const supabaseClient = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

window.appConfig = {
    supabase: supabaseClient,
    
    // Auth Functions
    signInWithGoogle: async () => {
        const { error } = await supabaseClient.auth.signInWithOAuth({
            provider: 'google',
            options: {
                redirectTo: window.location.origin + '/Territory%20Management.html'
            }
        });
        if (error) console.error("Login failed:", error.message);
    },

    signOut: async () => {
        await supabaseClient.auth.signOut();
        window.location.href = 'index.html';
    },

    // Campaign Mode Toggle
    toggleCampaignMode: (isEnabled) => {
        const body = document.body;
        if (isEnabled) {
            body.classList.add('campaign-active');
            localStorage.setItem('campaign_mode', 'true');
        } else {
            body.classList.remove('campaign-active');
            localStorage.setItem('campaign_mode', 'false');
        }
    }
};
