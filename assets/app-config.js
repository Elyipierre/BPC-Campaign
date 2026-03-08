/**
 * BPC Campaign - Master Configuration & Auth
 */
const SUPABASE_URL = "https://dlncebwzunuxouyxteir.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRsbmNlYnd6dW51eG91eXh0ZWlyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI4NDQ1NzgsImV4cCI6MjA4ODQyMDU3OH0.WyM45C1Co_XmG-p_g793p3mImAIHqVWRpdxGer_95qQ";

const supabaseClient = typeof supabase !== 'undefined' ? supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY) : null;

window.appConfig = {
    supabase: supabaseClient,
    
    signInWithGoogle: async () => {
        if (!supabaseClient) return alert("Supabase library not loaded.");
        
        // Dynamically routes back to your exact dashboard URL (handling the space in the filename)
        const basePath = window.location.pathname.replace('index.html', '');
        const targetUrl = `${window.location.origin}${basePath}Territory%20Management.html`;

        const { error } = await supabaseClient.auth.signInWithOAuth({
            provider: 'google',
            options: { redirectTo: targetUrl }
        });
        if (error) console.error("Login failed:", error.message);
    },

    signOut: async () => {
        if (supabaseClient) await supabaseClient.auth.signOut();
        window.location.href = 'index.html';
    }
};
