/**
 * BPC Campaign - App Configuration & Supabase Initialization
 * Replace the placeholder keys with your actual Supabase Project details.
 */

// 1. Configuration Constants
const SUPABASE_URL = "https://dlncebwzunuxouyxteir.supabase.co";
// Use your Project API Key (anon/public) from Supabase Settings > API
const SUPABASE_ANON_KEY = "your-anon-key-here"; 

// 2. Initialize the Supabase Client
// Note: This assumes you have included the Supabase CDN in your HTML files:
// <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
const supabase = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

/**
 * Trigger Google OAuth Login
 * This will redirect the user to Google's login page.
 * Ensure you have added the Redirect URI in Supabase & Google Cloud Console.
 */
async function signInWithGoogle() {
    try {
        const { data, error } = await supabase.auth.signInWithOAuth({
            provider: 'google',
            options: {
                // This is where Google sends the user back after a successful login
                redirectTo: window.location.origin + '/Territory%20Management.html',
                queryParams: {
                    access_type: 'offline',
                    prompt: 'consent',
                },
            },
        });

        if (error) throw error;
    } catch (error) {
        console.error("Authentication Error:", error.message);
        alert("Failed to sign in with Google: " + error.message);
    }
}

/**
 * Sign Out Function
 */
async function signOutUser() {
    const { error } = await supabase.auth.signOut();
    if (error) console.error("Error signing out:", error.message);
    window.location.href = 'index.html'; // Redirect to login page
}

/**
 * Helper to check if a user is currently logged in
 */
async function getCurrentUser() {
    const { data: { user } } = await supabase.auth.getUser();
    return user;
}

// Export functions for use in other scripts if using modules, 
// otherwise they are globally available via the window object.
window.appConfig = {
    supabase,
    signInWithGoogle,
    signOutUser,
    getCurrentUser
};
