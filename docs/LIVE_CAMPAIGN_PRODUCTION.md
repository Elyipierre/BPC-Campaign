## Live Campaign Production Setup

This app is already published on GitHub Pages:

- Campaign landing page: `https://elyipierre.github.io/BPC-Campaign/`
- Campaign direct page: `https://elyipierre.github.io/BPC-Campaign/campaign.html`
- Manager: `https://elyipierre.github.io/BPC-Campaign/Territory%20Management.html`

Local campaigns already work without Supabase. The steps below are only for shared live campaigns across different phones and devices.

### 1. Fill the public runtime config

Update [assets/app-config.js](/d:/Code Projects/Territory App/assets/app-config.js):

- `supabaseUrl`
- `supabaseAnonKey`

Use the public values from Supabase:

- `Project Settings -> API -> Project URL`
- `Project Settings -> API -> anon / publishable key`

Leave `siteBaseUrl` blank so the app auto-detects the active host:

- localhost stays localhost for local testing
- GitHub Pages stays `https://elyipierre.github.io/BPC-Campaign/` in production

### 2. Apply the database schema

Run [campaign-schema.sql](/d:/Code Projects/Territory App/supabase/campaign-schema.sql) in your Supabase SQL Editor.

This creates:

- `campaigns`
- `campaign_territories`
- `campaign_load`
- `campaign_publish`
- `campaign_set_completion`

### 3. Configure Supabase Auth

In Supabase:

- `Authentication -> Providers -> Google`
  - enable Google
  - paste your Google OAuth client ID and secret

- `Authentication -> URL Configuration`
  - `Site URL`:
    - `https://elyipierre.github.io/BPC-Campaign/`
  - `Redirect URLs`:
    - `https://elyipierre.github.io/BPC-Campaign/campaign.html`
    - `http://127.0.0.1:4173/campaign.html`

### 4. Configure Google Cloud OAuth

Create or edit a Web OAuth client in Google Cloud.

Use:

- Authorized JavaScript origins:
  - `https://elyipierre.github.io`
  - `http://127.0.0.1:4173`

- Authorized redirect URIs:
  - `https://<your-project-ref>.supabase.co/auth/v1/callback`

The callback URI comes from Supabase Google provider setup.

### 5. Publish a live campaign

After the config is in place:

1. Open the manager page on GitHub Pages.
2. Go to `Campaign Mode`.
3. Add approved Google email addresses.
4. Click `Publish Live Campaign`.
5. Share the public campaign link.

Workers on approved Google accounts can then:

- open the public campaign page
- sign in with Google
- select a territory
- mark it complete
- sync progress back to the shared campaign

### 6. Verify production

Use this checklist:

- manager loads on GitHub Pages
- campaign page loads on GitHub Pages
- Google sign-in succeeds
- a non-approved account is denied
- an approved account can mark a territory complete
- the completion appears on another device and in the manager

### Notes

- Same-computer local campaigns do not need Supabase.
- Live campaigns do need Supabase because GitHub Pages is static hosting only.
- Never put a service-role key in the repo or browser config.
