create extension if not exists pgcrypto;

create table if not exists public.campaigns (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  public_code text not null unique,
  worker_pin_hash text not null default '',
  created_by text not null default '',
  created_by_user_id uuid,
  approved_emails jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

alter table public.campaigns
  add column if not exists created_by_user_id uuid,
  add column if not exists approved_emails jsonb not null default '[]'::jsonb;

create table if not exists public.campaign_territories (
  id bigint generated always as identity primary key,
  campaign_id uuid not null references public.campaigns(id) on delete cascade,
  territory_id text not null,
  territory_no text not null,
  locality text not null default '',
  polygon jsonb not null,
  label_anchor jsonb,
  completed boolean not null default false,
  completed_by text not null default '',
  completed_by_user_id uuid,
  completed_by_email text not null default '',
  completed_by_avatar_url text not null default '',
  completed_at timestamptz,
  updated_at timestamptz not null default timezone('utc', now()),
  unique (campaign_id, territory_id)
);

alter table public.campaign_territories
  add column if not exists completed_by_user_id uuid,
  add column if not exists completed_by_email text not null default '',
  add column if not exists completed_by_avatar_url text not null default '';

create index if not exists idx_campaign_territories_campaign_id on public.campaign_territories(campaign_id);
create index if not exists idx_campaigns_public_code on public.campaigns(public_code);

alter table public.campaigns enable row level security;
alter table public.campaign_territories enable row level security;
alter table public.campaign_territories replica identity full;

drop policy if exists "campaigns_select_public" on public.campaigns;
create policy "campaigns_select_public"
on public.campaigns
for select
to anon, authenticated
using (true);

drop policy if exists "campaign_territories_select_public" on public.campaign_territories;
create policy "campaign_territories_select_public"
on public.campaign_territories
for select
to anon, authenticated
using (true);

grant usage on schema public to anon, authenticated;
grant select on public.campaigns to anon, authenticated;
grant select on public.campaign_territories to anon, authenticated;

alter publication supabase_realtime add table public.campaign_territories;

create or replace function public.set_campaign_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists trg_campaigns_updated_at on public.campaigns;
create trigger trg_campaigns_updated_at
before update on public.campaigns
for each row
execute function public.set_campaign_updated_at();

drop trigger if exists trg_campaign_territories_updated_at on public.campaign_territories;
create trigger trg_campaign_territories_updated_at
before update on public.campaign_territories
for each row
execute function public.set_campaign_updated_at();

create or replace function public.generate_campaign_public_code()
returns text
language plpgsql
as $$
declare
  candidate text;
begin
  loop
    candidate := upper(substr(replace(gen_random_uuid()::text, '-', ''), 1, 8));
    exit when not exists (
      select 1
      from public.campaigns
      where public_code = candidate
    );
  end loop;
  return candidate;
end;
$$;

create or replace function public.normalize_campaign_email_list(p_emails jsonb)
returns jsonb
language sql
immutable
as $$
  with normalized as (
    select distinct lower(trim(value)) as email
    from jsonb_array_elements_text(
      case
        when jsonb_typeof(coalesce(p_emails, '[]'::jsonb)) = 'array' then coalesce(p_emails, '[]'::jsonb)
        else '[]'::jsonb
      end
    ) as rows(value)
    where trim(value) <> ''
      and trim(value) ~* '^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$'
  )
  select coalesce(jsonb_agg(email order by email), '[]'::jsonb)
  from normalized;
$$;

create or replace function public.campaign_email_is_approved(p_approved_emails jsonb, p_email text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from jsonb_array_elements_text(public.normalize_campaign_email_list(p_approved_emails)) as emails(value)
    where value = lower(trim(coalesce(p_email, '')))
  );
$$;

create or replace function public.campaign_current_user_email()
returns text
language sql
stable
as $$
  select lower(trim(coalesce(
    nullif(auth.jwt() ->> 'email', ''),
    nullif(auth.jwt() -> 'user_metadata' ->> 'email', ''),
    ''
  )));
$$;

create or replace function public.campaign_current_user_display_name()
returns text
language sql
stable
as $$
  select trim(coalesce(
    nullif(auth.jwt() -> 'user_metadata' ->> 'full_name', ''),
    nullif(auth.jwt() -> 'user_metadata' ->> 'name', ''),
    nullif(auth.jwt() -> 'user_metadata' ->> 'preferred_username', ''),
    nullif(auth.jwt() ->> 'email', ''),
    ''
  ));
$$;

create or replace function public.campaign_current_user_avatar_url()
returns text
language sql
stable
as $$
  select trim(coalesce(
    nullif(auth.jwt() -> 'user_metadata' ->> 'avatar_url', ''),
    nullif(auth.jwt() -> 'user_metadata' ->> 'picture', ''),
    ''
  ));
$$;

create or replace function public.campaign_build_payload(p_campaign_id uuid)
returns jsonb
language sql
stable
as $$
  with selected_campaign as (
    select id, name, public_code, created_by, created_by_user_id, approved_emails, created_at, updated_at
    from public.campaigns
    where id = p_campaign_id
  ),
  selected_territories as (
    select
      territory_id,
      territory_no,
      locality,
      polygon,
      label_anchor,
      completed,
      completed_by,
      completed_by_user_id,
      completed_by_email,
      completed_by_avatar_url,
      completed_at,
      updated_at
    from public.campaign_territories
    where campaign_id = p_campaign_id
    order by territory_no asc, territory_id asc
  )
  select jsonb_build_object(
    'schema_version', 2,
    'campaign', (
      select jsonb_build_object(
        'id', id,
        'name', name,
        'public_code', public_code,
        'created_by', created_by,
        'created_by_user_id', created_by_user_id,
        'approved_emails', approved_emails,
        'created_at', created_at,
        'updated_at', updated_at
      )
      from selected_campaign
    ),
    'viewer', (
      select jsonb_build_object(
        'signed_in', auth.uid() is not null,
        'user_id', auth.uid(),
        'email', public.campaign_current_user_email(),
        'display_name', public.campaign_current_user_display_name(),
        'avatar_url', public.campaign_current_user_avatar_url(),
        'authorized', public.campaign_email_is_approved(approved_emails, public.campaign_current_user_email())
      )
      from selected_campaign
    ),
    'territories', coalesce((
      select jsonb_agg(jsonb_build_object(
        'territory_id', territory_id,
        'territory_no', territory_no,
        'locality', locality,
        'polygon', polygon,
        'label_anchor', label_anchor,
        'completed', completed,
        'completed_by', completed_by,
        'completed_by_user_id', completed_by_user_id,
        'completed_by_email', completed_by_email,
        'completed_by_avatar_url', completed_by_avatar_url,
        'completed_at', completed_at,
        'updated_at', updated_at
      ))
      from selected_territories
    ), '[]'::jsonb)
  );
$$;

create or replace function public.campaign_load(p_public_code text)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_campaign_id uuid;
  v_code text := upper(trim(coalesce(p_public_code, '')));
begin
  if v_code = '' then
    raise exception 'Campaign code is required.';
  end if;

  select id
  into v_campaign_id
  from public.campaigns
  where public_code = v_code;

  if v_campaign_id is null then
    raise exception 'Campaign not found for code %.', v_code;
  end if;

  return public.campaign_build_payload(v_campaign_id);
end;
$$;

drop function if exists public.campaign_publish(text, text, text, text, jsonb);
drop function if exists public.campaign_publish(text, text, text, jsonb, jsonb);

create or replace function public.campaign_publish(
  p_name text,
  p_public_code text,
  p_created_by text default '',
  p_snapshot jsonb default '[]'::jsonb,
  p_approved_emails jsonb default '[]'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_code text := upper(trim(coalesce(p_public_code, '')));
  v_name text := trim(coalesce(p_name, ''));
  v_campaign public.campaigns%rowtype;
  v_entry jsonb;
  v_approved_emails jsonb := public.normalize_campaign_email_list(p_approved_emails);
begin
  if v_name = '' then
    raise exception 'Campaign name is required.';
  end if;
  if jsonb_typeof(coalesce(p_snapshot, '[]'::jsonb)) <> 'array' then
    raise exception 'Campaign snapshot must be a JSON array.';
  end if;
  if jsonb_array_length(coalesce(p_snapshot, '[]'::jsonb)) = 0 then
    raise exception 'Campaign snapshot is empty.';
  end if;
  if jsonb_array_length(v_approved_emails) = 0 then
    raise exception 'At least one approved Google email is required.';
  end if;
  if v_code = '' then
    v_code := public.generate_campaign_public_code();
  end if;

  select *
  into v_campaign
  from public.campaigns
  where public_code = v_code
  for update;

  if found then
    update public.campaigns
    set name = v_name,
        created_by = trim(coalesce(p_created_by, '')),
        created_by_user_id = coalesce(auth.uid(), created_by_user_id),
        approved_emails = v_approved_emails,
        updated_at = timezone('utc', now())
    where id = v_campaign.id
    returning * into v_campaign;
    delete from public.campaign_territories where campaign_id = v_campaign.id;
  else
    insert into public.campaigns (
      name,
      public_code,
      worker_pin_hash,
      created_by,
      created_by_user_id,
      approved_emails
    )
    values (
      v_name,
      v_code,
      '',
      trim(coalesce(p_created_by, '')),
      auth.uid(),
      v_approved_emails
    )
    returning * into v_campaign;
  end if;

  for v_entry in
    select value
    from jsonb_array_elements(coalesce(p_snapshot, '[]'::jsonb))
  loop
    insert into public.campaign_territories (
      campaign_id,
      territory_id,
      territory_no,
      locality,
      polygon,
      label_anchor,
      completed,
      completed_by,
      completed_by_user_id,
      completed_by_email,
      completed_by_avatar_url,
      completed_at
    )
    values (
      v_campaign.id,
      trim(coalesce(v_entry ->> 'territory_id', '')),
      trim(coalesce(v_entry ->> 'territory_no', '')),
      trim(coalesce(v_entry ->> 'locality', '')),
      coalesce(v_entry -> 'polygon', '[]'::jsonb),
      case
        when jsonb_typeof(v_entry -> 'label_anchor') = 'object' then v_entry -> 'label_anchor'
        else null
      end,
      false,
      '',
      null,
      '',
      '',
      null
    );
  end loop;

  return public.campaign_build_payload(v_campaign.id);
end;
$$;

drop function if exists public.campaign_set_completion(text, text, text, boolean, text);
drop function if exists public.campaign_set_completion(text, text, boolean);

create or replace function public.campaign_set_completion(
  p_public_code text,
  p_territory_id text,
  p_completed boolean
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_campaign public.campaigns%rowtype;
  v_territory public.campaign_territories%rowtype;
  v_territory_id text := trim(coalesce(p_territory_id, ''));
  v_user_email text := public.campaign_current_user_email();
  v_display_name text := public.campaign_current_user_display_name();
  v_avatar_url text := public.campaign_current_user_avatar_url();
  v_user_id uuid := auth.uid();
begin
  if trim(coalesce(p_public_code, '')) = '' then
    raise exception 'Campaign code is required.';
  end if;
  if v_territory_id = '' then
    raise exception 'Territory id is required.';
  end if;

  select *
  into v_campaign
  from public.campaigns
  where public_code = upper(trim(p_public_code))
  for update;

  if not found then
    raise exception 'Campaign not found.';
  end if;

  if coalesce(p_completed, false) then
    if v_user_id is null then
      raise exception 'Google sign-in is required to complete a territory.';
    end if;
    if v_user_email = '' or not public.campaign_email_is_approved(v_campaign.approved_emails, v_user_email) then
      raise exception 'Your Google account is not approved for this campaign.';
    end if;
  end if;

  select *
  into v_territory
  from public.campaign_territories
  where campaign_id = v_campaign.id
    and territory_id = v_territory_id
  for update;

  if not found then
    raise exception 'Territory % is not part of this campaign.', v_territory_id;
  end if;

  if not coalesce(p_completed, false) and coalesce(v_territory.completed, false) then
    if not (
      (v_user_id is not null and v_territory.completed_by_user_id is not null and v_user_id = v_territory.completed_by_user_id)
      or (v_user_id is not null and v_territory.completed_by_user_id is null and v_user_email <> '' and lower(v_user_email) = lower(coalesce(v_territory.completed_by_email, '')))
      or (v_user_id is not null and v_campaign.created_by_user_id is not null and v_user_id = v_campaign.created_by_user_id)
    ) then
      raise exception 'Only the worker who completed this territory or the campaign owner can reopen it.';
    end if;
  end if;

  update public.campaign_territories
  set completed = coalesce(p_completed, false),
      completed_by = case
        when coalesce(p_completed, false) then coalesce(nullif(v_display_name, ''), v_user_email)
        else ''
      end,
      completed_by_user_id = case
        when coalesce(p_completed, false) then v_user_id
        else null
      end,
      completed_by_email = case
        when coalesce(p_completed, false) then v_user_email
        else ''
      end,
      completed_by_avatar_url = case
        when coalesce(p_completed, false) then v_avatar_url
        else ''
      end,
      completed_at = case
        when coalesce(p_completed, false) then timezone('utc', now())
        else null
      end,
      updated_at = timezone('utc', now())
  where campaign_id = v_campaign.id
    and territory_id = v_territory_id;

  update public.campaigns
  set updated_at = timezone('utc', now())
  where id = v_campaign.id;

  return public.campaign_build_payload(v_campaign.id);
end;
$$;

grant execute on function public.campaign_load(text) to anon, authenticated;
grant execute on function public.campaign_publish(text, text, text, jsonb, jsonb) to anon, authenticated;
grant execute on function public.campaign_set_completion(text, text, boolean) to anon, authenticated;
