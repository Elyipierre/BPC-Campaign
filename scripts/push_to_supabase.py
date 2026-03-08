import json
import os
from supabase import create_client, Client

SUPABASE_URL = "https://dlncebwzunuxouyxteir.supabase.co"
SUPABASE_KEY = "sb_publishable_HC_hjbW81AxoTu3dpX3g6Q_n3D9EQWl"
JSON_FILE = "./data/enriched_territories.json"

def initialize_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def push_data_to_supabase():
    if not os.path.exists(JSON_FILE):
        print(f"Error: Could not find {JSON_FILE}")
        return

    supabase = initialize_supabase()
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for territory in data:
        territory_id = territory.get("id")
        territory_name = territory.get("name", "Unknown")
        status_map = {"Open": "Available", "Assigned": "Assigned", "Completed": "Completed"}
        db_status = status_map.get(territory.get("status", "Open"), "Available")

        t_record = {
            "id": territory_id,
            "territory_no": territory_name,
            "locality": "Baisley Park", 
            "city": "Queens",
            "state": "NY",
            "zip": "",
            "polygon": territory.get("polygon", []),
            "status": db_status
        }
        
        try:
            supabase.table("territories").upsert(t_record).execute()
        except Exception as e:
            print(f"Failed to upsert territory {territory_name}: {e}")
            continue

        addresses = territory.get("addresses", [])
        if not addresses: continue

        address_records = []
        for addr in addresses:
            address_records.append({
                "territory_id": territory_id,
                "address_full": addr.get("full", ""),
                "apt": addr.get("apt", ""),
                "resident_name": addr.get("name", "N/A"),
                "phone": addr.get("phone", "N/A"),
                "email": addr.get("email", "N/A"),
                "is_worked": addr.get("checked", False)
            })
        
        try:
            supabase.table("addresses").delete().eq("territory_id", territory_id).execute()
            supabase.table("addresses").insert(address_records).execute()
            print(f"  -> Inserted addresses for {territory_name}")
        except Exception as e:
            print(f"  -> Failed: {e}")

if __name__ == "__main__":
    push_data_to_supabase()