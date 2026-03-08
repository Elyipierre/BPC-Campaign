import asyncio
import csv
import os
import json
import random
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async

# Configuration
CSV_DIRECTORY = "./data/territories/"
OUTPUT_JSON = "./data/enriched_territories.json"

# Proxy Configuration (Replace with your proxy provider's details)
PROXY_LIST = [
    "http://user:pass@proxy1.example.com:8080",
    "http://user:pass@proxy2.example.com:8080",
    "http://user:pass@proxy3.example.com:8080"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
]

def format_cbc_url(raw_address):
    try:
        parts = raw_address.split(',')
        if len(parts) < 3:
            return None
        street = parts[0].strip().lower().replace(' ', '-')
        city = parts[1].strip().lower().replace(' ', '-')
        state_zip = parts[2].strip().split()
        if len(state_zip) < 2:
            return None
        state = state_zip[0].lower()
        zipcode = state_zip[1]
        return f"https://www.cyberbackgroundchecks.com/address/{street}/{city}/{state}/{zipcode}"
    except Exception as e:
        print(f"Error parsing address {raw_address}: {e}")
        return None

async def scrape_address(page, url):
    if not url:
        return {"name": "N/A", "phone": "N/A", "email": "N/A"}
    try:
        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
        try:
            await page.wait_for_selector('.name-list', timeout=10000)
        except:
            return {"name": "No current records found", "phone": "N/A", "email": "N/A"}

        name_locator = page.locator('.name-list .name-link').first
        phone_locator = page.locator('.name-list .phone-link').first
        email_locator = page.locator('.name-list .email-link').first

        name = await name_locator.text_content() if await name_locator.count() > 0 else "N/A"
        phone = await phone_locator.text_content() if await phone_locator.count() > 0 else "N/A"
        email = await email_locator.text_content() if await email_locator.count() > 0 else "N/A"

        return {
            "name": name.strip() if name else "N/A",
            "phone": phone.strip() if phone else "N/A",
            "email": email.strip() if email else "N/A"
        }
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        return {"name": "Error fetching", "phone": "N/A", "email": "N/A"}

async def process_territory_file(filepath, territory_name, proxy):
    territory_obj = {
        "id": f"import_csv_{territory_name.replace(' ', '_')}",
        "name": territory_name,
        "status": "Available",
        "progress": 0,
        "assignee": None,
        "polygon": [], 
        "addresses": []
    }

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False, proxy={"server": proxy}) 
        selected_ua = random.choice(USER_AGENTS)
        context = await browser.new_context(user_agent=selected_ua)
        page = await context.new_page()
        await stealth_async(page)

        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_address = row.get("Complete Address", "")
                if not raw_address or raw_address.strip() == "No addresses fetched":
                    continue
                    
                url = format_cbc_url(raw_address)
                print(f"  -> Scraping: {raw_address} (Proxy: {proxy.split('@')[-1]})")
                resident_info = await scrape_address(page, url)
                
                territory_obj["addresses"].append({
                    "full": raw_address,
                    "apt": "", 
                    "name": resident_info["name"],
                    "phone": resident_info["phone"],
                    "email": resident_info["email"],
                    "checked": False
                })
                await asyncio.sleep(random.uniform(2.0, 6.0)) 

        await browser.close()
        return territory_obj

async def main():
    if not os.path.exists(CSV_DIRECTORY):
        print(f"Directory '{CSV_DIRECTORY}' not found.")
        return
    enriched_data = []
    proxy_index = 0
    csv_files = [f for f in os.listdir(CSV_DIRECTORY) if f.endswith(".csv")]

    for filename in csv_files:
        territory_name = filename.replace('.csv', '')
        print(f"\nProcessing Territory: {territory_name}")
        filepath = os.path.join(CSV_DIRECTORY, filename)
        current_proxy = PROXY_LIST[proxy_index % len(PROXY_LIST)]
        proxy_index += 1
        territory_data = await process_territory_file(filepath, territory_name, current_proxy)
        enriched_data.append(territory_data)

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(enriched_data, f, indent=2)
    print(f"\nFinished! Saved to {OUTPUT_JSON}")

if __name__ == "__main__":
    asyncio.run(main())