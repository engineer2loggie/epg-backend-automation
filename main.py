# main.py
import os
import requests
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client
import sys

# --- Configuration ---
# Your list of EPG filenames from https://github.com/iptv-org/epg/blob/master/SITES.md
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz",
]



# --- Supabase Configuration ---
# These will be loaded from environment variables (GitHub Secrets)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

def initialize_supabase_client():
    """Initializes and returns the Supabase client, exiting if config is missing."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("❌ ERROR: Supabase URL and Service Key must be set as environment variables in GitHub Secrets.", file=sys.stderr)
        sys.exit(1)
    
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        print("✅ Successfully connected to Supabase.")
        return client
    except Exception as e:
        print(f"❌ ERROR: Failed to connect to Supabase: {e}", file=sys.stderr)
        sys.exit(1)

def parse_xmltv_datetime(dt_str):
    """Parses the unique XMLTV datetime format into a timezone-aware datetime object."""
    try:
        if ' ' in dt_str:
            parts = dt_str.rsplit(' ', 1)
            dt_str = ''.join(parts)
        return datetime.strptime(dt_str, '%Y%m%d%H%M%S%z')
    except (ValueError, TypeError):
        return None # Return None if format is invalid

def fetch_and_process_epg(supabase: Client):
    """Fetches, parses, and upserts EPG data into Supabase."""
    print("🚀 Starting EPG update process...")

    all_channels_to_upsert = []
    all_programs_to_upsert = []
    valid_channel_ids = set()

    # Loop through each EPG URL and process it
    for url in EPG_URLS: # Your loop variable is named 'url'
        is_gzipped = url.endswith('.gz')
        
        print(f"\n📡 Fetching EPG data from {url}...")
        
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            if is_gzipped:
                with gzip.GzipFile(fileobj=response.raw) as f:
                    xml_content = f.read()
            else:
                xml_content = response.content
            
            # This is the line you need to change.
            # Change 'filename' to 'url'.
            print(f"✅ Successfully downloaded data from {url}.")
            root = ET.fromstring(xml_content)

            # --- Process Channels from this file ---
            for channel_node in root.findall('channel'):
                channel_id = channel_node.get('id')
                display_name_node = channel_node.find('display-name')
                icon_node = channel_node.find('icon')
                if channel_id and channel_id not in valid_channel_ids and display_name_node is not None and display_name_node.text:
                    all_channels_to_upsert.append({
                        'id': channel_id,
                        'display_name': display_name_node.text,
                        'icon_url': icon_node.get('src') if icon_node is not None else None
                    })
                    valid_channel_ids.add(channel_id)

            # --- Process Programs from this file ---
            for program_node in root.findall('programme'):
                channel_id = program_node.get('channel')
                start_dt = parse_xmltv_datetime(program_node.get('start'))
                end_dt = parse_xmltv_datetime(program_node.get('stop'))

                if not start_dt or not end_dt:
                    continue

                title_node = program_node.find('title')
                desc_node = program_node.find('desc')
                
                # Create a unique ID for each program
                program_id = f"{channel_id}_{start_dt.strftime('%Y%m%d%H%M%S')}"

                all_programs_to_upsert.append({
                    'id': program_id,
                    'channel_id': channel_id,
                    'start_time': start_dt.isoformat(),
                    'end_time': end_dt.isoformat(),
                    'title': title_node.text if title_node is not None else 'No Title',
                    'description': desc_node.text if desc_node is not None else None
                })
        
        except requests.exceptions.RequestException as e:
            print(f"❌ ERROR: Failed to fetch EPG data from {full_url}: {e}", file=sys.stderr)
            continue # Skip to the next URL on failure
        except ET.ParseError as e:
            print(f"❌ ERROR: Failed to parse XML data from {full_url}: {e}", file=sys.stderr)
            continue # Skip to the next URL on failure
    
    # --- Bulk Upsert Channels and Programs after looping through all files ---
    if all_channels_to_upsert:
        print(f"\n--- Upserting {len(all_channels_to_upsert)} Channels ---")
        try:
            supabase.table('channels').upsert(all_channels_to_upsert).execute()
            print("✅ Channels upserted successfully.")
        except Exception as e:
            print(f"❌ ERROR: Failed to upsert channels: {e}", file=sys.stderr)

    if all_programs_to_upsert:
        print(f"\n--- Upserting {len(all_programs_to_upsert)} Programs ---")
        try:
            # We use `on_conflict='id'` to ensure we update existing rows if they exist
            supabase.table('programs').upsert(all_programs_to_upsert, on_conflict='id').execute()
            print("✅ Programs upserted successfully.")
        except Exception as e:
            print(f"❌ ERROR: Failed to upsert programs: {e}", file=sys.stderr)
    
    # --- Cleanup Old Programs ---
    print("\n--- Cleaning up old programs ---")
    try:
        # Delete programs that ended more than 24 hours ago
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        supabase.table('programs').delete().lt('end_time', yesterday).execute()
        print("✅ Successfully deleted old programs.")
    except Exception as e:
        print(f"⚠️ WARNING: Could not delete old programs. Error: {e}")
    
    print("\n✅ EPG update process completed!")

if __name__ == "__main__":
    supabase_client = initialize_supabase_client()
    fetch_and_process_epg(supabase_client)
