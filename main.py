# main.py
import os
import requests
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client
import sys

# --- Configuration ---
# Your list of EPG URLs
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
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

    # Loop through each EPG URL and process it
    for url in EPG_URLS:
        try:
            print(f"\n📡 Fetching EPG data from {url}...")
            # Use stream=True for large file downloads
            response = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()

            # Decompress and parse the XML incrementally
            if url.endswith('.gz'):
                with gzip.GzipFile(fileobj=response.raw) as f:
                    context = ET.iterparse(f, events=('start', 'end'))
                    # Skip the root element
                    event, root = next(context)
            else:
                context = ET.iterparse(response.raw, events=('start', 'end'))
                event, root = next(context)

            print(f"✅ Successfully downloaded and starting to parse data from {url}.")

            channels_in_this_file = 0
            programs_in_this_file = 0

            for event, elem in context:
                if event == 'end':
                    if elem.tag == 'channel':
                        channel_id = elem.get('id')
                        display_name_node = elem.find('display-name')
                        icon_node = elem.find('icon')

                        if channel_id and display_name_node is not None and display_name_node.text:
                            all_channels_to_upsert.append({
                                'id': channel_id,
                                'display_name': display_name_node.text,
                                'icon_url': icon_node.get('src') if icon_node is not None else None
                            })
                            channels_in_this_file += 1
                        elem.clear() # Clear the element to free memory

                    elif elem.tag == 'programme':
                        channel_id = elem.get('channel')
                        start_dt = parse_xmltv_datetime(elem.get('start'))
                        end_dt = parse_xmltv_datetime(elem.get('stop'))

                        if not start_dt or not end_dt:
                            elem.clear()
                            continue

                        title_node = elem.find('title')
                        desc_node = elem.find('desc')
                        
                        program_id = f"{channel_id}_{start_dt.strftime('%Y%m%d%H%M%S')}"

                        all_programs_to_upsert.append({
                            'id': program_id,
                            'channel_id': channel_id,
                            'start_time': start_dt.isoformat(),
                            'end_time': end_dt.isoformat(),
                            'title': title_node.text if title_node is not None else 'No Title',
                            'description': desc_node.text if desc_node is not None else None
                        })
                        programs_in_this_file += 1
                        elem.clear() # Clear the element to free memory
            
            print(f"Found {channels_in_this_file} channels and {programs_in_this_file} programs.")

        except requests.exceptions.RequestException as e:
            print(f"❌ ERROR: Failed to download data from {url}: {e}", file=sys.stderr)
        except ET.ParseError as e:
            print(f"❌ ERROR: Failed to parse XML data from {url}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"❌ An unexpected error occurred with {url}: {e}", file=sys.stderr)
    
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
