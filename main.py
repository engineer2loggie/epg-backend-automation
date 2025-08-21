{\rtf1\ansi\ansicpg1252\cocoartf2759
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red77\green80\blue85;\red236\green241\blue247;\red0\green0\blue0;
\red111\green14\blue195;\red24\green112\blue43;\red164\green69\blue11;}
{\*\expandedcolortbl;;\cssrgb\c37255\c38824\c40784;\cssrgb\c94118\c95686\c97647;\cssrgb\c0\c0\c0;
\cssrgb\c51765\c18824\c80784;\cssrgb\c9412\c50196\c21961;\cssrgb\c70980\c34902\c3137;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs28 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 # main.py\cf0 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5 import\cf0 \strokec4  os\cb1 \
\cf5 \cb3 \strokec5 import\cf0 \strokec4  requests\cb1 \
\cf5 \cb3 \strokec5 import\cf0 \strokec4  gzip\cb1 \
\cf5 \cb3 \strokec5 import\cf0 \strokec4  xml.etree.ElementTree \cf5 \strokec5 as\cf0 \strokec4  ET\cb1 \
\cf5 \cb3 \strokec5 from\cf0 \strokec4  datetime \cf5 \strokec5 import\cf0 \strokec4  datetime, timezone\cb1 \
\cf5 \cb3 \strokec5 from\cf0 \strokec4  supabase \cf5 \strokec5 import\cf0 \strokec4  create_client, Client\cb1 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 # --- Configuration ---\cf0 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 # Choose an EPG source from https://github.com/iptv-org/epg/blob/master/SITES.md\cf0 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 # e.g., 'guides/tvguide.com.epg.xml.gz' for a comprehensive US guide.\cf0 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf0 \cb3 EPG_SOURCE_PATH = \cf6 \strokec6 'guides/tvguide.com.epg.xml.gz'\cf0 \cb1 \strokec4 \
\cb3 IPTV_ORG_EPG_URL = \cf6 \strokec6 f'https://github.com/iptv-org/epg/raw/master/\cf0 \strokec4 \{EPG_SOURCE_PATH\}\cf6 \strokec6 '\cf0 \cb1 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 # --- Supabase Configuration ---\cf0 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 # These will be loaded from environment variables (GitHub Secrets)\cf0 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf0 \cb3 SUPABASE_URL = os.environ.get(\cf6 \strokec6 "SUPABASE_URL"\cf0 \strokec4 )\cb1 \
\cb3 SUPABASE_SERVICE_KEY = os.environ.get(\cf6 \strokec6 "SUPABASE_SERVICE_KEY"\cf0 \strokec4 )\cb1 \
\
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5 if\cf0 \strokec4  \cf5 \strokec5 not\cf0 \strokec4  SUPABASE_URL \cf5 \strokec5 or\cf0 \strokec4  \cf5 \strokec5 not\cf0 \strokec4  SUPABASE_SERVICE_KEY:\cb1 \
\pard\pardeftab720\partightenfactor0
\cf0 \cb3     \cf5 \strokec5 raise\cf0 \strokec4  ValueError(\cf6 \strokec6 "Supabase URL and Service Key must be set as environment variables."\cf0 \strokec4 )\cb1 \
\
\cb3 supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)\cb1 \
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 "Successfully connected to Supabase."\cf0 \strokec4 )\cb1 \
\
\cf5 \cb3 \strokec5 def\cf0 \strokec4  parse_xmltv_datetime(dt_str):\cb1 \
\pard\pardeftab720\partightenfactor0
\cf0 \cb3     \cf6 \strokec6 """Parses the unique XMLTV datetime format into a timezone-aware datetime object."""\cf0 \cb1 \strokec4 \
\cb3     \cf2 \strokec2 # Format is like '20230821140000 +0000'\cf0 \cb1 \strokec4 \
\cb3     \cf2 \strokec2 # We remove the space before the timezone offset to match Python's strptime format\cf0 \cb1 \strokec4 \
\cb3     \cf5 \strokec5 if\cf0 \strokec4  \cf6 \strokec6 ' '\cf0 \strokec4  \cf5 \strokec5 in\cf0 \strokec4  dt_str:\cb1 \
\cb3         parts = dt_str.rsplit(\cf6 \strokec6 ' '\cf0 \strokec4 , \cf7 \strokec7 1\cf0 \strokec4 )\cb1 \
\cb3         dt_str = \cf6 \strokec6 ''\cf0 \strokec4 .join(parts)\cb1 \
\cb3     \cb1 \
\cb3     \cf2 \strokec2 # Python's %z can handle timezone offsets like +0000 or -0500\cf0 \cb1 \strokec4 \
\cb3     \cf5 \strokec5 return\cf0 \strokec4  datetime.strptime(dt_str, \cf6 \strokec6 '%Y%m%d%H%M%S%z'\cf0 \strokec4 )\cb1 \
\
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5 def\cf0 \strokec4  fetch_and_process_epg():\cb1 \
\pard\pardeftab720\partightenfactor0
\cf0 \cb3     \cf6 \strokec6 """Fetches, parses, and upserts EPG data into Supabase."""\cf0 \cb1 \strokec4 \
\cb3     \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"Fetching EPG data from \cf0 \strokec4 \{IPTV_ORG_EPG_URL\}\cf6 \strokec6 ..."\cf0 \strokec4 )\cb1 \
\cb3     \cb1 \
\cb3     \cf5 \strokec5 try\cf0 \strokec4 :\cb1 \
\cb3         response = requests.get(IPTV_ORG_EPG_URL, stream=\cf5 \strokec5 True\cf0 \strokec4 )\cb1 \
\cb3         response.raise_for_status()\cb1 \
\
\cb3         \cf2 \strokec2 # Decompress the .gz file content\cf0 \cb1 \strokec4 \
\cb3         \cf5 \strokec5 with\cf0 \strokec4  gzip.GzipFile(fileobj=response.raw) \cf5 \strokec5 as\cf0 \strokec4  f:\cb1 \
\cb3             xml_content = f.read()\cb1 \
\cb3         \cb1 \
\cb3         \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 "Successfully downloaded and decompressed EPG data."\cf0 \strokec4 )\cb1 \
\cb3         root = ET.fromstring(xml_content)\cb1 \
\cb3         \cb1 \
\cb3         \cf2 \strokec2 # --- Process Channels ---\cf0 \cb1 \strokec4 \
\cb3         channels_to_upsert = []\cb1 \
\cb3         \cf5 \strokec5 for\cf0 \strokec4  channel_node \cf5 \strokec5 in\cf0 \strokec4  root.findall(\cf6 \strokec6 'channel'\cf0 \strokec4 ):\cb1 \
\cb3             channel_id = channel_node.get(\cf6 \strokec6 'id'\cf0 \strokec4 )\cb1 \
\cb3             display_name_node = channel_node.find(\cf6 \strokec6 'display-name'\cf0 \strokec4 )\cb1 \
\cb3             icon_node = channel_node.find(\cf6 \strokec6 'icon'\cf0 \strokec4 )\cb1 \
\
\cb3             \cf5 \strokec5 if\cf0 \strokec4  channel_id \cf5 \strokec5 and\cf0 \strokec4  display_name_node \cf5 \strokec5 is\cf0 \strokec4  \cf5 \strokec5 not\cf0 \strokec4  \cf5 \strokec5 None\cf0 \strokec4 :\cb1 \
\cb3                 channels_to_upsert.append(\{\cb1 \
\cb3                     \cf6 \strokec6 'id'\cf0 \strokec4 : channel_id,\cb1 \
\cb3                     \cf6 \strokec6 'display_name'\cf0 \strokec4 : display_name_node.text,\cb1 \
\cb3                     \cf6 \strokec6 'icon_url'\cf0 \strokec4 : icon_node.get(\cf6 \strokec6 'src'\cf0 \strokec4 ) \cf5 \strokec5 if\cf0 \strokec4  icon_node \cf5 \strokec5 is\cf0 \strokec4  \cf5 \strokec5 not\cf0 \strokec4  \cf5 \strokec5 None\cf0 \strokec4  \cf5 \strokec5 else\cf0 \strokec4  \cf5 \strokec5 None\cf0 \cb1 \strokec4 \
\cb3                 \})\cb1 \
\cb3         \cb1 \
\cb3         \cf5 \strokec5 if\cf0 \strokec4  channels_to_upsert:\cb1 \
\cb3             \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"Upserting \cf0 \strokec4 \{len(channels_to_upsert)\}\cf6 \strokec6  channels..."\cf0 \strokec4 )\cb1 \
\cb3             \cf2 \strokec2 # 'upsert' will insert new channels and update existing ones based on the primary key ('id')\cf0 \cb1 \strokec4 \
\cb3             supabase.table(\cf6 \strokec6 'channels'\cf0 \strokec4 ).upsert(channels_to_upsert).execute()\cb1 \
\
\cb3         \cf2 \strokec2 # --- Process Programs ---\cf0 \cb1 \strokec4 \
\cb3         programs_to_insert = []\cb1 \
\cb3         \cf5 \strokec5 for\cf0 \strokec4  program_node \cf5 \strokec5 in\cf0 \strokec4  root.findall(\cf6 \strokec6 'programme'\cf0 \strokec4 ):\cb1 \
\cb3             title_node = program_node.find(\cf6 \strokec6 'title'\cf0 \strokec4 )\cb1 \
\cb3             desc_node = program_node.find(\cf6 \strokec6 'desc'\cf0 \strokec4 )\cb1 \
\cb3             \cb1 \
\cb3             programs_to_insert.append(\{\cb1 \
\cb3                 \cf6 \strokec6 'channel_id'\cf0 \strokec4 : program_node.get(\cf6 \strokec6 'channel'\cf0 \strokec4 ),\cb1 \
\cb3                 \cf6 \strokec6 'start_time'\cf0 \strokec4 : parse_xmltv_datetime(program_node.get(\cf6 \strokec6 'start'\cf0 \strokec4 )).isoformat(),\cb1 \
\cb3                 \cf6 \strokec6 'end_time'\cf0 \strokec4 : parse_xmltv_datetime(program_node.get(\cf6 \strokec6 'stop'\cf0 \strokec4 )).isoformat(),\cb1 \
\cb3                 \cf6 \strokec6 'title'\cf0 \strokec4 : title_node.text \cf5 \strokec5 if\cf0 \strokec4  title_node \cf5 \strokec5 is\cf0 \strokec4  \cf5 \strokec5 not\cf0 \strokec4  \cf5 \strokec5 None\cf0 \strokec4  \cf5 \strokec5 else\cf0 \strokec4  \cf6 \strokec6 'No Title'\cf0 \strokec4 ,\cb1 \
\cb3                 \cf6 \strokec6 'description'\cf0 \strokec4 : desc_node.text \cf5 \strokec5 if\cf0 \strokec4  desc_node \cf5 \strokec5 is\cf0 \strokec4  \cf5 \strokec5 not\cf0 \strokec4  \cf5 \strokec5 None\cf0 \strokec4  \cf5 \strokec5 else\cf0 \strokec4  \cf5 \strokec5 None\cf0 \cb1 \strokec4 \
\cb3             \})\cb1 \
\
\cb3         \cf5 \strokec5 if\cf0 \strokec4  programs_to_insert:\cb1 \
\cb3             \cf2 \strokec2 # For programs, we'll clear old data and insert fresh data.\cf0 \cb1 \strokec4 \
\cb3             \cf2 \strokec2 # This is simpler than upserting for a large, constantly changing dataset.\cf0 \cb1 \strokec4 \
\cb3             \cf2 \strokec2 # Let's clear programs that ended more than 1 hour ago.\cf0 \cb1 \strokec4 \
\cb3             \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 "Deleting old program data..."\cf0 \strokec4 )\cb1 \
\cb3             past_time = datetime.now(timezone.utc).isoformat()\cb1 \
\cb3             supabase.table(\cf6 \strokec6 'programs'\cf0 \strokec4 ).delete().lt(\cf6 \strokec6 'end_time'\cf0 \strokec4 , past_time).execute()\cb1 \
\
\cb3             \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"Inserting \cf0 \strokec4 \{len(programs_to_insert)\}\cf6 \strokec6  new programs..."\cf0 \strokec4 )\cb1 \
\cb3             \cf2 \strokec2 # Insert in batches to stay within API limits\cf0 \cb1 \strokec4 \
\cb3             batch_size = \cf7 \strokec7 500\cf0 \cb1 \strokec4 \
\cb3             \cf5 \strokec5 for\cf0 \strokec4  i \cf5 \strokec5 in\cf0 \strokec4  \cf5 \strokec5 range\cf0 \strokec4 (\cf7 \strokec7 0\cf0 \strokec4 , \cf5 \strokec5 len\cf0 \strokec4 (programs_to_insert), batch_size):\cb1 \
\cb3                 batch = programs_to_insert[i:i + batch_size]\cb1 \
\cb3                 supabase.table(\cf6 \strokec6 'programs'\cf0 \strokec4 ).insert(batch).execute()\cb1 \
\cb3                 \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"  - Inserted batch \cf0 \strokec4 \{i//batch_size + 1\}\cf6 \strokec6 "\cf0 \strokec4 )\cb1 \
\
\cb3         \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 "EPG update process completed successfully!"\cf0 \strokec4 )\cb1 \
\
\cb3     \cf5 \strokec5 except\cf0 \strokec4  requests.exceptions.RequestException \cf5 \strokec5 as\cf0 \strokec4  e:\cb1 \
\cb3         \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"Error fetching EPG data: \cf0 \strokec4 \{e\}\cf6 \strokec6 "\cf0 \strokec4 )\cb1 \
\cb3     \cf5 \strokec5 except\cf0 \strokec4  ET.ParseError \cf5 \strokec5 as\cf0 \strokec4  e:\cb1 \
\cb3         \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"Error parsing XML data: \cf0 \strokec4 \{e\}\cf6 \strokec6 "\cf0 \strokec4 )\cb1 \
\cb3     \cf5 \strokec5 except\cf0 \strokec4  Exception \cf5 \strokec5 as\cf0 \strokec4  e:\cb1 \
\cb3         \cf5 \strokec5 print\cf0 \strokec4 (\cf6 \strokec6 f"An unexpected error occurred: \cf0 \strokec4 \{e\}\cf6 \strokec6 "\cf0 \strokec4 )\cb1 \
\
\pard\pardeftab720\partightenfactor0
\cf5 \cb3 \strokec5 if\cf0 \strokec4  \cf5 \strokec5 __name__\cf0 \strokec4  == \cf6 \strokec6 "__main__"\cf0 \strokec4 :\cb1 \
\pard\pardeftab720\partightenfactor0
\cf0 \cb3     fetch_and_process_epg()\cb1 \
\
}