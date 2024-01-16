import requests
from datetime import datetime
import sqlite3
def parse_bgp_update(resource, endtime, starttime=None, rrcs=None, unix_timestamps=False,table_name=None):
    base_url = "https://stat.ripe.net/data/bgp-updates/data.json"

    params = {
        'resource': resource,
        'endtime': endtime,
        'starttime': starttime,
        'rrcs': rrcs,
        'unix_timestamps': str(unix_timestamps).upper(),
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # If req success
        if response.status_code == 200 and 'data' in data:
            updates_data = data['data']

            nr_updates = updates_data.get('nr_updates')

            # unused (question mark?)
            query_starttime = updates_data.get('query_starttime')
            query_endtime = updates_data.get('query_endtime')
            resource_used = updates_data.get('resource')

            print(f"Total # updates: {nr_updates}")

            updates = updates_data.get('updates', [])
            for update in updates:
                # Parse update and gather relevant data
                update_type = update.get('type')

                timestamp = update.get('timestamp')
                utc_datetime = datetime.utcfromtimestamp(timestamp)
                utc_string = utc_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

                attrs = update.get('attrs', {})

                target_prefix = attrs.get('target_prefix')
                source_id = attrs.get('source_id')

                print(f"Update Type: {update_type}")
                print(f"Timestamp (UTC): {utc_string}")
                print(f"Target Prefix: {target_prefix}")
                print(f"Source ID: {source_id}")
                print("------------")

                # insert data into table
                cursor.execute(f'''
                                INSERT INTO {table_name} (update_type, timestamp, target_prefix, source_id)
                                VALUES (?, ?, ?, ?)
                            ''', (update_type, utc_string, target_prefix, source_id))
            sql_conn.commit()
        else:
            print(f"Error: {response.status_code}, {data.get('message')}")
    except requests.RequestException as e:
        print(f"Request error: {e}")


# generate table based on current time
table_name = f"bgp_updates_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# init sql
sql_conn = sqlite3.connect('bgp_updates.db')
cursor = sql_conn.cursor()
cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        update_type TEXT,
        timestamp TEXT,
        target_prefix TEXT,
        source_id TEXT
    )
''')
sql_conn.commit()

# input params (placeholder for now)
resource = "140.78.0.0/16"
starttime = "2012-12-21T04:00:00"
endtime = "2012-12-21T12:00:00"

parse_bgp_update(resource=resource, endtime=endtime, starttime=starttime,
                 rrcs=None, unix_timestamps=True,table_name=table_name)
