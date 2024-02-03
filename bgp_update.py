import csv
import requests
from datetime import datetime, timedelta
import sqlite3

MIN_NUM_SKEWERS = 50

# go back 24 hours each loop
# need number of updates == MIN_NUM_SKEWERS
# FIXME: be able to filter by 'pickiness' - strict time cutoff to converge on updates for each buddy candidate 
# TODO: segment between training and monitoring stages
# TODO: training should be more diverse
# TODO: training should be 24 hour recursive loop
# TODO: monitoring should be real time from all monitors, checking 3-6 minute intervals (can be higher)
# TODO: training should reach monitored prefix and buddy candidates, monitoring should reach monitored prefix and buddies
# TODO: flow -> get 50 (abritrary) updates for p -> loop through each timestamp (+- arbitrary interval close to convergence) for the update for each buddy candidate


def augment_utc(timestamp,interval, minutes):
    utc = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
    if minutes:
        decremented_utc= utc + timedelta(minutes=interval)
    else:
        decremented_utc= utc + timedelta(hours=interval)
    return decremented_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

def fetch_updates(resource, endtime, starttime, rrcs=None, unix_timestamps=False,hijack_name=None):
    base_url = "https://stat.ripe.net/data/bgp-updates/data.json"

    params = {
        'resource': resource,
        'endtime': endtime,
        'starttime': starttime,
        'rrcs': rrcs,
        'unix_timestamps': str(unix_timestamps).upper()
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
            return nr_updates, updates
            # for update in updates:
            #     # Parse update and gather relevant data
            #     update_type = update.get('type')

            #     timestamp = update.get('timestamp')
            #     utc_datetime = datetime.utcfromtimestamp(timestamp)
            #     utc_string = utc_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

            #     attrs = update.get('attrs', {})

            #     path = attrs.get('path')
            #     path_str = ', '.join(map(str, path)) if path is not None else None # cannot add dict to db

            #     target_prefix = attrs.get('target_prefix')
            #     source_id = attrs.get('source_id')

            #     print(f"Hijack: {hijack_name}")
            #     print(f"Update Type: {update_type}")
            #     print(f"Timestamp (UTC): {utc_string}")
            #     print(f"Target Prefix: {target_prefix}")
            #     print(f"Source ID: {source_id}")
            #     print(f"Path: {path}")
            #     print("------------")

            #     # insert data into table
            #     cursor.execute(f'''
            #                     INSERT INTO bgp_update(hijack_name,update_type, timestamp, target_prefix, 
            #                     source_id, path) VALUES (?, ?, ?, ?, ?, ?) ''', (hijack_name,update_type, utc_string, target_prefix,
            #                                                                   source_id, path_str))
            # sql_conn.commit()
        else:
            print(f"Error: {response.status_code}, {data.get('message')}")
    except requests.RequestException as e:
        print(f"Request error: {e}")

def read_monitors(filename):
    monitors = []
    with open(filename, mode='r',newline='',encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) # skip 'IP,ASN'
        for row in reader:
            monitors.append(row[0]) # read in ip only
    return monitors

# NOT COMPATIBLE WITH CURRENT VERSION OF fetch_updates
def process_monitors(filename,starttime,endtime):
    monitors = read_monitors(filename)
    name = str(filename.split('.')[0])

    for monitor in monitors:
        fetch_updates(resource=monitor, endtime=endtime, starttime=starttime,
                         rrcs=None, unix_timestamps=True, hijack_name=name)
        
def write_skewers(skewers):
    for timestamp in skewers:

        # create table in db
        sql_conn = sqlite3.connect('bgp_updates.db')
        cursor = sql_conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {timestamp} ( 
                monitor TEXT,
                buddy_candidate TEXT,
                path TEXT
            )
        ''')
        sql_conn.commit()
        #insert data into table
        for update in skewers[timestamp]:
            
            cursor.execute(f'''
                            INSERT INTO {timestamp}(monitor, 
                            buddy_candidate, path) VALUES (?, ?, ?, ?, ?, ?) ''', 
                            (monitor, buddy_candidate, path_str))
            sql_conn.commit()


'''
    Will collect data to train buddy candidates (skewers) and store skewers in a database
    each table = one monitored prefix update timestamp, contains updates from all monitors
    loops 24hr back recursively 
'''
def training(resource, endtime, interval, monitors_csv):
    ''' #TODO: make recursive ? pseudocode:
        skewers = []
        while len(skewers) < MIN_NUM_SKEWERS:
            updates = get_bgp_updates(monitored_prefix, endtime -24h, endtime) # TODO: need exactly min_num_skewers
            for timestamp in updates:
                starttime = timestamp - interval
                endttime = timestamp + interval
                for monitor in monitors:
                    buddy_cand_updates = get_bgp_updates(buddy_cand,starttime, endtime)
                    skewers[timestamp].append(buddy_cand_updates)
    '''
    skewers = []
    monitors = read_monitors(monitors_csv)

    # TODO: if no update for 30 days, discard monitor
    # each monitor needs min skewers for the monitored prefix
    # then for each update in each monitor, find what everyone heard in that interval
    # store what everyone heard
    while len(skewers) < MIN_NUM_SKEWERS:
        # Get new starttime (endtime - 24)
        starttime = augment_utc(endtime,-24, False)
        nr_updates, updates = fetch_updates(resource,endtime,starttime) #somewhere to use nr_updates?
        for update in updates:
            timestamp = update.get('timestamp')
            for monitor in monitors:
                everyone_heard = fetch_updates(monitor,augment_utc(timestamp, interval, True),augment_utc(timestamp,-interval, True))
                skewers[monitor].append(timestamp,everyone_heard)
                # each monitor needs MIN_NUM_SKEWER updates for the monitored prefix
                if (len(skewers) == MIN_NUM_SKEWERS):
                    continue
    print(skewers)
    #write_skewers(skewers)
 

def test():
    # init sql
    sql_conn = sqlite3.connect('bgp_updates.db')
    cursor = sql_conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS bgp_update (
            hijack_name TEXT,
            update_type TEXT,
            timestamp TEXT, 
            target_prefix TEXT,
            source_id TEXT,
            path TEXT
        )
    ''')
    sql_conn.commit()

    #monitors_csv = ["./monitors/cogent05.csv","./monitors/coned06.csv","./monitors/youtube08.csv","./monitors/canada12.csv"]

    # hijack name, start time (3 months before start time), end time
    monitor_map = [('youtube08','2007-11-26T18:00:00','2008-02-25T02:00:00'),
                    ('cogent05','2005-02-05T09:00:00','2005-05-09T17:00:00'),
                    ('coned06','2005-10-24T04:00:00','2006-01-25T12:00:00'),
                    ('canada12','2012-05-10T16:00:00','2012-08-09T00:00:00')]

    # hijack name, ip, start time (same as in the final report [p.3]), end time
    target_prefixes_map = [('youtube08','208.65.152.0/22','2008-02-24T18:00:00','2008-02-25T02:00:00'),
                    ('cogent05','64.233.161.0/24','2005-05-06T09:00:00','2005-05-09T17:00:00'),
                    ('coned06','12.173.227.0/24','2006-01-22T04:00:00','2006-01-25T12:00:00'),
                    ('canada12','"8.8.8.0/24','2012-08-08T16:00:00','2012-08-09T00:00:00')]

    for hij_name, starttime, endtime in monitor_map:
        filename = f"./monitors/{hij_name}.csv"
        process_monitors(filename,starttime,endtime)
    for hij_name, ip, starttime, endtime in target_prefixes_map:
        parse_bgp_update(resource=ip, endtime=endtime, starttime=starttime,
                        rrcs=None, unix_timestamps=True, hijack_name=hij_name)

# test on youtube
training('208.65.152.0/22','2008-02-24T18:00:00',6,"./monitors/youtube08.csv")

