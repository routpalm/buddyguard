import requests
import csv
import sys

def fetch_monitors(name, timestamp):
    url = "https://stat.ripe.net/data/ris-peers/data.json"
    params = {'query_time': timestamp}

    # Request data from ripestat
    response = requests.get(url, params=params)
    data = response.json()

    # Ensure the monitors directory exists in the directory of execution
    filename = f"./monitors/{name}.csv"

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # ipv6 true or false
        writer.writerow(['ASN', 'IP', 'IPv6'])

        for rrc, peers in data['data']['peers'].items():
            for peer in peers:
                if peer['asn'] != "0" and peer['ip'] != "0.0.0.0":
                    # determine if ipv6
                    is_ipv6 = ':' in peer['ip']
                    writer.writerow([peer['asn'], peer['ip'], is_ipv6])
'''
# Data to process
hijack_map = {
    "youtube08": "2008-02-24T18:00:00",
    "cogent05": "2005-05-06T09:00:00",
    "canada12": "2012-08-08T16:00:00",
    "coned06": "2006-01-22T04:00:00"
}

# Loop over timestamps
for hijack, timestamp in hijack_map.items():
    print(f"FETCHING: {hijack}")
    fetch_monitors(hijack, timestamp)
'''

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python monitors.py <name> <unix_timestamp>")
        sys.exit(1)

    name = sys.argv[1]
    timestamp = sys.argv[2]
    
    print(f"FETCHING: {name}")
    fetch_monitors(name, timestamp)
