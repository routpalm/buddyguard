import requests
import csv

def fetch_monitors(name, timestamp):
    url = "https://stat.ripe.net/data/ris-peers/data.json"
    params = {'timestamp': timestamp}

    # request data from ripestat
    response = requests.get(url, params=params)
    data = response.json()

    # need monitors directory in directory of exec
    filename = f"./monitors/{name}.csv"

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['ASN', 'IP']) # format

        for rrc, peers in data['data']['peers'].items():
            for peer in peers:
                writer.writerow([peer['asn'], peer['ip']])

# data to proc
hijack_map = {
    "youtube08": "2008-02-24T18:00:00",
    "cogent05": "2005-05-06T09:00:00",
    "canada12": "2012-08-08T16:00:00",
    "coned06": "2006-01-22T04:00:00"
}

# loop over timestamps
for hijack, timestamp in hijack_map.items():
    fetch_monitors(hijack, timestamp)
