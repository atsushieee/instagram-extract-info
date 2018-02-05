import json
import urllib.request
from time import sleep

def get_json_info(url):
    html = urllib.request.urlopen(url)
    json_data = json.loads(html.read().decode())
    return json_data
