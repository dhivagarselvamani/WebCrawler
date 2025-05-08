import requests
import json

url = "https://nse-stock-market-india.p.rapidapi.com/symbols"
headers = {
    "x-rapidapi-host": "nse-stock-market-india.p.rapidapi.com",
    "x-rapidapi-key": "08a4abe5c8msh19d496447b1032ap13e31ajsn284601dcd0d7"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))
else:
    print(f"Request failed with status code: {response.status_code}")
