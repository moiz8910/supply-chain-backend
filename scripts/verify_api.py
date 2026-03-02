import requests
import json

try:
    response = requests.get("http://localhost:8000/api/kpis")
    if response.status_code == 200:
        print("API Verified!")
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"Failed: {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"Error connecting: {e}")
