"""
This module imports the dashboard to a running Superset Server.
It was taken from https://github.com/apache/superset/issues/20288
The superset cli dashboard import does not work with passwords.
"""
import os
import requests

# Get/Set env vars and dashboard path
DASHBOARD_ZIP_FILE = './dashboard.zip'
username = os.getenv("ADMIN_USERNAME")
password = os.getenv("ADMIN_PASSWORD")
BASE_URL = 'http://superset:8088'

# Avoid a docker health check and wait until the superset server is available:
while True:
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        break
    except Exception:
        pass

# Set endpoint for login
login_url = f"{BASE_URL}/login/"
session = requests.Session()

payload = {}
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9'
}

# Get a token from the login endpoint
response = session.request("GET", login_url, headers=headers, data=payload)
csrf_token = response.text.split(
    '<input id="csrf_token" name="csrf_token" type="hidden" value="')[1].split('">')[0]

# Perform login
payload = f'csrf_token={csrf_token}&username={username}&password={password}'
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Content-Type': 'application/x-www-form-urlencoded'}

session.request("POST",
                login_url,
                headers=headers,
                data=payload,
                allow_redirects=False)

cookie = session.cookies.get_dict().get('session')

# Import dashboards
import_dashboard_url = f"{BASE_URL}/api/v1/dashboard/import/"

# Build post payload to import dashboard from zip file
with open(DASHBOARD_ZIP_FILE, 'rb') as f:
    payload = {
        'passwords': '{"databases/PostgreSQL.yaml":"postgres"}',
        'overwrite': 'true'
    }
    files = [
        ('formData', ('dashboards.zip', f, 'application/zip'))
    ]
    headers = {
        'Accept': 'application/json',
        'Cookie': f'session={cookie}',
        'X-CSRFToken': csrf_token
    }

    response = requests.request("POST",
                                import_dashboard_url,
                                headers=headers,
                                data=payload,
                                files=files)
