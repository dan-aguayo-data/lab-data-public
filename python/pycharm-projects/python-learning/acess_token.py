import requests

url = 'http://127.0.0.1:8088/api/v1/security/guest_token/'
data = {
  'resources': [{'type': 'dashboard', 'id': '1'}],
  'user': {'username': 'admin'}
}
headers = {'Authorization': 'Bearer hDQH-GEYi11!'}

response = requests.post(url, json=data, headers=headers)
print(response.json())