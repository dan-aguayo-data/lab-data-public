import requests


def trigger_logic_app(url):
    try:
        # Send POST request
        response = requests.post(url)
        response.raise_for_status()  # Raise error for bad status codes

        print(f"Logic App triggered successfully. Status Code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to trigger Logic App: {e}")


if __name__ == "__main__":
    # You can pass different URLs here
    url_to_trigger = "https://xxxxx.azurewebsites.net/api/http_invocation?"

    trigger_logic_app(url_to_trigger)
