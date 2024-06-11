import requests
import json
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Access the API key
api_key = os.getenv("OPENAI_API_KEY")

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

data = {
    "input": "Your text string goes here",
    "model": "text-embedding-3-small"
}

response = requests.post('https://api.openai.com/v1/embeddings', headers=headers, data=json.dumps(data))

if response.status_code == 200:
    embedding = response.json()
    print(embedding)
else:
    print(f"Error: {response.status_code}")
    print(response.text)
