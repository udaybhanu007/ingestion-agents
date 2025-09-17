import os,sys,requests,json
from dotenv import load_dotenv
load_dotenv('.env.dev')
endpoint=os.getenv('AZURE_OPENAI_ENDPOINT')
api_key=os.getenv('AZURE_OPENAI_API_KEY')
deployment=os.getenv('AZURE_OPENAI_DEPLOYMENT')
apiversion=os.getenv('AZURE_OPENAI_API_VERSION','2024-12-01-preview')
print('Endpoint:',endpoint)
print('Deployment:',deployment)
if not endpoint or not api_key or not deployment:
    print('Missing config; aborting')
    sys.exit(2)

url = endpoint.rstrip('/') + '/openai/chat/completions?api-version=' + apiversion
headers = {'api-key': api_key, 'Content-Type':'application/json'}
payload = {
  'model': deployment,
  'messages': [{'role':'user','content':'Say hi'}],
  'max_tokens': 5
}
print('Posting to:', url)
try:
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    print('Status:', r.status_code)
    try:
        print('Body:', json.dumps(r.json(), indent=2))
    except Exception:
        print('Body raw:', r.text)
except Exception as e:
    print('Request failed:', e)
