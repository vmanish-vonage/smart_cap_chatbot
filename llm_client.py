import os
import requests
import signature_cache

def call_llm_api(api_key_from_session, messages):
    api_url = os.getenv("LLM_API_URL")
    api_key_name = os.getenv("API_KEY_NAME", "dev") # Optional override

    if not api_url:
        return "LLM_API_URL not set"

    if not (signature_cache.signature and signature_cache.date):
        return "Signature not initialized yet."

    headers = {
        "Authorization": f"HmacSHA512 {api_key_name}:XXXX:{signature_cache.signature}",
        "X-VON-DATE": signature_cache.date,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # Build the payload
    payload = {
        "model": {
            "modelId": "anthropic.claude-3-5-sonnet-20240620-v1:0",
            "modelRegion": "us-west-2"
        },
        "systemPrompt": "You are a helpful AI assistant. End the response with ###.",
        "messages": messages,
        "responseFormat": "text",
        "inferenceConfig": {
            "stopSequences": ["###"],
            "maxTokens": 4096,
            "temperature": 0.7,
            "topP": 0.9
        },
        "additionalModelRequestFields": {
            "top_k": 400
        }
    }

    try:
        print("Request Sent: {}", payload)
        response = requests.post(api_url, json=payload, headers=headers, timeout=(5, 100))
        response.raise_for_status()
        return response.json().get("response", "LLM response missing.")
    except requests.exceptions.RequestException as e:
        return f"Error calling LLM API: {str(e)}"

