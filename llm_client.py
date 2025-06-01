import os
import requests
import signature_cache

def call_llm_api(api_key_from_session, messages, prompt = """You are a helpful AI assistant that helps allocate additional extra tps capacity to a customer.
            Your job is to ask the customer questions like:
            - How many TPS do you need? (Example: 10)
            - What countries are you targeting? (Example: US)
            - What's your peak time window? (0 to 23 hour format) (optional, default is 0-23)
            - What's your peak TPS? (Peak time TPS)
            - What's your traffic volume per week? (optional)
            
            Once you’ve collected enough information, respond with the keyword WE_ARE_READY_TO_ALLOCATE in 1st line.
            In 2nd line have this json like,
            {
                "requested_tps": 50,
                "destinations": ["CA", "US],
                "traffic_volume": 100000,
                "peak_window": "10-12",
                "peak_tps": 20
            }
            
            You will return WE_ARE_READY_TO_ALLOCATE atmost once and only when allocating the request.
            After allocating, if user sends any other messages, give messages like thank you etc.
            Don't ask all questions at once, ask one by one. Take care of customer experience.
            Always end every message with ###.
        """):
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
        "systemPrompt": prompt,
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


def generate_formatted_summary(api_key, raw_summary):
    # System prompt specifically for formatting
    formatting_prompt = """
    You are a customer-friendly assistant.
    Your job is to take a raw TPS allocation summary and return a clean, formatted version using bullet points, tables, only.
    Emphasize clarity and professionalism.
    We directly pass your response to customer, so don't include texts like, Here's a clean, formatted version of the TPS allocation summary etc. Directly start with the Summary.
    Always end the message with ###
    """

    # Messages to send to the LLM (no need for system role inside the message array)
    pretty_messages = [
        {"role": "user", "content": [{"text": raw_summary}]}
    ]

    # Use the enhanced call_llm_api with custom prompt
    pretty_response = call_llm_api(api_key, pretty_messages, prompt=formatting_prompt)

    # Clean up LLM's ending syntax
    return pretty_response.strip().rstrip('#').strip()
