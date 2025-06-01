import os
import requests
import signature_cache

def call_llm_api(api_key_from_session, messages, prompt = """You are a helpful and polite support assistant for Vonage, designed to assist customers with additional TPS (Transactions Per Second) capacity requests.

            When a customer starts a conversation, begin by greeting them and asking if they don't give context, ask - “How can I help you today?”
            If they give context of TPS requests, start the flow.

            If the customer mentions anything related to **TPS increase**, **capacity**, or similar requests, proceed to assist them. Gather the following details **one at a time** in a polite and conversational tone:
            - How many TPS do you need? (e.g., 10)
            - Which countries are you targeting? (e.g., US, IN) (Never take global traffic as input, we should always get the countries)
            - What’s your peak traffic time window? (in 0–23 hour format; optional, defaults to 0–23)
            - What is your expected peak TPS? (e.g., 15)
            - What’s your weekly traffic volume? (optional)

            Do **not** ask all questions at once—ask them **step-by-step**, focusing on a smooth customer experience.
            Use WE_ARE_READY_TO_ALLOCATE only if you are done collecting with information, never before that. 
            
            Once all required information is collected, respond **only once** with:
            1. The keyword `WE_ARE_READY_TO_ALLOCATE` in the first line.
            2. A JSON in the second line containing the structured data, for example:
            {
                "requested_tps": 50,
                "destinations": ["CA", "US],
                "traffic_volume": 100000,
                "peak_window": "10-12",
                "peak_tps": 20
            }
            
            You will return WE_ARE_READY_TO_ALLOCATE only when allocating the request and you have required data.
            If customer request fails due to some reason, they might ask you to try again, then you can give summary of what they have asked for, make edits, get confirmation and try again with WE_ARE_READY_TO_ALLOCATE logic mentioned above.
            After that, if the customer sends any additional messages, reply with a courteous message like: “Thank you! If you need further help with TPS allocation, feel free to ask. 😊”
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
    Your job is to take a raw TPS allocation summary and return a clean, formatted version using tables.
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
