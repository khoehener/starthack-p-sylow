# debug_claude.py
import os
import anthropic

api_key = os.getenv("ANTHROPIC_API_KEY")
print(f"Key found: {api_key is not None}")
print(f"Key starts with: {api_key[:20] if api_key else 'N/A'}")

try:
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=100,
        temperature=0,
        messages=[{"role": "user", "content": "Reply with just the word: WORKING"}],
    )
    print(f"API response: {message.content[0].text}")
    print("Claude is working correctly")
except Exception as e:
    print(f"API error: {type(e).__name__}: {e}")