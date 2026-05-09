import os
from openai import OpenAI
api_key=os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

try:
    response = client.chat.completions.create(
        model="gpt-4o-mini",  # use any model you have access to
        messages=[
            {"role": "user", "content": "How is the weather today in san fransisco"}
        ],
        max_tokens=20,
    )
    print("✅ Success!")
    print(response.choices[0].message.content)

except Exception as e:
    print("❌ API call failed")
    print(e)
