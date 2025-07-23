from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)
API_KEY = os.getenv("OPENROUTER_API_KEY") 

@app.route('/rewrite', methods=['POST'])
def rewrite():
    data = request.get_json()
    caption = data.get("caption", "")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "mistralai/mistral-7b-instruct",
        "messages": [
            {"role": "system", "content": "You rewrite social media captions to make them more engaging, clean, human, and short. Preserve hashtags."},
            {"role": "user", "content": caption}
        ]
    }

    try:
        res = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=30)
        output = res.json()["choices"][0]["message"]["content"]
        return jsonify({"rewritten": output})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def home():
    return "Caption Rewriter API is live!"

