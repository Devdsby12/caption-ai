from flask import Flask, request, jsonify
from transformers import pipeline

app = Flask(__name__)
rewriter = pipeline("summarization", model="facebook/bart-large-cnn")

@app.route('/rewrite', methods=['POST'])
def rewrite():
    data = request.get_json()
    caption = data.get("caption", "")
    result = rewriter(caption, max_length=30, min_length=10, do_sample=False)
    return jsonify({"rewritten": result[0]['summary_text']})
