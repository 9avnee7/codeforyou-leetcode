import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from leetcode_scraper import LeetcodeScraper  # Import your scraper class

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

scraper = LeetcodeScraper()

@app.route('/scrape/leetcode', methods=['GET'])
def get_leetcode_data():
    username = request.args.get('username')
    if not username:
        return jsonify({"error": "Username is required"}), 400

    data = scraper.scrape_user_profile(username) 
    return jsonify(data)

if __name__ == '__main__':
    port = int(os.getenv("PORT", 4000)) 
    debug = os.getenv("DEBUG", "True").lower() == "true"
    app.run(host='0.0.0.0', port=port, debug=debug)