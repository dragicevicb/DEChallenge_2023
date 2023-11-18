from data_cleaning import data_processing
from db import db_connection
from flask import Flask, request, jsonify
from datetime import datetime
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


@app.route('/queryUserData', methods=['GET'])
def query_user_data():
    try:
        user_id = request.args.get('user_id', type=str)
        date_str = request.args.get('date', default=None, type=str)

        if not user_id:
            return jsonify({"error": "user_id is required"}), 400

        date = None
        if date_str:
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400

        user_stats = db_connection.query_user_data(user_id, date)

        return jsonify(user_stats)
    except Exception as ex:
        return jsonify(ex), 500


@app.route('/queryGameData', methods=['GET'])
def query_game_data():
    try:
        date_str = request.args.get('date', default=None, type=str)
        country = request.args.get('country', default=None, type=str)

        date = None
        if date_str:
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400

        game_stats = db_connection.query_game_data(date, country)

        return jsonify(game_stats)
    except Exception as ex:
        return jsonify(error=str(ex)), 500


if __name__ == "__main__":
    records_for_load = data_processing.prepare_for_load()
    db_connection.load_data(records_for_load)
    app.run(host='0.0.0.0', port=8086)
