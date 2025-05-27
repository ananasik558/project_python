from flask import request, jsonify
import os
import pandas as pd
from etl.save_to_data_lake import save_to_data_lake

def configure_routes(app):
    @app.route("/upload/api", methods=["POST"])
    def upload_api():
        """Загрузка данных из внешнего API."""
        try:
            # Получаем URL и заголовки из запроса
            data = request.json
            url = data.get("url")
            headers = data.get("headers", {})

            if not url:
                return jsonify({"error": "URL is required"}), 400

            # Извлекаем данные
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                return jsonify({"error": "Failed to fetch data from API"}), 500

            # Сохраняем данные в Data Lake
            source_type = "api_data"
            save_to_data_lake(response.json(), source_type, source_type)
            return jsonify({"message": "Data uploaded successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/upload/file", methods=["POST"])
    def upload_file():
        """Загрузка данных из файла."""
        try:
            # Проверяем, был ли отправлен файл
            if "file" not in request.files:
                return jsonify({"error": "No file part in the request"}), 400

            file = request.files["file"]
            if file.filename == "":
                return jsonify({"error": "No selected file"}), 400

            # Определяем тип файла
            if file.filename.endswith(".csv"):
                data = pd.read_csv(file)
                source_type = "csv_data"
            elif file.filename.endswith(".json"):
                data = pd.read_json(file)
                source_type = "json_data"
            elif file.filename.endswith(".xml"):
                import xml.etree.ElementTree as ET
                tree = ET.parse(file)
                root = tree.getroot()
                data = [{child.tag: child.text for child in item} for item in root]
                data = pd.DataFrame(data)
                source_type = "xml_data"
            else:
                return jsonify({"error": "Unsupported file type"}), 400
            save_to_data_lake(data, source_type, source_type)
            return jsonify({"message": "File uploaded successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/status", methods=["GET"])
    def status():
        """Проверка статуса системы."""
        return jsonify({"status": "OK"}), 200