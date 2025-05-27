from flask import Flask
from routes import configure_routes

app = Flask(__name__)

# Настройка маршрутов
configure_routes(app)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)