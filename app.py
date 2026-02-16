from flask import Flask
from flask_login import LoginManager
from dotenv import load_dotenv
import os

from models.user import User
from routes.auth import auth_bp
from routes.dashboard import dashboard_bp

load_dotenv()

login_manager = LoginManager()

def create_app():
    app = Flask(__name__)

    # ── Config ────────────────────────────────────────────────────────────────
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'change-me-in-prod')

    # ── Login manager ─────────────────────────────────────────────────────────
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message_category = 'info'

    @login_manager.user_loader
    def load_user(user_id):
        # Single hardcoded user — ID is always '1'
        if user_id == '1':
            return User.get()
        return None

    # ── Blueprints ────────────────────────────────────────────────────────────
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
