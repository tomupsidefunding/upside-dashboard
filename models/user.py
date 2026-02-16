import os
import bcrypt
from flask_login import UserMixin


class User(UserMixin):
    """
    Single hardcoded user — credentials live in .env.
    No database table needed.
    """
    id   = '1'
    role = 'admin'

    def __init__(self):
        self.email = os.environ.get('DASHBOARD_EMAIL', 'admin@upside.com')
        self.name  = os.environ.get('DASHBOARD_NAME', 'Admin')

    def get_id(self):
        return '1'

    @property
    def is_manager(self):
        return True

    @property
    def is_trader(self):
        return False

    @staticmethod
    def get():
        """Return the single user instance."""
        return User()

    @staticmethod
    def check_password(email: str, password: str) -> bool:
        """Check supplied credentials against .env values."""
        correct_email    = os.environ.get('DASHBOARD_EMAIL', '').strip().lower()
        correct_password = os.environ.get('DASHBOARD_PASSWORD', '')

        if email.strip().lower() != correct_email:
            return False

        # Support both plain text and bcrypt hashed passwords
        if correct_password.startswith('$2b$') or correct_password.startswith('$2a$'):
            return bcrypt.checkpw(password.encode('utf-8'), correct_password.encode('utf-8'))
        else:
            return password == correct_password
