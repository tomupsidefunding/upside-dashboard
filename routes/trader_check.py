from flask import Blueprint, render_template
from flask_login import login_required, current_user
from flask import abort

trader_check_bp = Blueprint('trader_check', __name__)


@trader_check_bp.route('/trader-check')
@login_required
def index():
    if not current_user.is_manager:
        abort(403)
    return render_template('trader_check/index.html')
