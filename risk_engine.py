from flask import Blueprint, render_template
from flask_login import login_required, current_user
from flask import abort

risk_engine_bp = Blueprint('risk_engine', __name__)


@risk_engine_bp.route('/risk-engine')
@login_required
def index():
    if not current_user.is_manager:
        abort(403)
    return render_template('risk_engine/index.html')
