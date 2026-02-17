from flask import Blueprint, render_template, redirect, url_for, abort, jsonify
from flask_login import login_required, current_user
from models.db_connector import (
    get_trader_roster, get_summary_stats,
    get_trader_roster_row, get_daily_equity_series, get_deals,
    PHASE_ORDER,
)

dashboard_bp = Blueprint('dashboard', __name__)


@dashboard_bp.route('/')
@login_required
def index():
    """Manager overview — roster of all traders with summary cards."""
    if not current_user.is_manager:
        return redirect(url_for('dashboard.trader_detail', login=current_user.trader_id))

    roster  = get_trader_roster()
    summary = get_summary_stats(roster)

    # Group roster by phase for the template
    by_phase = {phase: [] for phase in PHASE_ORDER}
    for row in roster:
        by_phase.setdefault(row['phase'], []).append(row)

    return render_template('dashboard/index.html',
                           summary=summary,
                           by_phase=by_phase,
                           phase_order=PHASE_ORDER)


@dashboard_bp.route('/trader/<int:login>')
@login_required
def trader_detail(login):
    """Drill-down view for a single trader login."""
    if current_user.is_trader and current_user.trader_id != login:
        abort(403)

    trader = get_trader_roster_row(login)
    if not trader:
        abort(404)

    deals = get_deals(login)

    return render_template('dashboard/trader.html',
                           trader=trader,
                           deals=deals)


@dashboard_bp.route('/api/trader/<int:login>/equity')
@login_required
def trader_equity_api(login):
    """JSON endpoint — equity curve data for Chart.js."""
    if current_user.is_trader and current_user.trader_id != login:
        abort(403)

    rows = get_daily_equity_series(login)
    return jsonify({
        'labels':        [str(r['date']) for r in rows],
        'equity':        [float(r['equity'])        if r['equity']        is not None else None for r in rows],
        'daily_balance': [float(r['daily_balance'])  if r['daily_balance'] is not None else None for r in rows],
    })
