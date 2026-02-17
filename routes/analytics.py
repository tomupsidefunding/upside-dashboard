from flask import Blueprint, render_template, request, redirect, url_for, abort
from flask_login import login_required, current_user
from models.db_connector import get_trader_analytics, format_hold_time

analytics_bp = Blueprint('analytics', __name__)


@analytics_bp.route('/analytics', methods=['GET', 'POST'])
@login_required
def analytics_search():
    """Search page for trader analytics"""
    if not current_user.is_manager:
        abort(403)
    
    if request.method == 'POST':
        identifier = request.form.get('identifier', '').strip()
        if identifier:
            return redirect(url_for('analytics.trader_analytics', identifier=identifier))
    
    return render_template('analytics/search.html')


@analytics_bp.route('/analytics/<identifier>')
@login_required
def trader_analytics(identifier):
    """Display comprehensive analytics for a trader"""
    if not current_user.is_manager:
        abort(403)
    
    # Get analytics data
    analytics = get_trader_analytics(identifier)
    
    if not analytics:
        return render_template('analytics/not_found.html', identifier=identifier)
    
    # Format hold times for display
    analytics['avg_hold_formatted'] = format_hold_time(analytics.get('avg_hold_minutes', 0))
    analytics['avg_winner_hold_formatted'] = format_hold_time(analytics.get('avg_winner_hold_minutes', 0))
    analytics['avg_loser_hold_formatted'] = format_hold_time(analytics.get('avg_loser_hold_minutes', 0))
    
    # Generate insights
    insights = _generate_insights(analytics)
    
    return render_template('analytics/trader_analytics.html', 
                          analytics=analytics,
                          insights=insights)


def _generate_insights(analytics):
    """Generate trading insights from analytics"""
    insights = []
    
    trade_expectancy = analytics.get('trade_expectancy', 0)
    win_rate = analytics.get('win_rate', 0)
    total_pnl = analytics.get('total_pnl', 0)
    
    # Expectancy insights
    if trade_expectancy > 50:
        insights.append("Excellent trade expectancy - strategy is highly scalable")
    elif trade_expectancy > 20:
        insights.append("Good trade expectancy - consider increasing position sizes")
    elif trade_expectancy > 0:
        insights.append("Positive expectancy - focus on consistency")
    else:
        insights.append("Negative expectancy - strategy needs fundamental changes")
    
    # Performance insights
    if total_pnl > 0 and win_rate > 55:
        insights.append("Strong overall performance with good profitability")
    elif total_pnl > 0:
        insights.append("Profitable but consider improving win rate")
    else:
        insights.append("Performance needs improvement - focus on risk management")
    
    # Hold time insights
    avg_winner_hold = analytics.get('avg_winner_hold_minutes', 0)
    avg_loser_hold = analytics.get('avg_loser_hold_minutes', 0)
    
    if avg_winner_hold > avg_loser_hold * 1.5:
        insights.append("Good discipline - winners held longer than losers")
    elif avg_loser_hold > avg_winner_hold * 1.5:
        insights.append("Consider cutting losses faster")
    
    # Symbol insights
    symbol_breakdown = analytics.get('symbol_breakdown', [])
    if symbol_breakdown:
        best_symbol = symbol_breakdown[0]
        insights.append(f"Best symbol: {best_symbol['symbol']} (${best_symbol['expectancy']:.2f} expectancy)")
        
        negative_symbols = [s for s in symbol_breakdown if s['expectancy'] < 0]
        if negative_symbols:
            insights.append(f"Warning: {len(negative_symbols)} symbols have negative expectancy - consider eliminating")
    
    return insights
