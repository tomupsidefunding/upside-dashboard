"""
db_connector.py
---------------
All database queries for the Upside Funding dashboard.
Uses the exact schema and query logic from the production reporting script.

Databases:
  - `TheUpsideFunding-MT5`  (MT5 trading data)
  - `bl_fund_production`    (challenge/trader management)
"""

import os
import re
import json
import pymysql
import pymysql.cursors
from contextlib import contextmanager


# ── Connection helpers ─────────────────────────────────────────────────────────

def _get_conn(database: str):
    return pymysql.connect(
        host=os.environ.get('DB_HOST', '188.245.226.143'),
        port=int(os.environ.get('DB_PORT', 3306)),
        user=os.environ.get('DB_USER', 'client_fund'),
        password=os.environ['DB_PASSWORD'],
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=15,
    )


@contextmanager
def mt5_conn():
    conn = _get_conn('TheUpsideFunding-MT5')
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def prod_conn():
    conn = _get_conn('bl_fund_production')
    try:
        yield conn
    finally:
        conn.close()


# ── Payout percentage query ────────────────────────────────────────────────────

def get_payout_percentages():
    """
    Query payout percentages for all traders.
    Returns dict mapping login -> payout_pct (0.85 or 0.95)
    Default to 0.85 if not found or on error.
    """
    try:
        with prod_conn() as conn:
            cursor = conn.cursor()
            
            # Query to get payout percentages for all accounts
            payout_query = """
            SELECT
                ca.number,
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'title', ups.title,
                        'value', ups_items.val
                    )
                ) AS upsale_values
            FROM bl_fund_production.challenge_accounts ca
            LEFT JOIN bl_fund_production.challenge_types ct
                ON ca.challenge_type_id = ct.id
            LEFT JOIN bl_fund_production.account_types at
                ON ct.account_type_id = at.id
            LEFT JOIN bl_fund_production.task_challenge_demo_account_payloads AS demo_payload
                ON ca.id = demo_payload.challenge_account_id
                AND at.type = 'demo'
            LEFT JOIN bl_fund_production.task_challenge_live_account_payloads AS live_payload
                ON ca.id = live_payload.challenge_account_id
                AND at.type = 'live'
            LEFT JOIN bl_fund_production.challenge_payments cp
                ON COALESCE(demo_payload.challenge_payment_id, live_payload.challenge_payment_id) = cp.id
            LEFT JOIN bl_fund_production.challenge_types_upsale ups
                ON JSON_CONTAINS(cp.conditions, JSON_QUOTE(ups.id), '$')
            LEFT JOIN JSON_TABLE(
                ups.values,
                '$[*]' COLUMNS (
                    challengeTypeId CHAR(36) PATH '$.challengeTypeId',
                    val BOOLEAN PATH '$.value'
                )
            ) AS ups_items
                ON ups_items.challengeTypeId = ct.id 
            WHERE cp.type = 'deposit'
            GROUP BY ca.number
            """
            
            cursor.execute(payout_query)
            payout_rows = cursor.fetchall()
            
            # Create mapping: login -> payout percentage
            payout_map = {}
            for row in payout_rows:
                login = row['number']
                upsale_json = row['upsale_values']
                
                # Default to 85% (0.85)
                payout_pct = 0.85
                
                # Parse the JSON to check for 95% upgrade
                if upsale_json:
                    try:
                        upsale_data = json.loads(upsale_json)
                        # Check if any upsale has a non-null value
                        for item in upsale_data:
                            if item.get('title') and item.get('value') is not None:
                                # If we have a valid upsale (95% payout), set to 0.95
                                payout_pct = 0.95
                                break
                    except:
                        pass
                
                payout_map[login] = payout_pct
            
            return payout_map
            
    except Exception as e:
        print(f"Warning: Could not retrieve payout percentages: {e}")
        print("Defaulting all accounts to 85% payout")
        return {}


# ── Main roster query ──────────────────────────────────────────────────────────

# Matches the production reporting script exactly
ROSTER_QUERY = """
SELECT * FROM (
    SELECT e.login, d.title, d.category,
           CAST(f_earliest.daily_balance AS DECIMAL(15,2)) as Account_Size,
           a.Equity as Margin_Equity,
           f_latest.equity as Daily_Equity,
           e.balance as Challenges_Balance,
    CASE
        WHEN d.title LIKE '%1-Step%'   THEN REGEXP_SUBSTR(d.title, '1-Step')
        WHEN d.title LIKE '%Phase%'    THEN REGEXP_SUBSTR(d.title, 'Phase [0-9]+')
        WHEN d.title LIKE '%Funded%'   THEN REGEXP_SUBSTR(d.title, 'Funded')
        WHEN d.title LIKE '%2Step_No%' THEN 'Phase 1'
        ELSE 'UNKNOWN'
    END as Phase,
    FROM_UNIXTIME(f_latest.date_time) AS date_time,
    b.trader_id, c.email, c.first_name, c.last_name, b.status,
    ROW_NUMBER() OVER (PARTITION BY e.login ORDER BY e.ID DESC) as rn
    FROM `TheUpsideFunding-MT5`.CHALLENGES e
    LEFT JOIN `TheUpsideFunding-MT5`.mt5_accounts_margin a ON a.login = e.login
    LEFT JOIN (
        SELECT login, date_time, equity,
               ROW_NUMBER() OVER (PARTITION BY login ORDER BY date_time DESC) as rn
        FROM `TheUpsideFunding-MT5`.daily
        WHERE date_time IS NOT NULL
    ) f_latest ON e.login = f_latest.login AND f_latest.rn = 1
    LEFT JOIN (
        SELECT login, daily_balance,
               ROW_NUMBER() OVER (PARTITION BY login ORDER BY date_time ASC) as rn
        FROM `TheUpsideFunding-MT5`.daily
        WHERE date_time IS NOT NULL AND daily_balance IS NOT NULL
    ) f_earliest ON e.login = f_earliest.login AND f_earliest.rn = 1
    LEFT JOIN bl_fund_production.challenge_accounts b ON e.login = b.number
    LEFT JOIN bl_fund_production.traders c ON b.trader_id = c.id
    LEFT JOIN bl_fund_production.challenge_types d ON b.challenge_type_id = d.id
    WHERE e.status IN ('PLAYING', 'REVIEW')
) as ranked_accounts
WHERE rn = 1
"""

HOUSE_ACCOUNT_EMAIL = 'analytics@theupsidefunding.com'
PHASE_ORDER = ['House Account', 'Funded', 'Phase 2', '1-Step', 'Phase 1', 'UNKNOWN']


def _extract_category_amount(category_str) -> float | None:
    """Extract numeric value from category string e.g. '$ 100,000' -> 100000.0"""
    if not category_str:
        return None
    cleaned = str(category_str).replace('$', '').replace(',', '').strip()
    match = re.search(r'\d+', cleaned)
    return float(match.group()) if match else None


def _to_float(val) -> float | None:
    try:
        f = float(val)
        return None if f == 0 else f
    except (TypeError, ValueError):
        return None


def _process_rows(rows: list, payout_map: dict) -> list:
    """
    Apply the same post-processing logic as the production script:
    - Starting balance fallback chain
    - Equity fallback chain
    - Gain/loss %
    - Pot. liability (with dynamic payout %)
    - House account labelling
    - Exclude TEST NEW accounts
    """
    processed = []

    for row in rows:
        # Skip TEST NEW accounts
        title = str(row.get('title') or '')
        if title.startswith('TEST NEW'):
            continue

        # House account override
        phase = row['Phase'] if row.get('email') != HOUSE_ACCOUNT_EMAIL else 'House Account'

        # Starting balance: earliest daily_balance → category amount
        account_size = _to_float(row.get('Account_Size'))
        category_amount = _extract_category_amount(row.get('category'))
        starting_balance = account_size or category_amount

        # Current equity: Margin → Daily → Challenges_Balance
        current_equity = (
            _to_float(row.get('Margin_Equity'))
            or _to_float(row.get('Daily_Equity'))
            or _to_float(row.get('Challenges_Balance'))
        )

        # Gain/loss %
        change = (current_equity - starting_balance) if (current_equity and starting_balance) else 0
        gain_loss_pct = round((change / starting_balance * 100), 3) if starting_balance else 0

        # Get payout percentage for this login (default 0.85 if not found)
        login = row.get('login')
        payout_pct = payout_map.get(login, 0.85)
        
        # Pot. liability - ONLY for Funded with positive gains, using dynamic payout %
        pot_liability = change * payout_pct if (phase == 'Funded' and change > 0) else 0

        processed.append({
            'login':            login,
            'category':         row.get('category'),
            'phase':            phase,
            'first_name':       row.get('first_name'),
            'last_name':        row.get('last_name'),
            'email':            row.get('email'),
            'starting_balance': starting_balance or 0,
            'current_equity':   current_equity or 0,
            'change':           change,
            'gain_loss_pct':    gain_loss_pct,
            'pot_liability':    pot_liability,
            'payout_pct':       payout_pct * 100,  # Convert to percentage for display
            'status':           row.get('status'),
        })

    return processed


def get_roster():
    """
    Main roster query — returns all active traders with stats.
    Also fetches dynamic payout percentages.
    """
    with prod_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(ROSTER_QUERY)
        rows = cursor.fetchall()

    # Get payout percentages
    payout_map = get_payout_percentages()
    
    # Process with dynamic payout map
    processed = _process_rows(rows, payout_map)

    # Sort by phase, then by gain_loss_pct descending
    phase_idx_map = {p: i for i, p in enumerate(PHASE_ORDER)}
    processed.sort(
        key=lambda x: (phase_idx_map.get(x['phase'], 999), -x['gain_loss_pct'])
    )

    return processed


def get_trader_roster_row(login: int):
    """Get a single trader row by login"""
    roster = get_roster()
    for row in roster:
        if row['login'] == login:
            return row
    return None


def get_roster_summary(roster: list):
    """Calculate summary statistics from roster"""
    total = len(roster)
    funded = sum(1 for r in roster if r['phase'] == 'Funded')
    phase2 = sum(1 for r in roster if r['phase'] == 'Phase 2')
    one_step = sum(1 for r in roster if r['phase'] == '1-Step')
    phase1 = sum(1 for r in roster if r['phase'] == 'Phase 1')
    
    total_liability = sum(r['pot_liability'] for r in roster if r['phase'] == 'Funded')
    total_equity = sum(r['current_equity'] for r in roster)
    
    # Weighted average performance
    weighted_avg_gain_loss = (
        sum(r['gain_loss_pct'] * r['current_equity'] for r in roster) / total_equity
        if total_equity > 0 else 0
    )

    return {
        'total': total,
        'funded': funded,
        'phase2': phase2,
        'one_step': one_step,
        'phase1': phase1,
        'total_liability': total_liability,
        'total_equity': total_equity,
        'weighted_avg_gain_loss': round(weighted_avg_gain_loss, 3),
    }


# ── Trade history queries ──────────────────────────────────────────────────────

def get_deals(login: int, limit: int = 50):
    """Get recent deals for a trader"""
    with mt5_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Deal, Login, Time, Symbol, Action, Entry, Volume, Price, Profit
            FROM deals
            WHERE Login = %s
            ORDER BY Time DESC
            LIMIT %s
        """, (login, limit))
        return cursor.fetchall()


def get_daily_equity_series(login: int):
    """Get equity curve data for Chart.js"""
    with mt5_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DATE(FROM_UNIXTIME(date_time)) as date, equity, daily_balance
            FROM daily
            WHERE login = %s AND date_time IS NOT NULL
            ORDER BY date_time ASC
        """, (login,))
        return cursor.fetchall()
# ── Analytics Functions ────────────────────────────────────────────────────────
# Add these functions to the END of your models/db_connector.py file

def get_trader_analytics(identifier: str):
    """
    Get comprehensive analytics for a trader by email or login.
    Returns detailed performance metrics and trade breakdown.
    """
    with prod_conn() as conn:
        cursor = conn.cursor()
        
        # Build query to get all deals for this trader
        # Check if identifier is email or login
        if '@' in identifier:
            filter_clause = "c.email = %s"
        else:
            filter_clause = "a.login = %s"
        
        query = f"""
        SELECT a.login, d.initial_balance, b.challenge_type_id, 
               a.position_id AS Position_ID, a.deal AS Deal_ID, a.order AS Order_ID,
               REPLACE(REPLACE(a.action, 0, 'BUY'), 1, 'SELL') AS Action,
               REPLACE(REPLACE(REPLACE(a.entry, 0, 'OPEN'), 1, 'CLOSE'), 3, 'CLOSE') AS Entry_Exit,
               a.contract_size, a.commission, a.time_msc, FROM_UNIXTIME(a.time) AS time,
               a.symbol, a.price, a.volume/10000*a.contract_size AS Volume, 
               a.profit, (a.profit + a.commission) AS net_profit,
               b.trader_id, c.first_name, c.last_name, c.email
        FROM `TheUpsideFunding-MT5`.deals a
        LEFT JOIN bl_fund_production.challenge_accounts b ON a.login = b.number
        LEFT JOIN bl_fund_production.traders c ON b.trader_id = c.id
        LEFT JOIN bl_fund_production.challenge_types d ON d.id = b.challenge_type_id
        WHERE a.Order <> 0 AND {filter_clause}
        ORDER BY a.position_id, a.time
        """
        
        cursor.execute(query, (identifier,))
        raw_deals = cursor.fetchall()
    
    if not raw_deals:
        return None
    
    # Process deals into completed positions
    import pandas as pd
    df = pd.DataFrame(raw_deals)
    
    # Get trader info
    trader_info = {
        'login': df.iloc[0]['login'],
        'email': df.iloc[0]['email'],
        'first_name': df.iloc[0]['first_name'],
        'last_name': df.iloc[0]['last_name'],
    }
    
    # Process into completed positions
    processed_positions = _process_deals_to_positions(df)
    
    if not processed_positions:
        return None
    
    # Calculate analytics
    analytics = _calculate_analytics_metrics(processed_positions)
    analytics['trader_info'] = trader_info
    analytics['total_positions'] = len(processed_positions)
    analytics['positions'] = processed_positions
    
    return analytics


def _process_deals_to_positions(df):
    """Process raw deals into completed positions"""
    import pandas as pd
    
    # Separate opens and closes
    opens = df[df['Entry_Exit'] == 'OPEN'].copy()
    closes = df[df['Entry_Exit'] == 'CLOSE'].copy()
    
    # Find completed positions
    open_positions = set(opens['Position_ID'].unique())
    close_positions = set(closes['Position_ID'].unique())
    completed = open_positions.intersection(close_positions)
    
    positions = []
    
    for position_id in completed:
        pos_opens = opens[opens['Position_ID'] == position_id]
        pos_closes = closes[closes['Position_ID'] == position_id]
        
        if pos_opens.empty or pos_closes.empty:
            continue
        
        # Aggregate volumes and calculate weighted average prices
        total_volume = pos_opens['Volume'].sum()
        entry_price = (pos_opens['price'] * pos_opens['Volume']).sum() / total_volume if total_volume > 0 else 0
        exit_price = (pos_closes['price'] * pos_closes['Volume']).sum() / total_volume if total_volume > 0 else 0
        
        profit_loss = pos_opens['net_profit'].sum() + pos_closes['net_profit'].sum()
        
        # Get times
        entry_time = pd.to_datetime(pos_opens['time'].min())
        exit_time = pd.to_datetime(pos_closes['time'].max())
        hold_minutes = (exit_time - entry_time).total_seconds() / 60 if exit_time > entry_time else 0
        
        positions.append({
            'position_id': position_id,
            'symbol': pos_opens.iloc[0]['symbol'],
            'action': pos_opens.iloc[0]['Action'],
            'entry_time': entry_time,
            'exit_time': exit_time,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'volume': total_volume,
            'profit_loss': profit_loss,
            'hold_time_minutes': hold_minutes,
        })
    
    return positions


def _calculate_analytics_metrics(positions):
    """Calculate comprehensive analytics from positions"""
    if not positions:
        return {}
    
    import pandas as pd
    import numpy as np
    
    df = pd.DataFrame(positions)
    
    total_trades = len(df)
    wins = df[df['profit_loss'] > 0]
    losses = df[df['profit_loss'] < 0]
    
    winning_trades = len(wins)
    losing_trades = len(losses)
    
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
    
    total_pnl = df['profit_loss'].sum()
    avg_pnl = df['profit_loss'].mean()
    
    avg_win = wins['profit_loss'].mean() if not wins.empty else 0
    avg_loss = losses['profit_loss'].mean() if not losses.empty else 0
    
    largest_win = df['profit_loss'].max()
    largest_loss = df['profit_loss'].min()
    
    # Profit factor
    gross_profit = wins['profit_loss'].sum() if not wins.empty else 0
    gross_loss = abs(losses['profit_loss'].sum()) if not losses.empty else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (float('inf') if gross_profit > 0 else 0)
    
    # Risk/Reward
    risk_reward = abs(avg_win / avg_loss) if avg_loss != 0 else (float('inf') if avg_win > 0 else 0)
    
    # Trade expectancy
    loss_rate = (losing_trades / total_trades * 100) if total_trades > 0 else 0
    trade_expectancy = (win_rate/100 * avg_win) - (loss_rate/100 * abs(avg_loss))
    
    # Symbol breakdown
    symbol_stats = []
    for symbol, symbol_df in df.groupby('symbol'):
        symbol_total = len(symbol_df)
        symbol_wins = len(symbol_df[symbol_df['profit_loss'] > 0])
        symbol_losses = len(symbol_df[symbol_df['profit_loss'] < 0])
        symbol_win_rate = (symbol_wins / symbol_total * 100) if symbol_total > 0 else 0
        symbol_pnl = symbol_df['profit_loss'].sum()
        
        symbol_wins_df = symbol_df[symbol_df['profit_loss'] > 0]
        symbol_losses_df = symbol_df[symbol_df['profit_loss'] < 0]
        symbol_avg_win = symbol_wins_df['profit_loss'].mean() if not symbol_wins_df.empty else 0
        symbol_avg_loss = symbol_losses_df['profit_loss'].mean() if not symbol_losses_df.empty else 0
        
        symbol_expectancy = (symbol_win_rate/100 * symbol_avg_win) - ((100-symbol_win_rate)/100 * abs(symbol_avg_loss))
        
        symbol_stats.append({
            'symbol': symbol,
            'total_trades': symbol_total,
            'win_rate': round(symbol_win_rate, 1),
            'total_pnl': round(symbol_pnl, 2),
            'avg_win': round(symbol_avg_win, 2),
            'avg_loss': round(symbol_avg_loss, 2),
            'expectancy': round(symbol_expectancy, 2),
        })
    
    # Sort by expectancy descending
    symbol_stats = sorted(symbol_stats, key=lambda x: x['expectancy'], reverse=True)
    
    # Hold time analysis
    avg_hold = df['hold_time_minutes'].mean()
    avg_winner_hold = wins['hold_time_minutes'].mean() if not wins.empty else 0
    avg_loser_hold = losses['hold_time_minutes'].mean() if not losses.empty else 0
    
    return {
        'total_trades': total_trades,
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': round(win_rate, 1),
        'total_pnl': round(total_pnl, 2),
        'avg_pnl': round(avg_pnl, 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'largest_win': round(largest_win, 2),
        'largest_loss': round(largest_loss, 2),
        'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'Infinite',
        'risk_reward': round(risk_reward, 2) if risk_reward != float('inf') else 'Infinite',
        'trade_expectancy': round(trade_expectancy, 2),
        'avg_hold_minutes': round(avg_hold, 1),
        'avg_winner_hold_minutes': round(avg_winner_hold, 1),
        'avg_loser_hold_minutes': round(avg_loser_hold, 1),
        'symbol_breakdown': symbol_stats,
    }


def format_hold_time(minutes):
    """Format hold time in readable format"""
    if minutes < 1:
        return f"{int(minutes * 60)} seconds"
    elif minutes < 60:
        return f"{int(minutes)} minutes"
    elif minutes < 1440:
        hours = int(minutes / 60)
        mins = int(minutes % 60)
        return f"{hours}h {mins}m"
    else:
        days = int(minutes / 1440)
        hours = int((minutes % 1440) / 60)
        return f"{days}d {hours}h"
