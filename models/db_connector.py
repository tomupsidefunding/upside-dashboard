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
            'login':          login,
            'category':       row.get('category'),
            'phase':          phase,
            'first_name':     row.get('first_name'),
            'last_name':      row.get('last_name'),
            'email':          row.get('email'),
            'equity':         current_equity or 0,
            'gain_loss_pct':  gain_loss_pct,
            'gain_loss_raw':  change,
            'pot_liability':  pot_liability,
            'payout_pct':     payout_pct * 100,  # Convert to percentage for display
            'status':         row.get('status'),
        })

    return processed


def get_trader_roster():
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
    roster = get_trader_roster()
    for row in roster:
        if row['login'] == login:
            return row
    return None


def get_summary_stats(roster: list):
    """Calculate summary statistics from roster"""
    total = len(roster)
    funded = sum(1 for r in roster if r['phase'] == 'Funded')
    phase2 = sum(1 for r in roster if r['phase'] == 'Phase 2')
    one_step = sum(1 for r in roster if r['phase'] == '1-Step')
    phase1 = sum(1 for r in roster if r['phase'] == 'Phase 1')
    
    total_liability = sum(r['pot_liability'] for r in roster if r['phase'] == 'Funded')
    
    # Weighted average performance
    total_equity = sum(r['equity'] for r in roster)
    weighted_avg = (
        sum(r['gain_loss_pct'] * r['equity'] for r in roster) / total_equity
        if total_equity > 0 else 0
    )

    return {
        'total': total,
        'funded': funded,
        'phase2': phase2,
        'one_step': one_step,
        'phase1': phase1,
        'total_liability': total_liability,
        'weighted_avg_pct': round(weighted_avg, 3),
        'total_equity': total_equity,
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
