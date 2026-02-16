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

PROFIT_SHARE_PCT = 0.70
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


def _process_rows(rows: list) -> list:
    """
    Apply the same post-processing logic as the production script:
    - Starting balance fallback chain
    - Equity fallback chain
    - Gain/loss %
    - Pot. liability
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

        # Pot. liability (Funded only)
        pot_liability = (change * PROFIT_SHARE_PCT) if (phase == 'Funded' and change > 0) else 0

        processed.append({
            'login':            row.get('login'),
            'category':         row.get('category'),
            'title':            title,
            'phase':            phase,
            'trader_id':        row.get('trader_id'),
            'first_name':       row.get('first_name') or '',
            'last_name':        row.get('last_name') or '',
            'email':            row.get('email') or '',
            'status':           str(row.get('status') or '').upper(),
            'starting_balance': starting_balance,
            'current_equity':   current_equity,
            'change':           change,
            'gain_loss_pct':    gain_loss_pct,
            'profit_share_pct': PROFIT_SHARE_PCT * 100 if phase == 'Funded' else None,
            'pot_liability':    round(pot_liability, 2),
            'date_time':        row.get('date_time'),
        })

    return processed


def get_roster() -> list:
    """
    Full trader roster — the main dataset for the manager dashboard.
    Returns processed rows sorted by phase order then gain/loss desc.
    """
    with prod_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ROSTER_QUERY)
            rows = cur.fetchall()

    processed = _process_rows(rows)

    # Sort: phase order first, then gain/loss desc within phase
    phase_rank = {p: i for i, p in enumerate(PHASE_ORDER)}
    processed.sort(
        key=lambda r: (phase_rank.get(r['phase'], 99), -r['gain_loss_pct'])
    )
    return processed


def get_roster_summary(roster: list | None = None) -> dict:
    """
    Aggregate summary cards for the top of the manager dashboard.
    Pass an existing roster list to avoid a second DB hit.
    """
    rows = roster if roster is not None else get_roster()

    summary = {
        'total':           len(rows),
        'house':           0,
        'funded':          0,
        'one_step':        0,
        'phase2':          0,
        'phase1':          0,
        'unknown':         0,
        'total_liability': 0.0,
        'total_equity':    0.0,
    }

    for r in rows:
        phase = r['phase']
        if phase == 'House Account': summary['house'] += 1
        elif phase == 'Funded':      summary['funded'] += 1
        elif phase == '1-Step':      summary['one_step'] += 1
        elif phase == 'Phase 2':     summary['phase2'] += 1
        elif phase == 'Phase 1':     summary['phase1'] += 1
        else:                        summary['unknown'] += 1

        summary['total_liability'] += r['pot_liability'] or 0
        summary['total_equity']    += r['current_equity'] or 0

    # Weighted average gain/loss across all non-house accounts
    trading_rows = [r for r in rows if r['phase'] != 'House Account']
    total_eq = sum(r['current_equity'] or 0 for r in trading_rows)
    if total_eq:
        summary['weighted_avg_gain_loss'] = round(
            sum((r['gain_loss_pct'] * (r['current_equity'] or 0)) for r in trading_rows) / total_eq, 3
        )
    else:
        summary['weighted_avg_gain_loss'] = 0.0

    return summary


def get_trader_roster_row(login: int) -> dict | None:
    """Single trader's roster row (for the drill-down header)."""
    roster = get_roster()
    return next((r for r in roster if r['login'] == login), None)


def get_daily_equity_series(login: int) -> list:
    """
    Daily equity snapshots for a single login — used to render the equity curve.
    Returns list of {date, equity, daily_balance} dicts ordered oldest to newest.
    """
    with mt5_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    FROM_UNIXTIME(date_time) AS date,
                    equity,
                    daily_balance
                FROM `TheUpsideFunding-MT5`.daily
                WHERE login = %s
                  AND date_time IS NOT NULL
                ORDER BY date_time ASC
            """, (login,))
            return cur.fetchall()


def get_deals(login: int, limit: int = 500) -> list:
    """
    Closed deals for a single login, newest first.
    """
    with mt5_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM `TheUpsideFunding-MT5`.deals
                WHERE login = %s
                ORDER BY time DESC
                LIMIT %s
            """, (login, limit))
            return cur.fetchall()


def test_connections() -> dict:
    """Smoke-test both DB connections — call on app startup."""
    results = {}
    for name, ctx in [('MT5', mt5_conn), ('Production', prod_conn)]:
        try:
            with ctx() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            results[name] = 'connected'
        except Exception as e:
            results[name] = f'ERROR: {e}'
    return results
