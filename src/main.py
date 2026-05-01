"""
UCL DATA PIPELINE - Master Orchestrator
========================================
Single command: python src/main.py

Flow:
  Phase 0: Run all tests (quality gate)
  Phase 1: Run all scrapers (UEFA season, Transfermarkt, match-level)
  Phase 2: Format raw data, save JSON & text logs
  Phase 3: Merge formatted data into data/processed/ CSVs
"""
import pandas as pd
import os, sys, json, subprocess
from difflib import SequenceMatcher
from datetime import datetime

# Add current dir to path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.uefa import UEFAScraper
from scrapers.espn import ESPNScraper
from scrapers.utils import is_null, teams_match
from config import expand_aliases, load_espn_overrides
from formatter import (
    are_equivalent, format_possession, generate_player_id,
    soft_norm, norm_unit
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
RESULTS_DIR = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'results')

ENRICHABLE = [
    'referee', 'stadium', 'city', 'country',
    'possession_home', 'possession_away'
]


def table_path(raw_relative, processed_relative=None):
    """Prefer the legacy raw/core layout, fall back to current processed layout."""
    raw_path = os.path.join(RAW_DIR, *raw_relative)
    if os.path.exists(raw_path) or processed_relative is None:
        return raw_path
    processed_path = os.path.join(PROCESSED_DIR, *processed_relative)
    return processed_path


def player_id_candidates(name):
    pid = generate_player_id(name)
    return [pid, f"player_{pid}"]


def player_name_score(source_name, target_name):
    source = soft_norm(source_name)
    target = soft_norm(target_name)
    if not source or not target:
        return 0.0
    if source == target:
        return 1.0
    source_parts = set(source.split())
    target_parts = set(target.split())
    overlap = len(source_parts & target_parts) / max(len(source_parts | target_parts), 1)
    ratio = SequenceMatcher(None, source, target).ratio()
    last_match = bool(source.split() and target.split() and source.split()[-1] == target.split()[-1])
    return max(ratio, overlap + (0.15 if last_match else 0))


# ============================================================
# LOGGING
# ============================================================
class PipelineLogger:
    """Dual logger: writes to text file + builds JSON report."""

    def __init__(self):
        os.makedirs(LOGS_DIR, exist_ok=True)
        self.ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.text_path = os.path.join(LOGS_DIR, f'pipeline_{self.ts}.log')
        self.json_report = {
            'run': self.ts,
            'phases': {},
            'matches_enriched': [],
            'scraped_records': {},
            'errors': []
        }
        self._f = open(self.text_path, 'w', encoding='utf-8')

    def log(self, msg):
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        print(line)
        self._f.write(line + '\n')
        self._f.flush()

    def log_match(self, info, fields):
        self.json_report['matches_enriched'].append({
            'match': info, 'fields': fields
        })

    def log_error(self, msg):
        self.json_report['errors'].append(msg)
        self.log(f"[!] ERROR: {msg}")

    def set_phase(self, name, status, detail=None):
        self.json_report['phases'][name] = {
            'status': status, 'detail': detail or ''
        }

    def save(self):
        self._f.close()
        json_path = os.path.join(LOGS_DIR, f'pipeline_{self.ts}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.json_report, f, indent=2, ensure_ascii=False)
        print(f"\n[*] Logs saved: {self.text_path}")
        print(f"[*] JSON report: {json_path}")


# ============================================================
# PHASE 0: QUALITY GATE
# ============================================================
def phase_0_tests(log):
    log.log("=" * 60)
    log.log("PHASE 0: QUALITY GATE")
    log.log("=" * 60)

    r = subprocess.run(
        [sys.executable, '-m', 'pytest', 'tests/', '-v', '--tb=short'],
        cwd=BASE_DIR, capture_output=True, text=True
    )
    log.log(r.stdout[-800:] if len(r.stdout) > 800 else r.stdout)

    if r.returncode != 0:
        log.log("[X] TESTS FAILED — Pipeline aborted.")
        log.set_phase('tests', 'FAILED')
        log.save()
        sys.exit(1)

    log.log("[V] All tests passed.\n")
    log.set_phase('tests', 'PASSED')


# ============================================================
# PHASE 1: SCRAPING
# ============================================================
def phase_1_scraping(log):
    log.log("=" * 60)
    log.log("PHASE 1: SCRAPING")
    log.log("=" * 60)

    # --- 1a. UEFA Season Stats (Playwright, subprocess) ---
    log.log("\n--- 1a. UEFA Season Stats ---")
    r = subprocess.run(
        [sys.executable, os.path.join(BASE_DIR, 'src', 'scrapers', 'uefa_season_scraper.py')],
        cwd=BASE_DIR, capture_output=True, text=True
    )
    log.log(r.stdout[-600:] if len(r.stdout) > 600 else r.stdout)
    if r.returncode != 0:
        log.log_error(f"UEFA scraper exit code {r.returncode}")
    log.set_phase('scrape_uefa_season', 'DONE' if r.returncode == 0 else 'ERROR')

    # --- 1b. Transfermarkt (Playwright, subprocess) ---
    log.log("\n--- 1b. Transfermarkt Player Profiles ---")
    r = subprocess.run(
        [sys.executable, os.path.join(BASE_DIR, 'src', 'scrapers', 'transfermarkt.py')],
        cwd=BASE_DIR, capture_output=True, text=True
    )
    log.log(r.stdout[-600:] if len(r.stdout) > 600 else r.stdout)
    if r.returncode != 0:
        log.log_error(f"Transfermarkt scraper exit code {r.returncode}")
    log.set_phase('scrape_transfermarkt', 'DONE' if r.returncode == 0 else 'ERROR')

    # --- 1c. Match-level enrichment (UEFA API + ESPN API) ---
    log.log("\n--- 1c. Match-Level Enrichment (UEFA API + ESPN) ---")
    enrichment_results = _run_match_enrichment(log)
    log.set_phase('scrape_match_enrichment', 'DONE',
                  f"{len(enrichment_results)} matches processed")

    return enrichment_results


def _run_match_enrichment(log):
    """Scrape missing match-level data AND player stats per match."""
    matches_path = table_path(('core', 'matches.csv'), ('core', 'matches_cleaned.csv'))
    teams_path = table_path(('core', 'teams.csv'), ('core', 'teams_cleaned.csv'))
    events_path = table_path(('events', 'goals_events.csv'), ('events', 'goals_events_cleaned.csv'))
    stats_path = table_path(('stats', 'player_match_stats.csv'), ('stats', 'player_match_stats_cleaned.csv'))
    players_path = table_path(('core', 'players.csv'), ('core', 'players_cleaned.csv'))

    if not os.path.exists(matches_path):
        log.log_error(f"matches.csv not found at {matches_path}")
        return []

    df_matches = pd.read_csv(matches_path, keep_default_na=False)
    df_teams = pd.read_csv(teams_path, keep_default_na=False)
    df_events = pd.read_csv(events_path, keep_default_na=False) if os.path.exists(events_path) else pd.DataFrame()
    df_stats = pd.read_csv(stats_path, keep_default_na=False) if os.path.exists(stats_path) else pd.DataFrame()
    df_players = pd.read_csv(players_path, keep_default_na=False) if os.path.exists(players_path) else pd.DataFrame()

    team_map = dict(zip(df_teams['team_id'], df_teams['team_name']))
    rev_team_map = dict(zip(df_teams['team_name'], df_teams['team_id']))

    uefa_idx = os.path.join(RESULTS_DIR, 'uefa', 'match_index.json')
    u_scraper = UEFAScraper(index_path=uefa_idx)
    e_scraper = ESPNScraper()
    espn_overrides = load_espn_overrides()

    results = []
    stats_updated = 0

    # ESPN stat fields → our CSV columns
    STAT_MAP = {
        'shots': 'shots', 'shots_on_target': 'shots_on_target',
        'fouls_committed': 'fouls_committed', 'fouls_suffered': 'fouls_suffered',
        'yellow_cards': 'yellow_cards', 'red_cards': 'red_cards',
        'goals': 'goals', 'assists': 'assists',
    }

    for idx, row in df_matches.iterrows():
        mid = row['match_id']
        season, date = str(row['season']), str(row['date'])
        local = team_map.get(row['home_team_id'], 'Unknown')
        away = team_map.get(row['away_team_id'], 'Unknown')
        info_str = f"{season} | {local} vs {away} ({date})"

        # Check what needs enrichment
        needs_info = any(
            is_null(row.get(c)) or (c in ['referee', 'stadium', 'city'] and str(row.get(c)) == '0')
            for c in ENRICHABLE
        )

        # Check if player stats for this match are mostly zeros
        match_stats = df_stats[df_stats['match_id'] == mid] if not df_stats.empty else pd.DataFrame()
        needs_stats = match_stats.empty or (
            not match_stats.empty and 
            match_stats[['shots', 'fouls_committed']].apply(pd.to_numeric, errors='coerce').fillna(0).sum().sum() == 0
        )

        if not needs_info and not needs_stats:
            continue

        # ESPN API
        e_id = e_scraper.find_event(date, local, away, expand_aliases, overrides=espn_overrides)
        e_struct = e_scraper.get_structured_data(e_id, local) if e_id else {
            'match_info': {}, 'player_stats': [], 'events': [], 'team_stats': {}
        }

        # UEFA API (only for match info)
        if needs_info:
            u_id = u_scraper.find_id(date, local, away, expand_aliases)
            u_data = u_scraper.get_match_info(u_id) if u_id else {}
            merged = {**e_struct['match_info'], **u_data}
            fields_updated = []
            for col in ENRICHABLE:
                if col in merged and not is_null(merged[col]):
                    cur = row.get(col)
                    new = format_possession(merged[col]) if col.startswith('possession') else merged[col]
                    if is_null(cur):
                        df_matches.at[idx, col] = new
                        fields_updated.append(col)
                    elif not are_equivalent(cur, new, col):
                        _log_mismatch(info_str, col, cur, new)
            if fields_updated:
                results.append({'match': info_str, 'fields': fields_updated, 'source': 'UEFA/ESPN'})
                log.log_match(info_str, fields_updated)

        # FILL PLAYER STATS from ESPN
        if needs_stats and e_struct.get('player_stats'):
            log.log(f"  [{idx}] Filling stats: {info_str}")
            
            # Build name lookup for players in this match
            match_rows = df_stats[df_stats['match_id'] == mid]
            if match_rows.empty:
                continue
            
            # Candidate list with team context. Avoid surname-only updates unless
            # the best match is unique and high-confidence.
            candidates = []
            for sidx, srow in match_rows.iterrows():
                pid = srow['player_id']
                p_row = df_players[df_players['player_id'] == pid]
                if not p_row.empty:
                    pname = p_row.iloc[0]['player_name']
                    tname = team_map.get(srow.get('team_id'), '')
                    candidates.append((sidx, pname, tname))
            
            for ps in e_struct['player_stats']:
                p_name = ps.get('player_name', '')
                if not p_name or p_name == 'Unknown':
                    continue
                
                # Try to find the player in our match stats
                espn_norm = soft_norm(p_name)
                espn_parts = espn_norm.split()
                sidx = None
                
                scored = []
                for idx_val, csv_name, csv_team in candidates:
                    score = player_name_score(p_name, csv_name)
                    if ps.get('team_name') and csv_team and not teams_match(ps.get('team_name'), csv_team):
                        score -= 0.2
                    scored.append((score, idx_val, csv_name))
                scored.sort(reverse=True)
                if scored and scored[0][0] >= 0.82:
                    second = scored[1][0] if len(scored) > 1 else 0
                    if scored[0][0] - second >= 0.05:
                        sidx = scored[0][1]
                
                if sidx is not None:
                    for espn_key, csv_col in STAT_MAP.items():
                        espn_val = ps.get(espn_key, 0)
                        if espn_val and int(espn_val) != 0:
                            cur_val = df_stats.at[sidx, csv_col]
                            if cur_val == 0 or cur_val == 0.0 or str(cur_val) == '0' or str(cur_val) == '0.0':
                                df_stats.at[sidx, csv_col] = int(espn_val)
                                stats_updated += 1

        # Merge events
        if e_struct.get('events') and (df_events.empty or mid not in df_events['match_id'].values):
            for ev in e_struct['events']:
                ev['match_id'] = mid
                df_events = pd.concat([df_events, pd.DataFrame([ev])], ignore_index=True)

    # Save all CSVs
    df_matches.to_csv(matches_path, index=False, encoding='utf-8')
    if not df_stats.empty:
        df_stats.to_csv(stats_path, index=False, encoding='utf-8')
    if not df_events.empty:
        df_events.to_csv(events_path, index=False, encoding='utf-8')

    log.log(f"  [V] Match enrichment done. {len(results)} matches updated, {stats_updated} stat fields filled.")
    return results


def _log_mismatch(match_info, field, current_val, new_val):
    path = os.path.join(LOGS_DIR, 'mismatches.log')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{ts}] {match_info} | {field} | Old: {current_val} | New: {new_val}\n")


# ============================================================
# PHASE 2: FORMAT & MERGE INTO CSVs
# ============================================================
def phase_2_format_and_merge(log):
    log.log("=" * 60)
    log.log("PHASE 2: FORMAT & MERGE INTO CSVs")
    log.log("=" * 60)

    # --- 2a. Ingest UEFA season dump ---
    uefa_dump = os.path.join(RESULTS_DIR, 'uefa', 'raw_dump.json')
    if os.path.exists(uefa_dump):
        log.log("\n--- 2a. Ingesting UEFA season stats ---")
        _ingest_uefa_dump(uefa_dump, log)
    else:
        log.log("\n--- 2a. No UEFA dump found, skipping ---")

    # --- 2b. Ingest Transfermarkt dump ---
    tm_dump = os.path.join(RESULTS_DIR, 'transfermarkt', 'raw_dump.json')
    if os.path.exists(tm_dump):
        log.log("\n--- 2b. Ingesting Transfermarkt data ---")
        _ingest_tm_dump(tm_dump, log)
    else:
        log.log("\n--- 2b. No Transfermarkt dump found, skipping ---")

    # --- 2c. Fill goals_events gaps ---
    log.log("\n--- 2c. Filling goals_events gaps ---")
    _fill_goals_events(log)

    # --- 2d. Cross-validate data across sources ---
    log.log("\n--- 2d. Cross-validating data integrity ---")
    _cross_validate(log)

    log.set_phase('format_merge', 'DONE')


def _ingest_uefa_dump(dump_path, log):
    """Read UEFA raw JSON dump, format records, merge into CSVs."""
    with open(dump_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    players_path = table_path(('core', 'players.csv'), ('core', 'players_cleaned.csv'))
    gk_path = table_path(('stats', 'goalkeeper_stats.csv'), ('stats', 'goalkeeper_stats_cleaned.csv'))
    ss_path = table_path(('stats', 'player_season_stats.csv'), ('stats', 'player_season_stats_cleaned.csv'))

    df_players = pd.read_csv(players_path, keep_default_na=False) if os.path.exists(players_path) else pd.DataFrame()
    df_gk = pd.read_csv(gk_path, keep_default_na=False) if os.path.exists(gk_path) else pd.DataFrame()
    df_ss = pd.read_csv(ss_path, keep_default_na=False) if os.path.exists(ss_path) else pd.DataFrame()

    new_gk, new_ss, new_players = [], [], []

    # Goalkeeping
    for row in data.get('goalkeeping', []):
        pid = row['player_id']
        s = row.get('stats', {})
        if s:
            new_gk.append({
                'player_id': pid, 'season': row['season'],
                'saves': s.get('saves', 0),
                'goals_conceded': s.get('goals_conceded', 0),
                'clean_sheets': s.get('clean_sheets', 0),
                'penalty_saves': s.get('penalty_saves', 0),
                'punches': s.get('punches', 0)
            })
        if df_players.empty or pid not in df_players['player_id'].values:
            new_players.append({'player_id': pid, 'player_name': row['player_name']})

    # Disciplinary, Attacking, Passing
    for cat in ['disciplinary', 'attacking', 'passing']:
        for row in data.get(cat, []):
            pid = row['player_id']
            s = row.get('stats', {})
            rec = {'player_id': pid, 'season': row['season']}
            rec.update(s)  # Named stats map directly to our columns
            new_ss.append(rec)
            if df_players.empty or pid not in df_players['player_id'].values:
                new_players.append({'player_id': pid, 'player_name': row['player_name']})

    # Merge & deduplicate
    if new_gk:
        df_gk = pd.concat([df_gk, pd.DataFrame(new_gk)]).drop_duplicates(
            subset=['player_id', 'season'], keep='last')
        df_gk.to_csv(gk_path, index=False, encoding='utf-8')
        log.log(f"  Goalkeeper stats: {len(df_gk)} records")

    if new_ss:
        df_ss = pd.concat([df_ss, pd.DataFrame(new_ss)]).drop_duplicates(
            subset=['player_id', 'season'], keep='last')
        df_ss.to_csv(ss_path, index=False, encoding='utf-8')
        log.log(f"  Season stats: {len(df_ss)} records")

    if new_players:
        df_players = pd.concat([df_players, pd.DataFrame(new_players)]).drop_duplicates(
            subset=['player_id'], keep='first')
        df_players.to_csv(players_path, index=False, encoding='utf-8')
        log.log(f"  Players: {len(df_players)} records")

    log.json_report['scraped_records']['uefa_season'] = {
        'goalkeepers': len(new_gk), 'season_stats': len(new_ss), 'new_players': len(new_players)
    }


def _ingest_tm_dump(dump_path, log):
    """Read Transfermarkt JSON dump, update player profiles in players.csv."""
    with open(dump_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    players_path = table_path(('core', 'players.csv'), ('core', 'players_cleaned.csv'))
    df = pd.read_csv(players_path, keep_default_na=False) if os.path.exists(players_path) else pd.DataFrame()

    updated = 0
    for entry in data:
        name = entry.get('player_name', '')
        if not name:
            continue
        candidates = player_id_candidates(name)
        mask = df['player_id'].isin(candidates)
        if mask.any():
            idx = df[mask].index[0]
            for field in ['height_cm', 'position', 'nationality', 'birth_date', 'birth_place']:
                val = entry.get(field)
                if val and str(val) != 'NULL' and is_null(df.at[idx, field] if field in df.columns else ''):
                    if field not in df.columns:
                        df[field] = ''
                    df.at[idx, field] = val
                    updated += 1

    df.to_csv(players_path, index=False, encoding='utf-8')
    log.log(f"  Transfermarkt: {updated} fields updated across player profiles")
    log.json_report['scraped_records']['transfermarkt'] = {'fields_updated': updated}


def _fill_goals_events(log):
    """Fill gaps in goals_events.csv using existing data + ESPN event data."""
    events_path = table_path(('events', 'goals_events.csv'), ('events', 'goals_events_cleaned.csv'))
    players_path = table_path(('core', 'players.csv'), ('core', 'players_cleaned.csv'))
    matches_path = table_path(('core', 'matches.csv'), ('core', 'matches_cleaned.csv'))

    if not os.path.exists(events_path):
        log.log("  No goals_events.csv found, skipping")
        return

    df_ev = pd.read_csv(events_path, keep_default_na=False)
    df_pl = pd.read_csv(players_path, keep_default_na=False) if os.path.exists(players_path) else pd.DataFrame()
    df_ma = pd.read_csv(matches_path, keep_default_na=False) if os.path.exists(matches_path) else pd.DataFrame()

    # Build player_id → player_name lookup
    pid_to_name = dict(zip(df_pl['player_id'], df_pl['player_name'])) if not df_pl.empty else {}

    filled = 0
    for idx, row in df_ev.iterrows():
        # 1. Fill player_name from players.csv
        if is_null(row.get('player_name', '')) and row.get('player_id'):
            name = pid_to_name.get(row['player_id'], '')
            if name:
                df_ev.at[idx, 'player_name'] = name
                filled += 1

        # 2. Fill event_type (always 'goal' for this table)
        if is_null(row.get('event_type', '')):
            df_ev.at[idx, 'event_type'] = 'goal'
            filled += 1

        # 3. Derive is_penalty and is_own_goal from goal_type
        goal_type = str(row.get('goal_type', '')).lower()
        if is_null(row.get('is_penalty', '')):
            df_ev.at[idx, 'is_penalty'] = 'penalty' in goal_type
            filled += 1
        if is_null(row.get('is_own_goal', '')):
            df_ev.at[idx, 'is_own_goal'] = 'own' in goal_type
            filled += 1

    # 4. Fill assist_player_id from ESPN events where available
    teams_path = table_path(('core', 'teams.csv'), ('core', 'teams_cleaned.csv'))
    df_te = pd.read_csv(teams_path, keep_default_na=False) if os.path.exists(teams_path) else pd.DataFrame()
    team_map = dict(zip(df_te['team_id'], df_te['team_name'])) if not df_te.empty else {}

    e_scraper = ESPNScraper()
    espn_overrides = load_espn_overrides()

    # Group events by match for efficient ESPN calls
    matches_needing_assists = set()
    for _, row in df_ev.iterrows():
        if is_null(row.get('assist_player_id', '')):
            matches_needing_assists.add(row['match_id'])

    assist_filled = 0
    for mid in list(matches_needing_assists)[:50]:  # Limit to avoid too many API calls
        m_row = df_ma[df_ma['match_id'] == mid]
        if m_row.empty:
            continue
        m_row = m_row.iloc[0]
        local = team_map.get(m_row['home_team_id'], 'Unknown')
        away = team_map.get(m_row['away_team_id'], 'Unknown')
        date = str(m_row['date'])

        e_id = e_scraper.find_event(date, local, away, expand_aliases, overrides=espn_overrides)
        if not e_id:
            continue

        e_data = e_scraper.get_structured_data(e_id, local)
        espn_events = e_data.get('events', [])

        # Match ESPN events to our events by minute
        match_events = df_ev[df_ev['match_id'] == mid]
        for espn_ev in espn_events:
            if espn_ev.get('event_type') != 'goal':
                continue
            espn_minute = str(espn_ev.get('minute', ''))
            espn_player = espn_ev.get('player_name', '')

            # Find matching event in our data by minute
            for eidx, erow in match_events.iterrows():
                ev_minute = str(erow.get('minute', ''))
                if ev_minute == espn_minute and is_null(erow.get('assist_player_id', '')):
                    assist_name = espn_ev.get('assist_player_name') or espn_ev.get('assist_name')
                    if assist_name:
                        assist_pid = player_id_candidates(assist_name)[-1]
                        if assist_pid != erow.get('player_id', ''):
                            df_ev.at[eidx, 'assist_player_id'] = assist_pid
                            assist_filled += 1
                    break

    df_ev.to_csv(events_path, index=False, encoding='utf-8')
    log.log(f"  Goals events: {filled} fields filled, {assist_filled} assists added")
    log.json_report['scraped_records']['goals_events'] = {
        'fields_filled': filled, 'assists_added': assist_filled
    }


def _cross_validate(log):
    """Cross-validate data consistency across all CSVs using are_equivalent()."""
    matches_path = table_path(('core', 'matches.csv'), ('core', 'matches_cleaned.csv'))
    stats_path = table_path(('stats', 'player_match_stats.csv'), ('stats', 'player_match_stats_cleaned.csv'))
    events_path = table_path(('events', 'goals_events.csv'), ('events', 'goals_events_cleaned.csv'))
    players_path = table_path(('core', 'players.csv'), ('core', 'players_cleaned.csv'))

    df_matches = pd.read_csv(matches_path, keep_default_na=False) if os.path.exists(matches_path) else pd.DataFrame()
    df_stats = pd.read_csv(stats_path, keep_default_na=False) if os.path.exists(stats_path) else pd.DataFrame()
    df_events = pd.read_csv(events_path, keep_default_na=False) if os.path.exists(events_path) else pd.DataFrame()
    df_players = pd.read_csv(players_path, keep_default_na=False) if os.path.exists(players_path) else pd.DataFrame()

    issues = []

    # 1. Validate: goal count in events matches match score
    if not df_events.empty and not df_matches.empty:
        for _, m in df_matches.iterrows():
            mid = m['match_id']
            match_goals = df_events[df_events['match_id'] == mid]
            expected_total = int(m.get('home_score', 0)) + int(m.get('away_score', 0))
            actual_total = len(match_goals)
            if actual_total > 0 and actual_total != expected_total:
                issues.append(
                    f"Match {mid}: score says {expected_total} goals but events has {actual_total}"
                )

    # 2. Validate: player_match_stats goals match events for that player
    if not df_stats.empty and not df_events.empty:
        for pid in df_stats['player_id'].unique()[:200]:  # Sample check
            stat_goals = df_stats[df_stats['player_id'] == pid]['goals'].astype(float).sum()
            event_goals = len(df_events[
                (df_events['player_id'] == pid) & 
                (df_events['goal_type'] != 'own_goal')
            ])
            if stat_goals > 0 and event_goals > 0:
                if not are_equivalent(str(int(stat_goals)), str(event_goals), 'goals'):
                    pname = df_players[df_players['player_id'] == pid]
                    pname = pname.iloc[0]['player_name'] if not pname.empty else pid
                    issues.append(
                        f"Player {pname}: stats say {int(stat_goals)} goals but events has {event_goals}"
                    )

    # 3. Validate: all player_ids in stats exist in players table
    if not df_stats.empty and not df_players.empty:
        stats_pids = set(df_stats['player_id'].unique())
        players_pids = set(df_players['player_id'].unique())
        orphans = stats_pids - players_pids
        if orphans:
            issues.append(f"Orphan player_ids in stats (not in players.csv): {len(orphans)}")

    # 4. Validate: all match_ids in stats exist in matches table
    if not df_stats.empty and not df_matches.empty:
        stats_mids = set(df_stats['match_id'].unique())
        matches_mids = set(df_matches['match_id'].unique())
        orphan_matches = stats_mids - matches_mids
        if orphan_matches:
            issues.append(f"Orphan match_ids in stats (not in matches.csv): {len(orphan_matches)}")

    # Log results
    if issues:
        log.log(f"  Found {len(issues)} data inconsistencies:")
        for i in issues[:20]:
            log.log(f"    - {i}")
            _log_mismatch("CROSS-VALIDATION", "integrity", "", i)
    else:
        log.log("  [V] No data inconsistencies found!")

    log.json_report['cross_validation'] = {
        'issues_found': len(issues),
        'details': issues[:50]
    }


# ============================================================
# PHASE 3: DIAGNOSTICS & SUMMARY
# ============================================================
def phase_3_diagnostics(log):
    log.log("=" * 60)
    log.log("PHASE 3: DIAGNOSTICS & COVERAGE REPORT")
    log.log("=" * 60)

    # Run coverage report if available
    cov_script = os.path.join(BASE_DIR, 'tests', 'api_diagnostics', 'generate_coverage_report.py')
    if os.path.exists(cov_script):
        r = subprocess.run([sys.executable, cov_script], cwd=BASE_DIR,
                           capture_output=True, text=True)
        log.log(r.stdout[-400:] if len(r.stdout) > 400 else r.stdout)

    log.set_phase('diagnostics', 'DONE')


# ============================================================
# MAIN ENTRY POINT
# ============================================================
def main():
    log = PipelineLogger()

    log.log("=" * 60)
    log.log("UCL DATA PIPELINE - MASTER ORCHESTRATOR")
    log.log(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.log("=" * 60)

    phase_0_tests(log)
    phase_1_scraping(log)
    phase_2_format_and_merge(log)
    phase_3_diagnostics(log)

    log.log("\n" + "=" * 60)
    log.log("PIPELINE COMPLETE")
    log.log("=" * 60)
    log.log("Data saved to: data/processed/")
    log.log("JSON logs: tests/api_diagnostics/results/")
    log.log(f"Text logs: {LOGS_DIR}/")

    log.save()


if __name__ == "__main__":
    main()
