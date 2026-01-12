# -*- coding: utf-8 -*-
"""
Multigol squadra – Serie A (Streamlit) | Versione PULITA

✅ Storico: Excel "MG ITALIA.xlsx"
✅ Stagione corrente: automatico (football-data.co.uk) – si aggiorna da solo
✅ Prossima partita: automatica; se non disponibile -> inserimento manuale (avversario + casa/trasferta)
✅ Mercati: Over 1.5, Multigol 0-1 / 1-2 / 1-3 / 2-3 / 2-4
✅ Filtri: quota fair minima (slider) + probabilità minima (slider)
✅ Output: mercato consigliato + Top N mercati dopo filtri

❌ Backtest rimosso
❌ Ritardi/strisce rimossi
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

import os
import json

QUOTES_STORE_PATH = "quotes_bookmaker.json"

def load_quotes_store() -> Dict[str, float]:
    """Carica le quote bookmaker salvate (persistenza locale)."""
    try:
        if os.path.exists(QUOTES_STORE_PATH):
            with open(QUOTES_STORE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                # normalizza a float/None
                out = {}
                for k, v in data.items():
                    try:
                        out[str(k)] = float(v)
                    except Exception:
                        # consenti None
                        out[str(k)] = None
                return out
    except Exception:
        pass
    return {}

def save_quotes_store(data: Dict[str, float]) -> None:
    """Salva le quote bookmaker in un file JSON (se possibile)."""
    try:
        with open(QUOTES_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        # su Streamlit Cloud può essere in sola lettura o effimero: in quel caso non blocchiamo l'app
        pass


# ---------------- Config ----------------
EXCEL_PATH_DEFAULT = "MG ITALIA.xlsx"
LEAGUE_CODE = "I1"
REQUEST_TIMEOUT = 20

MAX_GOALS = 5  # 0,1,2,3,4,5+
MULTIGOL_RANGES = [(0, 1), (1, 2), (1, 3), (2, 3), (2, 4)]
OVER_LINE = 1.5

TEAM_ALIASES: Dict[str, List[str]] = {
    "Inter": ["Inter", "Inter Milan", "Internazionale", "FC Internazionale"],
    "Milan": ["AC Milan", "Milan", "A.C. Milan"],
    "Juve": ["Juventus", "Juventus FC", "Juve"],
    "Roma": ["Roma", "AS Roma"],
    "Lazio": ["Lazio", "SS Lazio"],
    "Atalanta": ["Atalanta", "Atalanta BC"],
    "Bologna": ["Bologna", "Bologna FC", "Bologna 1909"],
    "Napoli": ["Napoli"],
}

# ---------------- Helpers ----------------
def _season_folder(start_year: int) -> str:
    end_year = start_year + 1
    return f"{str(start_year)[-2:]}{str(end_year)[-2:]}"

def _current_start_year(today: date) -> int:
    return today.year if today.month >= 7 else today.year - 1

def _norm_team(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9àèéìòù\s\.\-']", "", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _is_same_team(a: str, b: str) -> bool:
    na, nb = _norm_team(a), _norm_team(b)
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    for vals in TEAM_ALIASES.values():
        nvals = {_norm_team(v) for v in vals}
        if na in nvals and nb in nvals:
            return True
    return False

# ---------------- Poisson ----------------
def poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)

def poisson_probs(lam: float, max_goals: int = MAX_GOALS) -> List[float]:
    probs = [poisson_pmf(k, lam) for k in range(max_goals)]  # 0..3
    tail = max(0.0, 1.0 - sum(probs))
    probs.append(tail)  # 5+
    return probs

def prob_range(probs: List[float], a: int, b: int) -> float:
    if b >= MAX_GOALS:
        return float(sum(probs[a:]))
    return float(sum(probs[a:b + 1]))

def prob_over(probs: List[float], line: float) -> float:
    k_min = int(math.floor(line + 1e-9)) + 1  # over 1.5 -> k>=2
    return float(sum(probs[k_min:])) if k_min < len(probs) else 0.0

def fair_odds(p: float) -> Optional[float]:
    return None if p <= 0 else 1.0 / p

# ---------------- Online: football-data.co.uk ----------------
@st.cache_data(ttl=6 * 60 * 60)
def download_season_csv(season_folder: str, league_code: str = LEAGUE_CODE) -> pd.DataFrame:
    url = f"https://www.football-data.co.uk/mmz4281/{season_folder}/{league_code}.csv"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    content = r.content
    try:
        df = pd.read_csv(pd.io.common.BytesIO(content))
    except Exception:
        df = pd.read_csv(pd.io.common.BytesIO(content), encoding="latin1")
    df["__season_folder"] = season_folder
    return df

def parse_matches(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df["date"] = pd.to_datetime(df.get("Date"), errors="coerce", dayfirst=True)
    df["home"] = df.get("HomeTeam").astype(str)
    df["away"] = df.get("AwayTeam").astype(str)

    if "FTHG" not in df.columns or "FTAG" not in df.columns:
        raise ValueError("CSV non contiene FTHG/FTAG.")
    df["home_goals"] = pd.to_numeric(df["FTHG"], errors="coerce")
    df["away_goals"] = pd.to_numeric(df["FTAG"], errors="coerce")
    df["played"] = df["home_goals"].notna() & df["away_goals"].notna()

    season_folder = df["__season_folder"].iloc[0]
    start_year = 2000 + int(season_folder[:2])
    df["season_folder"] = season_folder
    df["season_start_year"] = start_year

    keep = ["season_start_year", "season_folder", "date", "home", "away", "home_goals", "away_goals", "played"]
    return df[keep]

@st.cache_data(ttl=6 * 60 * 60)
def load_recent_seasons(n_seasons: int = 4) -> pd.DataFrame:
    today = date.today()
    start = _current_start_year(today)
    season_folders = [_season_folder(start - i) for i in range(n_seasons + 1)]
    all_matches = []
    for sf in season_folders:
        try:
            raw = download_season_csv(sf, LEAGUE_CODE)
            all_matches.append(parse_matches(raw))
        except Exception:
            continue
    if not all_matches:
        raise RuntimeError("Non riesco a scaricare le stagioni dal sito football-data.co.uk.")
    return pd.concat(all_matches, ignore_index=True)

@st.cache_data(ttl=60 * 60)
def download_fixtures_csv() -> pd.DataFrame:
    url = "https://www.football-data.co.uk/fixtures.csv"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    content = r.content
    try:
        df = pd.read_csv(pd.io.common.BytesIO(content))
    except Exception:
        df = pd.read_csv(pd.io.common.BytesIO(content), encoding="latin1")
    return df

@dataclass
class Fixture:
    match_date: Optional[pd.Timestamp]
    home: str
    away: str
    is_home: bool

def next_fixture_from_fixtures_csv(team: str) -> Optional[Fixture]:
    df = download_fixtures_csv().copy()
    if not {"Div", "HomeTeam", "AwayTeam"}.issubset(df.columns):
        return None
    df = df[df["Div"].astype(str) == LEAGUE_CODE].copy()
    df["date"] = pd.to_datetime(df.get("Date"), errors="coerce", dayfirst=True)
    df["home"] = df["HomeTeam"].astype(str)
    df["away"] = df["AwayTeam"].astype(str)

    today_ts = pd.Timestamp(datetime.now().date())
    df = df[df["date"].notna() & (df["date"] >= today_ts)].copy()
    if df.empty:
        return None

    mask = df.apply(lambda r: _is_same_team(r["home"], team) or _is_same_team(r["away"], team), axis=1)
    df_team = df[mask].sort_values("date")
    if df_team.empty:
        return None

    row = df_team.iloc[0]
    is_home = _is_same_team(row["home"], team)
    return Fixture(match_date=row["date"], home=row["home"], away=row["away"], is_home=is_home)

def find_next_fixture(matches: pd.DataFrame, team: str) -> Optional[Fixture]:
    df = matches.copy()
    df = df[df["date"].notna()].copy()

    # prima prova: future nel CSV stagionale (quando presenti)
    future = df[~df["played"]].copy()
    if not future.empty:
        mask = future.apply(lambda r: _is_same_team(r["home"], team) or _is_same_team(r["away"], team), axis=1)
        ft = future[mask].sort_values("date")
        if not ft.empty:
            row = ft.iloc[0]
            is_home = _is_same_team(row["home"], team)
            return Fixture(match_date=row["date"], home=row["home"], away=row["away"], is_home=is_home)

    # fallback: fixtures.csv
    return next_fixture_from_fixtures_csv(team)

# ---------------- Excel storico ----------------
@st.cache_data(ttl=24 * 60 * 60)
def load_excel_goals_by_season(excel_path: str, sheet_name: str) -> Dict[str, List[int]]:
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
    season_re = re.compile(r"^\s*(20\d{2})\s*[-/]\s*(20\d{2})\s*$")
    seasons: Dict[str, List[Tuple[int, int]]] = {}
    current_season: Optional[str] = None

    for i in range(len(df)):
        cell = df.iloc[i, 1]
        if isinstance(cell, str) and season_re.match(cell.strip()):
            current_season = cell.strip().replace("/", "-")
            seasons.setdefault(current_season, [])
            continue
        if current_season is None:
            continue
        gday = df.iloc[i, 1]
        gsc = df.iloc[i, 2] if df.shape[1] > 2 else None
        if isinstance(gday, (int, float)) and isinstance(gsc, (int, float)) and not pd.isna(gsc):
            seasons[current_season].append((int(gday), int(gsc)))

    out: Dict[str, List[int]] = {}
    for s, pairs in seasons.items():
        pairs = sorted(pairs, key=lambda x: x[0])
        out[s] = [gs for _, gs in pairs]

    if not out:
        raise ValueError(f"Non riesco a leggere i gol dal foglio Excel '{sheet_name}'.")
    return out

def flatten_seasons(by_season: Dict[str, List[int]]) -> List[int]:
    seq: List[int] = []
    for _, v in by_season.items():
        seq.extend(list(v))
    return seq


def get_current_teams_list() -> List[str]:
    """Elenco squadre Serie A della stagione corrente (da football-data.co.uk)."""
    df = load_current_season_matches()
    teams = pd.unique(pd.concat([df["home"], df["away"]]).dropna())
    teams = sorted({str(t) for t in teams})
    return teams

# ---------------- Stagione corrente (auto) ----------------
@st.cache_data(ttl=3 * 60 * 60)
def load_current_season_matches() -> pd.DataFrame:
    sy = _current_start_year(date.today())
    sf = _season_folder(sy)
    raw = download_season_csv(sf, LEAGUE_CODE)
    return parse_matches(raw)

def resolve_online_team_name(team_from_excel: str, current_matches: pd.DataFrame) -> str:
    teams = pd.unique(pd.concat([current_matches["home"], current_matches["away"]]).dropna())
    for t in teams:
        if _is_same_team(t, team_from_excel):
            return str(t)
    return team_from_excel

def current_season_team_played_matches(team_from_excel: str) -> Tuple[str, pd.DataFrame]:
    df = load_current_season_matches()
    team_online = resolve_online_team_name(team_from_excel, df)
    played = df[df["played"] & df["date"].notna()].copy()
    mask = played.apply(lambda r: _is_same_team(r["home"], team_online) or _is_same_team(r["away"], team_online), axis=1)
    played = played[mask].sort_values("date")
    return team_online, played

def goals_seq_from_played(team_online: str, played_df: pd.DataFrame) -> List[int]:
    goals: List[int] = []
    for _, r in played_df.iterrows():
        if _is_same_team(r["home"], team_online):
            goals.append(int(r["home_goals"]))
        else:
            goals.append(int(r["away_goals"]))
    return goals

def last_included_match_text(played_df: pd.DataFrame) -> str:
    if played_df is None or played_df.empty:
        return "Nessuna partita giocata trovata (stagione corrente)."
    r = played_df.iloc[-1]
    dt = r["date"].strftime("%d/%m/%Y") if pd.notna(r["date"]) else "data n/d"
    return f"{dt} • {r['home']} {int(r['home_goals'])}-{int(r['away_goals'])} {r['away']}"


# ---------------- Affidabilità (risk control) ----------------
def _entropy(probs: List[float]) -> float:
    """Entropia (0=molto certa, alto=incerta)."""
    eps = 1e-12
    return float(-sum(p * math.log(p + eps) for p in probs))

def reliability_assessment(current_n: int, opp_n: int, probs: List[float]) -> Tuple[str, float, Dict[str, float]]:
    """
    Ritorna (label, score 0..1, dettagli).
    Score più alto = più affidabile (meno rischio).
    """
    # componenti semplici e interpretabili
    live_score = min(1.0, current_n / 12.0)         # pieno dopo ~12 gare
    opp_score = min(1.0, opp_n / 12.0)              # pieno dopo ~12 gare
    ent = _entropy(probs)                           # max ~ log(K)
    ent_norm = min(1.0, ent / math.log(len(probs))) # 0..1
    certainty_score = 1.0 - ent_norm                # più alto = più "deciso"

    score = 0.45 * live_score + 0.35 * opp_score + 0.20 * certainty_score

    if score >= 0.70:
        label = "🟢 Verde (affidabile)"
    elif score >= 0.50:
        label = "🟡 Giallo (medio rischio)"
    else:
        label = "🔴 Rosso (alto rischio)"

    details = {
        "live_score": live_score,
        "opp_score": opp_score,
        "certainty_score": certainty_score,
        "score": score,
    }
    return label, score, details

# ---------------- Modello (semplice ma robusto) ----------------
def exp_weighted_mean(seq: List[int], alpha: float) -> float:
    if not seq:
        return 1.25
    w = 1.0
    s = 0.0
    sw = 0.0
    for x in reversed(seq):  # più recenti prima
        s += w * float(x)
        sw += w
        w *= alpha
    return s / sw if sw > 0 else float(pd.Series(seq).mean())

def blended_attack_mean(historic_seq: List[int], current_seq: List[int], w_current: float, alpha_recent: float) -> float:
    m_hist = exp_weighted_mean(historic_seq, alpha_recent) if historic_seq else None
    m_cur = exp_weighted_mean(current_seq, alpha_recent) if current_seq else None
    if m_hist is None and m_cur is None:
        return 1.25
    if m_cur is None:
        return float(m_hist)
    if m_hist is None:
        return float(m_cur)
    return (1.0 - w_current) * float(m_hist) + w_current * float(m_cur)

def build_long_for_defense(matches: pd.DataFrame, cutoff_date: Optional[pd.Timestamp]) -> pd.DataFrame:
    df = matches.copy()
    df = df[df["played"] & df["date"].notna()].copy()
    if cutoff_date is not None and not pd.isna(cutoff_date):
        df = df[df["date"] < cutoff_date].copy()
    home = pd.DataFrame({"date": df["date"], "team": df["home"], "is_home": True, "ga": df["away_goals"]})
    away = pd.DataFrame({"date": df["date"], "team": df["away"], "is_home": False, "ga": df["home_goals"]})
    return pd.concat([home, away], ignore_index=True)


def opponent_sample_size(matches: pd.DataFrame, opponent: str, is_home: bool, cutoff_date: Optional[pd.Timestamp]) -> int:
    long = build_long_for_defense(matches, cutoff_date=cutoff_date)
    if long.empty:
        return 0
    if is_home:
        opp_split = long[(long["team"].apply(lambda x: _is_same_team(x, opponent))) & (~long["is_home"])]
    else:
        opp_split = long[(long["team"].apply(lambda x: _is_same_team(x, opponent))) & (long["is_home"])]
    return int(len(opp_split))

def estimate_lambda(matches: pd.DataFrame, opponent: str, is_home: bool, cutoff_date: Optional[pd.Timestamp],
                    k_smooth: float, team_attack_mean: float, home_adv: float) -> float:
    long = build_long_for_defense(matches, cutoff_date=cutoff_date)
    if long.empty:
        lam = team_attack_mean
    else:
        if is_home:
            opp_split = long[(long["team"].apply(lambda x: _is_same_team(x, opponent))) & (~long["is_home"])]
            league_split = long[~long["is_home"]]
        else:
            opp_split = long[(long["team"].apply(lambda x: _is_same_team(x, opponent))) & (long["is_home"])]
            league_split = long[long["is_home"]]

        league_ga = float(league_split["ga"].mean()) if len(league_split) else 1.25
        opp_ga = float(opp_split["ga"].mean()) if len(opp_split) else league_ga
        n_opp = len(opp_split)

        opp_ga_sm = (opp_ga * n_opp + league_ga * k_smooth) / (n_opp + k_smooth)
        lam = team_attack_mean * (opp_ga_sm / league_ga if league_ga > 0 else 1.0)

    lam = lam * (1.0 + home_adv) if is_home else lam * (1.0 - home_adv)
    return float(max(0.2, min(lam, 4.0)))

# ---------------- Mercati + selezione ----------------
def compute_markets(team_name: str, probs: List[float]) -> pd.DataFrame:
    rows = []
    for a, b in MULTIGOL_RANGES:
        p = prob_range(probs, a, b)
        rows.append({
            "Mercato": f"MULTIGOL {team_name} {a}-{b if b < MAX_GOALS else '5+'}",
            "Esito": f"{a}-{b if b < MAX_GOALS else '5+'}",
            "Prob%": round(p * 100, 1),
            "Quota fair": round(fair_odds(p), 2) if fair_odds(p) else None,
            "_p": p,
        })
    p_over = prob_over(probs, OVER_LINE)
    rows.append({
        "Mercato": f"OVER {team_name} {OVER_LINE}",
        "Esito": f"Over {OVER_LINE}",
        "Prob%": round(p_over * 100, 1),
        "Quota fair": round(fair_odds(p_over), 2) if fair_odds(p_over) else None,
        "_p": p_over,
    })
    return pd.DataFrame(rows).sort_values("_p", ascending=False)

def select_ranked(df: pd.DataFrame, min_prob: float, min_fair_odds: float) -> pd.DataFrame:
    cand = df[(df["_p"] >= float(min_prob)) & df["Quota fair"].notna() & (df["Quota fair"] >= float(min_fair_odds))].copy()
    return cand.sort_values("_p", ascending=False)

# ---------------- UI ----------------
st.set_page_config(page_title="Multigol – Serie A", layout="wide")

# PERSISTENZA quote bookmaker (anche dopo refresh)
if "quotes_store_loaded" not in st.session_state:
    st.session_state["quota_book_map"] = load_quotes_store()
    st.session_state["quotes_store_loaded"] = True
st.title("📊 Multigol & Over 1.5 (gol della squadra) – Serie A")
st.info(f"🟢 Stagione corrente aggiornata automaticamente (football-data.co.uk) | Refresh: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

with st.sidebar:
    st.header("Dati")
    excel_path = st.text_input("Percorso Excel", EXCEL_PATH_DEFAULT)

    st.markdown("---")
    st.subheader("Filtri giocata")
    min_fair_odds = st.slider("Quota fair minima", 1.30, 2.20, 1.50, 0.05)
    min_prob_auto = st.slider("Probabilità minima", 0.50, 0.70, 0.55, 0.01)
    n_suggestions = st.slider("Quanti mercati suggerire", 1, 5, 3, 1)
    st.markdown("---")
    st.subheader("Sicurezza (risk control)")
    if st.button("🧹 Cancella quote salvate"):
        st.session_state["quota_book_map"] = {}
        save_quotes_store({})
        st.success("Quote salvate cancellate.")

    no_bet_mode = st.checkbox("Attiva NO BET automatico", value=True)
    min_reliability = st.slider("Affidabilità minima per consigliare", 0.40, 0.85, 0.55, 0.05)
    min_value = st.slider("Soglia VALUE (quota book / quota fair)", 1.00, 1.15, 1.03, 0.01)
    st.caption("Inserisci la quota bookmaker nei mercati consigliati: l’app segnala OK/NO BET in base al VALUE.")


    st.markdown("---")
    st.subheader("Modello")
    w_current = st.slider("Peso stagione corrente", 0.0, 0.9, 0.6, 0.05)
    alpha_recent = st.slider("Peso RECENTE (più alto=ultime gare contano di più)", 0.50, 0.95, 0.85, 0.01)
    home_adv = st.slider("Vantaggio casa (soft)", 0.00, 0.12, 0.06, 0.01)

    st.markdown("---")
    st.subheader("Difesa avversario")

    n_seasons = st.slider("Stagioni online (per stimare difesa)", 2, 6, 4)
    k_smooth = st.slider("Smoothing difesa avversario", 2.0, 12.0, 6.0, 0.5)


    st.markdown("---")
    with st.expander("ℹ️ Legenda parametri"):
        st.markdown("""
**Filtri giocata**
- **Quota fair minima**: l'app considera "giocabili" solo i mercati con quota fair ≥ questa soglia.
- **Probabilità minima**: l'app considera "giocabili" solo i mercati con probabilità ≥ questa soglia.
- **Quanti mercati suggerire**: quanti mercati mostrare nella tabella *dopo filtri* (Top N).

**Sicurezza (risk control)**
- **NO BET automatico**: se attivo, l’app può bloccare il consiglio quando il rischio è alto.
- **Affidabilità minima**: soglia del semaforo (verde/giallo/rosso). Se lo score è sotto soglia → NO BET.
- **Soglia VALUE**: rapporto **quota bookmaker / quota fair**. Se VALUE ≥ soglia → c’è “valore”; se sotto → NO BET.

**Modello**
- **Peso stagione corrente**: quanto pesano i dati live della stagione in corso rispetto allo storico Excel.
- **Peso RECENTE**: quanto contano di più le ultime partite (più alto = più peso alle più recenti).
- **Vantaggio casa (soft)**: piccolo aggiustamento se la squadra gioca in casa (o in trasferta).

**Difesa avversario**
- **Stagioni online (per stimare difesa)**: quante stagioni usare per stimare quanti gol concede l’avversario.
- **Smoothing difesa avversario**: evita valori “strani” se l’avversario ha poche partite (più alto = più prudente/stabile).
""")


# Online matches (difesa + fixture)
matches_all = load_recent_seasons(n_seasons=n_seasons)

# Excel sheets list
try:
    xls = pd.ExcelFile(excel_path)
    sheets = [s for s in xls.sheet_names if s not in {"Giocate", "Report", "Report Enrico", "Foglio3"}]
except Exception as e:
    st.error(f"Errore Excel: {e}")
    st.stop()

col1, col2 = st.columns([2, 3])

with col1:
    team_sheet = st.selectbox("Squadra (Excel)", sheets)

    # storico excel
    by_season = load_excel_goals_by_season(excel_path, team_sheet)
    historic_seq = flatten_seasons(by_season)

    # live stagione corrente
    try:
        team_online, played_df = current_season_team_played_matches(team_sheet)
        current_seq = goals_seq_from_played(team_online, played_df)
        last_match_txt = last_included_match_text(played_df)
    except Exception as e:
        team_online, current_seq = team_sheet, []
        last_match_txt = "N/D"
        st.warning(f"Live non disponibile: {e}")

    attack_mean = blended_attack_mean(historic_seq, current_seq, float(w_current), float(alpha_recent))

    # fixture: auto -> manuale
    fixture = find_next_fixture(matches_all, team_online)

    st.subheader("Partita considerata")
    if fixture is None:
        st.warning("Prossima partita NON trovata automaticamente (spesso perché la giornata non è ancora chiusa o la fonte non è aggiornata).")
        st.markdown("### Inserimento manuale (temporaneo)")
        teams_list = get_current_teams_list()
        teams_list = [t for t in teams_list if not _is_same_team(t, team_online)]
        manual_opponent = st.selectbox("Avversario (scegli squadra Serie A)", options=[""] + teams_list)
        manual_is_home = st.radio("Campo", ["Casa", "Trasferta"], horizontal=True)
        manual_use_date = st.checkbox("Inserisci anche la data (consigliato)", value=False)
        manual_date = st.date_input("Data partita", value=date.today(), disabled=not manual_use_date)
        if not manual_opponent:
            st.stop()
        fixture = Fixture(
            match_date=pd.Timestamp(manual_date) if manual_use_date else None,
            home=team_online if manual_is_home == "Casa" else manual_opponent,
            away=manual_opponent if manual_is_home == "Casa" else team_online,
            is_home=(manual_is_home == "Casa"),
        )

    dt_str = fixture.match_date.strftime("%d/%m/%Y") if (fixture.match_date is not None and not pd.isna(fixture.match_date)) else "Data non disponibile"
    st.write(f"**{fixture.home} vs {fixture.away}**")
    st.write(f"📅 {dt_str} • {'Casa' if fixture.is_home else 'Trasferta'}")
    st.write(f"**Media gol (storico+live):** {attack_mean:.2f}")
    st.caption(f"Storico Excel: {len(historic_seq)} • Stagione corrente (online): {len(current_seq)} • peso live={w_current:.2f}")
    st.write(f"**Ultima partita inclusa (stagione corrente):** {last_match_txt}")

with col2:
    opponent = fixture.away if fixture.is_home else fixture.home
    lam = estimate_lambda(
        matches=matches_all,
        opponent=opponent,
        is_home=fixture.is_home,
        cutoff_date=fixture.match_date,
        k_smooth=float(k_smooth),
        team_attack_mean=float(attack_mean),
        home_adv=float(home_adv),
    )
    probs = poisson_probs(lam, MAX_GOALS)

    # Risk control: semaforo affidabilità
    opp_n = opponent_sample_size(matches_all, opponent, fixture.is_home, fixture.match_date)
    rel_label, rel_score, rel_details = reliability_assessment(len(current_seq), opp_n, probs)
    st.subheader("🛡️ Affidabilità stima")
    st.write(f"{rel_label}  —  Score: {rel_score:.2f}")
    with st.expander("Dettagli affidabilità"):
        st.write(f"Partite stagione corrente (live): {len(current_seq)}")
        st.write(f"Campione difesa avversario (match utili): {opp_n}")
        st.write(f"Certezza distribuzione (0-1): {rel_details['certainty_score']:.2f}")


    st.subheader(f"Distribuzione gol stimata – {team_online}")
    st.dataframe(
        pd.DataFrame({"Gol": [0, 1, 2, 3, 4, "5+"], "Prob%": [round(p * 100, 1) for p in probs]}),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"λ stimato (con difesa avversario + casa/trasferta): {lam:.2f}")

    df = compute_markets(team_online, probs)
    df_markets = df.drop(columns=["_p"]).copy()
    df_ranked = select_ranked(df, float(min_prob_auto), float(min_fair_odds)).drop(columns=["_p"]).copy()

    st.markdown("---")
    st.subheader("Mercati (ordinati per probabilità)")
    st.dataframe(df_markets, use_container_width=True, hide_index=True)

    st.subheader("🏁 Mercati consigliati (dopo filtri)")
    st.caption(f"Filtri: Prob ≥ {int(min_prob_auto*100)}% • Quota fair ≥ {min_fair_odds:.2f}")

    if df_ranked.empty:
        st.warning("Nessun mercato supera i filtri. Prova ad abbassare Probabilità minima o Quota minima.")
    else:
        # NO BET: se affidabilità bassa, non consigliamo anche se i filtri passano
        if no_bet_mode and (rel_score < float(min_reliability)):
            st.error(f"NO BET: affidabilità troppo bassa (score {rel_score:.2f} < soglia {float(min_reliability):.2f}).")
            st.info("Suggerimento: alza i filtri, aumenta il campione (più gare giocate), oppure attendi l’aggiornamento della fonte.")
        else:
            # Tabella VALUE su TUTTI i mercati analizzati (non solo il consigliato)
            allm = df_markets.copy()

            if "quota_book_map" not in st.session_state:
                st.session_state["quota_book_map"] = {}

            qb_map = st.session_state["quota_book_map"]
            keys = [f"{row['Mercato']}|{row['Esito']}" for _, row in allm.iterrows()]
            allm["Quota book"] = [qb_map.get(k, None) for k in keys]

            allm["Quota book"] = pd.to_numeric(allm["Quota book"], errors="coerce")
            allm["VALUE"] = allm["Quota book"] / allm["Quota fair"]
            allm["Esito VALUE"] = allm["VALUE"].apply(lambda v: "OK" if (pd.notna(v) and v >= float(min_value)) else ("NO BET" if pd.notna(v) else "—"))

            st.markdown("### 💶 Value betting (tutti i mercati)")
            st.markdown("Inserisci la **quota bookmaker** nella colonna *Quota book* per qualsiasi mercato ti interessa. "
                        "L’app calcola il **VALUE** e indica **OK/NO BET**.")

            edited = st.data_editor(
                allm,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "Quota book": st.column_config.NumberColumn("Quota book", min_value=1.01, step=0.01),
                    "VALUE": st.column_config.NumberColumn("VALUE", disabled=True),
                    "Esito VALUE": st.column_config.TextColumn("Esito VALUE", disabled=True),
                },
                disabled=["Mercato", "Esito", "Prob%", "Quota fair", "VALUE", "Esito VALUE"],
                key="value_editor_all",
            )

            # salva quote in sessione + file
            edited_qb = pd.to_numeric(edited["Quota book"], errors="coerce")
            for k, v in zip(keys, edited_qb.tolist()):
                qb_map[k] = (None if (v is None or (isinstance(v, float) and pd.isna(v))) else float(v))
            st.session_state["quota_book_map"] = qb_map
            save_quotes_store(qb_map)

            # ricalcolo
            ed = edited.copy()
            ed["Quota book"] = pd.to_numeric(ed["Quota book"], errors="coerce")
            ed["VALUE"] = ed["Quota book"] / ed["Quota fair"]
            ed["Esito VALUE"] = ed["VALUE"].apply(lambda v: "OK" if (pd.notna(v) and v >= float(min_value)) else ("NO BET" if pd.notna(v) else "—"))

            # Suggerimento finale: se ci sono quote inserite, scegliamo il miglior VALUE
            run_value = st.button("🔍 Valuta VALUE (OK / NO BET)")

            if run_value:
                has_quotes = ed["Quota book"].notna().any()
                if has_quotes:
                    candidates_value = ed[ed["VALUE"].notna() & (ed["VALUE"] >= float(min_value))].copy()
                    if candidates_value.empty:
                        if no_bet_mode:
                            st.error(f"NO BET: nessun mercato ha VALUE ≥ {float(min_value):.2f}.")
                        else:
                            st.warning(f"Nessun mercato con VALUE ≥ {float(min_value):.2f}.")
                    else:
                        best = candidates_value.sort_values("VALUE", ascending=False).iloc[0]
                        st.success(
                            f"👉 CONSIGLIO (VALUE): {best['Mercato']} | Prob {best['Prob%']}% | "
                            f"Quota book {best['Quota book']:.2f} | Quota fair {best['Quota fair']} | VALUE {best['VALUE']:.2f}"
                        )
                else:
                    st.warning("Nessuna quota bookmaker inserita.")
            else:
                st.info("Inserisci le quote bookmaker e poi clicca **Valuta VALUE** per vedere OK/NO BET.")

            best = df_ranked.iloc[0]
        topn = df_ranked.head(int(n_suggestions)).copy()
        st.dataframe(topn, use_container_width=True, hide_index=True)
        best = topn.iloc[0]
        st.success(f"👉 CONSIGLIO: {best['Mercato']} – {best['Esito']} | Prob {best['Prob%']}% | Quota fair {best['Quota fair']}")