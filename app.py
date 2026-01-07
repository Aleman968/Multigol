# -*- coding: utf-8 -*-
"""
Multigol / Over (gol della squadra) – SOLO SERIE A

✅ Fixture automatiche (gratis, senza API key):
   - risultati: football-data.co.uk mmz4281/<season>/I1.csv
   - fixture imminenti: football-data.co.uk/fixtures.csv
✅ Storico gol squadra: dal tuo Excel (MG ITALIA.xlsx), a partire dal 2021/2022.

H2H:
- Gli "ultimi 10 scontri diretti" vengono cercati SEMPRE fino a trovare 10 partite,
  indipendentemente dal numero di stagioni che scegli nel cursore "stagioni online".
  (Se non esistono 10 match disponibili nei dati scaricabili, mostra quelli trovati.)

Impostazioni:
- max gol: 4+ (bucket finale)
- mercati: Over 1.5; Multigol 0-1 / 1-2 / 1-3 / 2-3 / 2-4
- nei mercati mostra il nome della squadra

Avvio:
  python -m pip install -r requirements.txt
  python -m streamlit run app.py
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


EXCEL_PATH_DEFAULT = "MG ITALIA.xlsx"
LEAGUE_CODE = "I1"  # Serie A
REQUEST_TIMEOUT = 20

MAX_GOALS = 4  # 0,1,2,3,4+
MULTIGOL_RANGES = [(0, 1), (1, 2), (1, 3), (2, 3), (2, 4)]
OVER_LINE = 1.5

# H2H settings
H2H_TARGET_N = 10
H2H_MAX_SEASONS_SCAN = 12  # scan indietro fino a 12 stagioni per trovare 10 H2H


TEAM_ALIASES: Dict[str, List[str]] = {
    "Juve": ["Juventus", "Juventus FC", "Juve"],
    "Inter": ["Inter", "Inter Milan", "Internazionale", "FC Internazionale"],
    "Milan": ["AC Milan", "Milan", "A.C. Milan"],
    "Ata": ["Atalanta", "Atalanta BC", "Ata"],
    "Roma": ["Roma", "AS Roma"],
    "Lazio": ["Lazio", "SS Lazio"],
    "Bolo": ["Bologna", "Bologna FC", "Bologna 1909", "Bolo"],
    "Nap": ["Napoli", "Nap"],
}


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
    for _, vals in TEAM_ALIASES.items():
        nvals = {_norm_team(v) for v in vals}
        if na in nvals and nb in nvals:
            return True
    return False


# ---------- Poisson ----------
def poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def poisson_probs(lam: float, max_goals: int = MAX_GOALS) -> List[float]:
    probs = [poisson_pmf(k, lam) for k in range(max_goals + 1)]
    tail = max(0.0, 1.0 - sum(probs))
    probs[-1] += tail
    return probs


def prob_range(probs: List[float], a: int, b: int) -> float:
    a = max(0, a)
    b = min(b, len(probs) - 1)
    if a > b:
        return 0.0
    return float(sum(probs[a:b+1]))


def prob_over(probs: List[float], line: float) -> float:
    k_min = int(math.floor(line + 1e-9)) + 1
    return float(sum(probs[k_min:]))


def fair_odds(p: float) -> Optional[float]:
    return None if p <= 0 else 1.0 / p


# ---------- Online data ----------
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
        raise RuntimeError("Non riesco a scaricare stagioni dal sito.")
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
    future = df[~df["played"]].copy()
    if not future.empty:
        mask = future.apply(lambda r: _is_same_team(r["home"], team) or _is_same_team(r["away"], team), axis=1)
        ft = future[mask].sort_values("date")
        if not ft.empty:
            row = ft.iloc[0]
            is_home = _is_same_team(row["home"], team)
            return Fixture(match_date=row["date"], home=row["home"], away=row["away"], is_home=is_home)
    return next_fixture_from_fixtures_csv(team)


# ---------- Excel team goals ----------
@st.cache_data(ttl=24 * 60 * 60)
def load_excel_team_goals(excel_path: str, sheet_name: str) -> pd.Series:
    df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
    goals: List[float] = []
    season_re = re.compile(r"^\s*(20\d{2})\s*[-/]\s*(20\d{2})\s*$")
    current = None

    for i in range(len(df)):
        cell = df.iloc[i, 1]  # colonna B
        if isinstance(cell, str) and season_re.match(cell.strip()):
            current = cell.strip()
            continue
        gday = df.iloc[i, 1]
        gsc = df.iloc[i, 2] if df.shape[1] > 2 else None
        if current is not None:
            if isinstance(gday, (int, float)) and isinstance(gsc, (int, float)) and not pd.isna(gsc):
                goals.append(float(gsc))

    if not goals:
        raise ValueError(f"Non riesco a leggere i gol dal foglio Excel '{sheet_name}'.")
    return pd.Series(goals, name="goals")


def excel_attack_mean(excel_path: str, sheet_name: str) -> float:
    return float(load_excel_team_goals(excel_path, sheet_name).mean())


# ---------- Defense & H2H helpers ----------
def build_long_for_defense(matches: pd.DataFrame, cutoff_date: Optional[pd.Timestamp]) -> pd.DataFrame:
    df = matches.copy()
    df = df[df["played"] & df["date"].notna()].copy()
    if cutoff_date is not None and not pd.isna(cutoff_date):
        df = df[df["date"] < cutoff_date].copy()

    home = pd.DataFrame({"date": df["date"], "team": df["home"], "is_home": True, "ga": df["away_goals"]})
    away = pd.DataFrame({"date": df["date"], "team": df["away"], "is_home": False, "ga": df["home_goals"]})
    return pd.concat([home, away], ignore_index=True)


def estimate_lambda_base(
    matches: pd.DataFrame,
    excel_path: str,
    excel_sheet: str,
    opponent: str,
    is_home: bool,
    cutoff_date: Optional[pd.Timestamp],
    k_smooth: float,
) -> float:
    team_gf_excel = excel_attack_mean(excel_path, excel_sheet)
    long = build_long_for_defense(matches, cutoff_date=cutoff_date)

    if long.empty:
        return float(max(0.2, min(team_gf_excel, 4.0)))

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

    lam = team_gf_excel * (opp_ga_sm / league_ga if league_ga > 0 else 1.0)
    return float(max(0.2, min(lam, 4.0)))


def _filter_h2h_rows(matches: pd.DataFrame, team: str, opponent: str, cutoff_date: Optional[pd.Timestamp]) -> pd.DataFrame:
    df = matches.copy()
    df = df[df["played"] & df["date"].notna()].copy()
    if cutoff_date is not None and not pd.isna(cutoff_date):
        df = df[df["date"] < cutoff_date].copy()

    mask = df.apply(
        lambda r: (_is_same_team(r["home"], team) and _is_same_team(r["away"], opponent)) or
                  (_is_same_team(r["home"], opponent) and _is_same_team(r["away"], team)),
        axis=1,
    )
    return df[mask].copy()


@st.cache_data(ttl=6 * 60 * 60)
def get_last_h2h_10(team: str, opponent: str, cutoff_iso: str) -> pd.DataFrame:
    """
    Cerca SEMPRE fino a trovare 10 H2H (se possibile), scansionando stagioni indietro.
    cutoff_iso: stringa (cache-friendly)
    """
    cutoff_date = pd.Timestamp(cutoff_iso) if cutoff_iso else None

    start = _current_start_year(date.today())
    collected = []

    for i in range(H2H_MAX_SEASONS_SCAN):
        sf = _season_folder(start - i)
        try:
            raw = download_season_csv(sf, LEAGUE_CODE)
            season_matches = parse_matches(raw)
        except Exception:
            continue

        h2h_part = _filter_h2h_rows(season_matches, team, opponent, cutoff_date)
        if not h2h_part.empty:
            collected.append(h2h_part)

        if collected:
            h2h_all = pd.concat(collected, ignore_index=True)
            h2h_all = h2h_all.sort_values("date", ascending=False)
            if len(h2h_all) >= H2H_TARGET_N:
                return h2h_all.head(H2H_TARGET_N).copy()

    if collected:
        h2h_all = pd.concat(collected, ignore_index=True).sort_values("date", ascending=False)
        return h2h_all.head(H2H_TARGET_N).copy()
    return pd.DataFrame(columns=["season_start_year", "season_folder", "date", "home", "away", "home_goals", "away_goals", "played"])


def h2h_team_goals_mean(h2h: pd.DataFrame, team: str) -> Optional[float]:
    if h2h.empty:
        return None

    def team_goals_row(r):
        if _is_same_team(r["home"], team):
            return float(r["home_goals"])
        if _is_same_team(r["away"], team):
            return float(r["away_goals"])
        return float("nan")

    goals = h2h.apply(team_goals_row, axis=1).dropna()
    if goals.empty:
        return None
    return float(goals.mean())


def estimate_lambda_with_h2h(
    matches_for_defense: pd.DataFrame,
    excel_path: str,
    excel_sheet: str,
    team_display: str,
    opponent: str,
    is_home: bool,
    cutoff_date: Optional[pd.Timestamp],
    k_smooth_def: float,
    use_h2h: bool,
    h2h_weight_max: float,
) -> Tuple[float, pd.DataFrame, Optional[float], float]:
    lam_base = estimate_lambda_base(
        matches=matches_for_defense,
        excel_path=excel_path,
        excel_sheet=excel_sheet,
        opponent=opponent,
        is_home=is_home,
        cutoff_date=cutoff_date,
        k_smooth=k_smooth_def,
    )

    if not use_h2h:
        return lam_base, pd.DataFrame(), None, 0.0

    cutoff_iso = cutoff_date.isoformat() if (cutoff_date is not None and not pd.isna(cutoff_date)) else ""
    h2h = get_last_h2h_10(team_display, opponent, cutoff_iso)
    h2h_mean = h2h_team_goals_mean(h2h, team_display)

    if h2h_mean is None:
        return lam_base, h2h, None, 0.0

    n = len(h2h)
    w_eff = min(h2h_weight_max, n / 20.0)  # 10 match => 0.5 (poi clamp)
    lam_final = (1.0 - w_eff) * lam_base + w_eff * float(max(0.2, min(h2h_mean, 4.0)))
    return float(max(0.2, min(lam_final, 4.0))), h2h, float(h2h_mean), float(w_eff)


# ---------- UI ----------
st.set_page_config(page_title="Multigol – Serie A", layout="wide")
st.title("📊 Multigol & Over 1.5 (gol della squadra) – Serie A")
st.caption("Storico gol squadra dal tuo Excel + avversario/fixture online. H2H sempre a 10 (se disponibili).")

with st.sidebar:
    st.header("Dati")
    excel_path = st.text_input("Percorso Excel", EXCEL_PATH_DEFAULT)
    st.caption("Lascia 'MG ITALIA.xlsx' se il file è nella stessa cartella dell'app.")
    n_seasons = st.slider("Quante stagioni online usare (solo per difesa avversario)", 2, 6, 4)
    k_smooth = st.slider("Smoothing difesa avversario", 2.0, 12.0, 6.0, 0.5)

    st.markdown("---")
    st.header("Scontri diretti (H2H)")
    use_h2h = st.checkbox("Considera ultimi 10 scontri diretti", value=True)
    h2h_weight_max = st.slider("Peso massimo H2H (0.5 = forte)", 0.0, 0.5, 0.25, 0.05)

    st.markdown("---")
    st.header("Fixture")
    show_debug = st.checkbox("Mostra diagnostica fixture", value=False)

try:
    matches_all = load_recent_seasons(n_seasons=n_seasons)
except Exception as e:
    st.error(f"Errore caricamento dati online: {e}")
    st.stop()

try:
    xls = pd.ExcelFile(excel_path)
    excel_sheets = xls.sheet_names
except Exception as e:
    st.error(f"Non riesco ad aprire l'Excel '{excel_path}': {e}")
    st.stop()

exclude = {"Giocate", "Report", "Report Enrico", "Foglio3"}
team_sheets = [s for s in excel_sheets if s not in exclude]

col1, col2 = st.columns([2, 3])

with col1:
    excel_sheet = st.selectbox("Seleziona squadra (dal tuo Excel)", team_sheets)
    team_name = excel_sheet

    fixture = find_next_fixture(matches_all, team_name)
    if fixture is None:
        st.warning("Non ho trovato la prossima partita automaticamente. Puoi inserirla manualmente qui sotto.")
        teams_online = sorted(pd.unique(pd.concat([matches_all["home"], matches_all["away"]]).dropna()), key=_norm_team)
        opponents = [t for t in teams_online if not _is_same_team(t, team_name)]
        opponent_manual = st.selectbox("Seleziona avversario", opponents)
        home_away = st.radio(f"{team_name} gioca:", ["In casa", "In trasferta"], horizontal=True)
        is_home_manual = home_away == "In casa"
        match_date_manual = st.date_input("Data partita", value=date.today())
        fixture = Fixture(
            match_date=pd.Timestamp(match_date_manual),
            home=team_name if is_home_manual else opponent_manual,
            away=opponent_manual if is_home_manual else team_name,
            is_home=is_home_manual,
        )

        if show_debug:
            fx = download_fixtures_csv()
            if "Div" in fx.columns:
                fx_i1 = fx[fx["Div"].astype(str) == LEAGUE_CODE].copy()
                fx_i1["date"] = pd.to_datetime(fx_i1.get("Date"), errors="coerce", dayfirst=True)
                today_ts = pd.Timestamp(datetime.now().date())
                fx_i1 = fx_i1[fx_i1["date"].notna() & (fx_i1["date"] >= today_ts)].copy()
                st.write("Fixture future trovate per Div=I1:", len(fx_i1))
                st.dataframe(fx_i1.head(30), use_container_width=True)

    st.subheader("Partita considerata")
    dt_str = fixture.match_date.strftime("%d/%m/%Y") if fixture.match_date is not None and not pd.isna(fixture.match_date) else "Data non disponibile"
    st.write(f"**{fixture.home} vs {fixture.away}**")
    st.write(f"📅 {dt_str}  •  {'Casa' if fixture.is_home else 'Trasferta'}")

with col2:
    opponent = fixture.away if fixture.is_home else fixture.home

    try:
        lam, h2h_df, h2h_mean, w_eff = estimate_lambda_with_h2h(
            matches_for_defense=matches_all,
            excel_path=excel_path,
            excel_sheet=excel_sheet,
            team_display=team_name,
            opponent=opponent,
            is_home=fixture.is_home,
            cutoff_date=fixture.match_date,
            k_smooth_def=float(k_smooth),
            use_h2h=bool(use_h2h),
            h2h_weight_max=float(h2h_weight_max),
        )
    except Exception as e:
        st.error(f"Errore nel calcolo λ: {e}")
        st.stop()

    probs = poisson_probs(lam, max_goals=MAX_GOALS)

    st.subheader(f"Distribuzione gol stimata – {team_name}")
    df_dist = pd.DataFrame(
        {"Gol": [0, 1, 2, 3, "4+"], "Prob%": [round(p * 100, 1) for p in probs[:-1]] + [round(probs[-1] * 100, 1)]}
    )
    st.dataframe(df_dist, use_container_width=True, hide_index=True)

    if use_h2h:
        if h2h_mean is None:
            st.caption(f"λ finale = **{lam:.2f}**. H2H: non abbastanza dati disponibili.")
        else:
            st.caption(
                f"λ base (Excel+difesa) miscelato con H2H: peso effettivo **{w_eff:.2f}**. "
                f"H2H disponibili: **{len(h2h_df)}** / {H2H_TARGET_N}. "
                f"Media gol {team_name} in H2H = **{h2h_mean:.2f}**.  λ finale = **{lam:.2f}**."
            )
            with st.expander("Mostra ultimi 10 scontri diretti (H2H)"):
                show = h2h_df.copy()
                if not show.empty:
                    show["Data"] = show["date"].dt.strftime("%d/%m/%Y")
                    show = show[["Data", "home", "away", "home_goals", "away_goals"]].rename(
                        columns={"home": "Casa", "away": "Trasferta", "home_goals": "Gol Casa", "away_goals": "Gol Trasferta"}
                    )
                    st.dataframe(show, use_container_width=True, hide_index=True)
                else:
                    st.write("Nessun H2H trovato nei dati scaricabili.")
    else:
        st.caption(f"λ stimato = **{lam:.2f}**. Max bucket: 4+.")

# Mercati
markets = []
for a, b in MULTIGOL_RANGES:
    p = prob_range(probs, a, 4 if b == 4 else b)
    markets.append(
        {"Mercato": f"MULTIGOL {team_name}", "Esito": f"{a}-{b if b < 4 else '4+'}", "Prob%": round(p * 100, 1), "Quota fair": round(fair_odds(p), 2) if fair_odds(p) else None, "_p": p}
    )

p_over = prob_over(probs, OVER_LINE)
markets.append({"Mercato": f"OVER {team_name}", "Esito": f"Over {OVER_LINE}", "Prob%": round(p_over * 100, 1), "Quota fair": round(fair_odds(p_over), 2) if fair_odds(p_over) else None, "_p": p_over})

df_markets = pd.DataFrame(markets).sort_values("Prob%", ascending=False)

st.markdown("---")
st.subheader("Mercati calcolati (ordinati per probabilità)")
st.dataframe(df_markets[["Mercato", "Esito", "Prob%", "Quota fair"]], use_container_width=True, hide_index=True)

st.subheader("🏁 Mercato più probabile")
best = df_markets.iloc[0]
st.success(f"👉 {best['Mercato']} – {best['Esito']}  |  Prob: {best['Prob%']}%  |  Quota fair: {best['Quota fair']}")
