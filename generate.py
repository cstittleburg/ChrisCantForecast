#!/usr/bin/env python3
"""
South Region KPI Dashboard Generator
Usage: python3 generate.py
Output: dashboard.html  (open in any browser, no server needed)
"""

import pandas as pd
import json
import math
import re
import subprocess
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent

# ── Helpers ──────────────────────────────────────────────────────────────────

def wilson_ci(p, n, z=1.96):
    """Wilson score 95% confidence interval for a proportion."""
    if n == 0 or p is None:
        return [None, None]
    denom = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    half = (z / denom) * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2))
    return [round(max(0.0, center - half), 4), round(min(1.0, center + half), 4)]

def to_float(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, str):
        val = val.strip().rstrip('%').replace(',', '').replace('$', '').replace('"', '')
        if val == '' or val == '-':
            return None
        try:
            f = float(val)
            return f / 100 if '%' in str(val) else f
        except ValueError:
            return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def pct(val):
    """Parse a percentage string like '73.60%' → 0.736, or pass-through a float."""
    if isinstance(val, str) and '%' in val:
        return float(val.strip().rstrip('%')) / 100
    return to_float(val)

def usd(val):
    if isinstance(val, str):
        return float(re.sub(r'[,$"\s]', '', val) or 0)
    return to_float(val) or 0.0

# ── Lead source classification ────────────────────────────────────────────────
# Clinic-gen: high-confidence internal sources per regional manager guidance
# External: marketing-driven channels
# Uncertain: ambiguous — flagged for cross-check

CLINIC_GEN = {
    'Walk In', 'WOM Referral', 'Medical Referral',
    'Service Conversion', 'Fitting Fee', 'Wellness Referral', 'Events'
}
EXTERNAL = {
    'DM', 'Direct Web', 'Display', 'Email', 'Inbound Call', 'Outbound Call',
    'Paid Search', 'SEO', 'Social', 'TV', 'Affiliate',
    'SMS / Text Message', 'Door Drop', 'Print Ad'
}

def classify(cat):
    if cat in CLINIC_GEN:   return 'clinic'
    if cat in EXTERNAL:     return 'external'
    return 'uncertain'

# ── 1. Reporting Dimension (JAN-MAR aggregated by lead source) ────────────────
# Columns used: A=category, B=contracted_rev, H=tests_sched, J=show_rate, L=close_rate
# All other columns ignored per definitions

rd_raw = pd.read_excel(BASE / 'JAN-MAR Reporting Dimension.xlsx', header=0)
rd_raw.columns = list('ABCDEFGHIJKLMNOPQRS')[:len(rd_raw.columns)]

# Separate total row (blank category) from detail rows
rd_total = rd_raw[rd_raw['A'].isna()].iloc[0]
rd = rd_raw[rd_raw['A'].notna()].copy()

lead_sources = []
for _, row in rd.iterrows():
    cat     = str(row['A']).strip()
    n       = int(row['H'])   if pd.notna(row['H']) else 0
    show    = float(row['J']) if pd.notna(row['J']) else None
    close   = float(row['L']) if pd.notna(row['L']) else None
    rev     = float(row['B']) if pd.notna(row['B']) else 0.0   # contracted
    inv_rev = float(row['C']) if pd.notna(row['C']) else 0.0   # invoiced
    sdf_pct = float(row['M']) if pd.notna(row['M']) else None  # same-day-fit %
    comps   = round(n * show) if (show is not None and n > 0) else 0

    lead_sources.append({
        'category':    cat,
        'group':       classify(cat),
        'tests':       n,
        'show_rate':   round(show,  4) if show  is not None else None,
        'show_ci':     wilson_ci(show,  n)     if show  is not None else [None, None],
        'close_rate':  round(close, 4) if close is not None else None,
        'close_ci':    wilson_ci(close, comps) if close is not None else [None, None],
        'revenue':     round(rev, 2),       # contracted revenue
        'inv_revenue': round(inv_rev, 2),   # invoiced revenue
        'sdf_pct':     round(sdf_pct, 4) if sdf_pct is not None else None,
        'inv_ratio':   round(inv_rev / rev, 4) if rev > 0 else None,
    })

lead_sources.sort(key=lambda x: x['tests'], reverse=True)

# Region totals from the aggregate row
tot_tests    = int(rd_total['H'])   if pd.notna(rd_total['H']) else 0
tot_show     = float(rd_total['J']) if pd.notna(rd_total['J']) else 0.0
tot_close    = float(rd_total['L']) if pd.notna(rd_total['L']) else 0.0
tot_rev      = float(rd_total['B']) if pd.notna(rd_total['B']) else 0.0   # contracted
tot_invoiced = float(rd_total['C']) if pd.notna(rd_total['C']) else 0.0   # invoiced
tot_sdf      = float(rd_total['M']) if pd.notna(rd_total['M']) else None   # same-day-fit %
tot_units    = int(rd_total['N'])   if pd.notna(rd_total['N']) else 0
tot_asp      = float(rd_total['O']) if pd.notna(rd_total['O']) else 0.0

# ── 2. Appointment Journey CSV (clinic-level aggregated) ──────────────────────
# Columns (0-indexed):
#  0=clinic, 1=tests_sched, 9=show_confirm, 10=show_nonconfirm,
#  11=comp_rate, 12=show_total, 15=ha_units, 16=ha_asp, 17=revenue

aj_raw = pd.read_csv(BASE / 'test_appointment_journey_JAN-MAR.csv')
# Drop total row (first column empty/blank)
aj = aj_raw[aj_raw.iloc[:, 0].notna() & (aj_raw.iloc[:, 0].astype(str).str.strip() != '')].copy()

clinics = []
for _, row in aj.iterrows():
    name = str(row.iloc[0]).strip()
    n    = int(row.iloc[1])
    sr   = pct(row.iloc[12])
    src  = pct(row.iloc[9])
    srnc = pct(row.iloc[10])
    cr   = pct(row.iloc[11])
    units = int(row.iloc[15])
    asp_v = usd(str(row.iloc[16]))
    rev_v = usd(str(row.iloc[17]))

    clinics.append({
        'name':               name,
        'state':              name[:2],
        'tests':              n,
        'show_rate':          round(sr,   4) if sr   is not None else None,
        'show_ci':            wilson_ci(sr, n) if sr is not None else [None, None],
        'show_confirm':       round(src,  4) if src  is not None else None,
        'show_nonconfirm':    round(srnc, 4) if srnc is not None else None,
        'comp_rate':          round(cr,   4) if cr   is not None else None,
        'ha_units':           units,
        'asp':                asp_v,
        'revenue':            rev_v,
    })

clinics.sort(key=lambda x: x['show_rate'] or 0, reverse=True)

# ── 3. Sales Funnel (appointment-level, used for segmentation) ────────────────
# Columns used (positional, 0-indexed):
#  2=Clinic name, 6=Historical Contact Type Code 2 (patient_type),
#  8=Start Date, 11=Appointment status, 13=Appointment outcome,
#  14=Created-by logged role, 16=Campaign Activity code and name
# Clinic-gen creator roles: Dispenser, Clinic Assistant

sf_path = sorted(BASE.glob('Focused Sales Funnel*.xlsx'))[0]
sf_raw  = pd.read_excel(sf_path, header=0)
sf = sf_raw.iloc[:, [2, 3, 6, 11, 13, 14, 16]].copy()
sf.columns = ['clinic', 'provider', 'patient_type',
              'appt_status', 'outcome', 'created_by', 'campaign']

sf['showed']       = sf['appt_status'] == 'Completed'
sf['sold']         = sf['outcome'] == 'Sold'
sf['is_clinic_gen'] = sf['created_by'].isin(['Dispenser', 'Clinic Assistant'])

EXCL = {'Medical Referral', 'Wax Referral', 'Not Tested'}

def segment_stats(subset):
    n       = len(subset)
    showed  = int(subset['showed'].sum())
    sold    = int(subset['sold'].sum())
    sr      = showed / n if n > 0 else 0
    aidable = subset[subset['showed'] & ~subset['outcome'].isin(EXCL)]
    na      = len(aidable)
    cr      = sold / na if na > 0 else 0
    return {
        'count':      n,
        'showed':     showed,
        'show_rate':  round(sr, 4),
        'show_ci':    wilson_ci(sr, n),
        'sold':       sold,
        'aidable':    na,
        'close_rate': round(cr, 4),
        'close_ci':   wilson_ci(cr, na),
    }

origin = {
    'Clinic-Generated': segment_stats(sf[sf['is_clinic_gen']]),
    'External':         segment_stats(sf[~sf['is_clinic_gen']]),
}

patients = []
for pt in ['New Prospect', 'Customer', 'Database Prospect']:
    s = segment_stats(sf[sf['patient_type'] == pt])
    s['type'] = pt
    patients.append(s)

# ── Cross-check ───────────────────────────────────────────────────────────────
# Compare Reporting Dimension category-level classification vs
# Sales Funnel creator-role classification (independent signals)
rd_clinic   = sum(x['tests'] for x in lead_sources if x['group'] == 'clinic')
rd_external = sum(x['tests'] for x in lead_sources if x['group'] == 'external')
rd_uncertain= sum(x['tests'] for x in lead_sources if x['group'] == 'uncertain')
sf_n        = len(sf)
sf_clinic_pct   = round(sf['is_clinic_gen'].sum() / sf_n, 4)
sf_external_pct = round((~sf['is_clinic_gen']).sum() / sf_n, 4)

# Reporting Dimension proportions (of known-classified tests only)
rd_known = rd_clinic + rd_external
rd_clinic_pct   = round(rd_clinic   / rd_known, 4) if rd_known else None
rd_external_pct = round(rd_external / rd_known, 4) if rd_known else None

cross_check = {
    'rd_clinic_tests':    rd_clinic,
    'rd_external_tests':  rd_external,
    'rd_uncertain_tests': rd_uncertain,
    'rd_clinic_pct':      rd_clinic_pct,
    'rd_external_pct':    rd_external_pct,
    'sf_sample':          sf_n,
    'sf_clinic_pct':      sf_clinic_pct,
    'sf_external_pct':    sf_external_pct,
    'alignment_note': (
        f"RD category split (excl. uncertain): {round(rd_clinic_pct*100,1) if rd_clinic_pct else '?'}% clinic / "
        f"{round(rd_external_pct*100,1) if rd_external_pct else '?'}% external. "
        f"SF creator-role split (n={sf_n} sample): {round(sf_clinic_pct*100,1)}% clinic / "
        f"{round(sf_external_pct*100,1)}% external."
    )
}

# ── Forecast rates (embedded into dashboard for weekly prediction engine) ─────

# Keyword rules: campaign name → RD category
CAMPAIGN_RULES = [
    (r'walk.?in|walk in|clinic only|general.*clinic',               'Walk In'),
    (r'physician|medical ref',                                       'Medical Referral'),
    (r'patient.?ref|referral.*(patient|wom)|word of mouth',         'WOM Referral'),
    (r'wellness',                                                    'Wellness Referral'),
    (r'fitting fee',                                                 'Fitting Fee'),
    (r'\bevent',                                                     'Events'),
    (r'service.?conv|conversion',                                    'Service Conversion'),
    (r'direct.?mail|OOW|out.?of.?warrant|DM follow|TNS.*direct',   'DM'),
    (r'google|paid.?search',                                         'Paid Search'),
    (r'social media|facebook|instagram',                             'Social'),
    (r'organic.?search|\bseo\b',                                     'SEO'),
    (r'verified digital|hearinglife.*website|digital.*website|internet search|digital.*search|AG7Z', 'Direct Web'),
    (r'\bweb\b',                                                     'Direct Web'),
    (r'insurance|inbound',                                           'Inbound Call'),
    (r'PSC|TNS(?!.*direct)|recall|proactive|follow.?up|no.?show.*cancel|outbound', 'Outbound Call'),
    (r'\bdisplay\b',                                                 'Display'),
    (r'\bemail\b',                                                   'Email'),
    (r'\bTV\b|television|cable',                                     'TV'),
    (r'affiliate',                                                   'Affiliate'),
    (r'sms|text message',                                            'SMS / Text Message'),
    (r'door drop',                                                   'Door Drop'),
    (r'print|newspaper|magazine',                                    'Print Ad'),
    (r'service(?!.*conv)',                                           'Walk In'),
]

def campaign_to_cat(name):
    if not name or str(name) == 'nan': return None
    for pattern, cat in CAMPAIGN_RULES:
        if re.search(pattern, str(name), re.IGNORECASE): return cat
    return None

# Build lookup from all campaign codes seen in historical Sales Funnel
campaign_map = {}
for c in sf_raw.iloc[:, 16].dropna().unique():
    campaign_map[str(c)] = campaign_to_cat(str(c))

# Aidable rate — explicit funnel parameters (per regional manager definition)
# Shows → TEST_RATE get a hearing test → AIDABLE_OF_TESTED have aidable hearing loss
TEST_RATE         = 0.80   # % of shows who receive a hearing test
AIDABLE_OF_TESTED = 0.90   # % of tested patients with aidable hearing loss
aidable_rate      = round(TEST_RATE * AIDABLE_OF_TESTED, 4)   # 0.72

# SF-observed rate kept for reference (differs from formula: data includes some
# misclassified non-aidable outcomes as aidable and vice versa)
_showed  = sf['showed'].sum()
_aidable = (sf['showed'] & ~sf['outcome'].isin(EXCL)).sum()
aidable_rate_observed = round(_aidable / _showed, 4) if _showed > 0 else 0.84

# Region-wide confirmed / non-confirmed show rates from appointment journey totals row
_aj_tot        = aj_raw[aj_raw.iloc[:, 0].isna() | (aj_raw.iloc[:, 0].astype(str).str.strip() == '')].iloc[-1]
region_conf    = pct(_aj_tot.iloc[9])  or 0.789
region_nonconf = pct(_aj_tot.iloc[10]) or 0.379
region_aj_show = pct(_aj_tot.iloc[12]) or 0.671

# ── ha_per_sale: calibrated to contracted revenue ────────────────────────────
# Back-calculated so funnel model (shows × eff_aidable × close × ha × ASP)
# exactly matches full-period contracted revenue.
# Result: ~1.773 HA/sale (lower than the 1.9 binaural target because some sales
# are monaural and some contracted units haven't invoiced yet in the period).
_shows_est       = tot_tests * tot_show
_est_sales       = _shows_est * aidable_rate * tot_close
ha_per_sale_act  = round(tot_rev / (_est_sales * tot_asp), 4) if (_est_sales > 0 and tot_asp > 0) else 1.9

# ── EV/test: contracted_revenue ÷ tests_scheduled per category ────────────────
# More robust revenue predictor than funnel model — directly observed $/test
# from the Reporting Dimension (avoids compounding close-rate variability)
ev_per_test = {}
for ls in lead_sources:
    if ls['tests'] > 0 and ls['revenue'] > 0:
        ev_per_test[ls['category']] = round(ls['revenue'] / ls['tests'], 2)

# Group-level EV/test fallbacks (clinic-gen, external, overall)
ev_by_group = {}
for _grp in ('clinic', 'external'):
    _grp_tests = sum(ls['tests']   for ls in lead_sources if ls['group'] == _grp)
    _grp_rev   = sum(ls['revenue'] for ls in lead_sources if ls['group'] == _grp)
    ev_by_group[_grp] = round(_grp_rev / _grp_tests, 2) if _grp_tests > 0 else 0
ev_by_group['overall'] = round(tot_rev / tot_tests, 2) if tot_tests > 0 else 0

rates = {
    # Per-category show & close rates (from Reporting Dimension)
    'categories': {
        ls['category']: {
            'show':  ls['show_rate'],
            'close': ls['close_rate'],
            'group': ls['group'],
            'n':     ls['tests'],
        }
        for ls in lead_sources if ls['show_rate'] is not None
    },
    # Per-clinic confirmed / non-confirmed show rates (from Appointment Journey CSV)
    'clinics': {
        c['name']: {
            'confirmed':    c['show_confirm'],
            'nonconfirmed': c['show_nonconfirm'],
            'total':        c['show_rate'],
            'n':            c['tests'],
        }
        for c in clinics
    },
    # Region-wide fallback rates
    'region': {
        'confirmed':    round(region_conf,    4),
        'nonconfirmed': round(region_nonconf, 4),
        'show':         round(region_aj_show, 4),
        'close':        round(tot_close,      4),
    },
    # Creator-role aggregate rates (from Sales Funnel sample)
    'origin': {
        'clinic':   {'show': origin['Clinic-Generated']['show_rate'], 'close': origin['Clinic-Generated']['close_rate']},
        'external': {'show': origin['External']['show_rate'],         'close': origin['External']['close_rate']},
    },
    'aidable_rate':      aidable_rate,           # 0.72 = test_rate × aidable_of_tested
    'aidable_of_tested': AIDABLE_OF_TESTED,       # 0.90 (% of tested with aidable loss)
    'test_rate':         TEST_RATE,               # 0.80 (% of shows who get tested)
    'asp':               round(tot_asp, 2),
    'ha_per_sale':       ha_per_sale_act,         # ~1.773 calibrated to contracted rev
    'ev_per_test':       ev_per_test,
    'ev_per_group':      ev_by_group,
    'campaign_map':      campaign_map,
}

# ── Statistical helpers (no external deps) ───────────────────────────────────

def z_test_props(p1, n1, p2, n2):
    """Two-proportion z-test → (z, p_value) two-tailed."""
    if not all([n1, n2]): return None, None
    p_pool = (p1*n1 + p2*n2) / (n1+n2)
    if p_pool in (0, 1): return None, None
    se = math.sqrt(p_pool*(1-p_pool)*(1/n1+1/n2))
    if se == 0: return None, None
    z = (p1-p2)/se
    pval = math.erfc(abs(z)/math.sqrt(2))
    return round(z,3), round(pval,4)

def odds_ratio(p1, n1, p2, n2):
    """OR with 95% CI via log-odds method."""
    a,b = p1*n1, (1-p1)*n1
    c,d = p2*n2, (1-p2)*n2
    if min(a,b,c,d) < 1: return None, None, None
    OR = (a/b)/(c/d)
    hw = 1.96*math.sqrt(1/a+1/b+1/c+1/d)
    return round(OR,2), round(math.exp(math.log(OR)-hw),2), round(math.exp(math.log(OR)+hw),2)

def pearson_r_binary(x_num, y_bin):
    """Pearson r between a numeric and a binary series."""
    pairs = [(x,y) for x,y in zip(x_num,y_bin) if x is not None and not math.isnan(x)]
    n = len(pairs)
    if n < 5: return None, None
    xm = sum(p[0] for p in pairs)/n
    ym = sum(p[1] for p in pairs)/n
    num = sum((p[0]-xm)*(p[1]-ym) for p in pairs)
    dxsq = sum((p[0]-xm)**2 for p in pairs)
    dysq = sum((p[1]-ym)**2 for p in pairs)
    denom = math.sqrt(dxsq*dysq)
    if denom == 0: return None, None
    r = num/denom
    t = r*math.sqrt(n-2)/math.sqrt(max(1e-9, 1-r**2))
    pval = math.erfc(abs(t)/math.sqrt(2))  # normal approx (fine for n>30)
    return round(r,4), round(pval,4)

# ── Appointment-level data prep ───────────────────────────────────────────────
HALF_LIFE       = 30
BACKTEST_CUTOFF = pd.Timestamp('2026-03-18')

adf = sf_raw.copy()
adf['clinic']        = adf.iloc[:,  2].astype(str)   # Clinic name
adf['ptype']         = adf.iloc[:,  6].astype(str)   # Historical Contact Type Code 2
adf['created_date']  = pd.to_datetime(adf.iloc[:, 7], errors='coerce')  # Created Date
adf['start']         = pd.to_datetime(adf.iloc[:, 8], errors='coerce')  # Start Date
adf['biz_days']      = pd.to_numeric(adf.iloc[:, 10], errors='coerce')  # Business Days duration
adf['status']        = adf.iloc[:, 11].astype(str)   # Appointment status
adf['outcome']       = adf.iloc[:, 13].astype(str)   # Appointment outcome
adf['created_by']    = adf.iloc[:, 14].astype(str)   # Created-by logged role
adf['campaign']      = adf.iloc[:, 16].astype(str)   # Campaign Activity code and name
adf['conf_24']       = adf.iloc[:, 19]               # 24hr Confirmation Date
adf['showed']     = adf['status'] == 'Completed'
adf['sold']       = adf['outcome'] == 'Sold'
adf['is_cg']      = adf['created_by'].isin(['Dispenser','Clinic Assistant'])
adf['confirmed']  = (adf['conf_24'].notna() &
                     ~adf['conf_24'].astype(str).isin(['nan','NaT','']))
adf['dow']        = adf['start'].dt.day_name()
adf['category']   = adf['campaign'].apply(campaign_to_cat)

def lt_bin(x):
    if pd.isna(x): return 'Unknown'
    x = int(x)
    if x == 0:  return '0 – Same Day'
    if x <= 3:  return '1–3 days'
    if x <= 7:  return '4–7 days'
    if x <= 14: return '8–14 days'
    return '15+ days'

adf['lt_bin'] = adf['biz_days'].apply(lt_bin)

# Restrict to resolved appointments for analysis
hist = adf[adf['status'].isin(['Completed','Cancelled'])].copy()

# ── Correlation analysis ──────────────────────────────────────────────────────
def cat_breakdown(df, col, order=None):
    vals = order or df[col].dropna().unique().tolist()
    levels = []
    for v in vals:
        sub = df[df[col]==v]
        if len(sub) < 3: continue
        sr = sub['showed'].mean()
        levels.append({'label':str(v),'n':len(sub),
                       'show_rate':round(sr,4),'ci':wilson_ci(sr,len(sub))})
    return levels

correlations = []

# 1 — Confirmation status
p_c,  n_c  = hist[hist['confirmed']]['showed'].mean(),  hist['confirmed'].sum()
p_nc, n_nc = hist[~hist['confirmed']]['showed'].mean(), (~hist['confirmed']).sum()
z1, pv1 = z_test_props(p_c, n_c, p_nc, n_nc)
OR1, OR1l, OR1h = odds_ratio(p_c, n_c, p_nc, n_nc)
correlations.append({'feature':'Confirmation Status','type':'binary',
    'levels':[{'label':'Confirmed','n':int(n_c),'show_rate':round(p_c,4),'ci':wilson_ci(p_c,n_c)},
              {'label':'Not Confirmed','n':int(n_nc),'show_rate':round(p_nc,4),'ci':wilson_ci(p_nc,n_nc)}],
    'or':OR1,'or_ci':[OR1l,OR1h],'z':z1,'pval':pv1,
    'note':'Strongest single predictor of show rate'})

# 2 — Lead source origin
p_cg,  n_cg  = hist[hist['is_cg']]['showed'].mean(),  hist['is_cg'].sum()
p_ext, n_ext = hist[~hist['is_cg']]['showed'].mean(), (~hist['is_cg']).sum()
z2, pv2 = z_test_props(p_cg, n_cg, p_ext, n_ext)
OR2, OR2l, OR2h = odds_ratio(p_cg, n_cg, p_ext, n_ext)
correlations.append({'feature':'Lead Source Origin','type':'binary',
    'levels':[{'label':'Clinic-Generated','n':int(n_cg),'show_rate':round(p_cg,4),'ci':wilson_ci(p_cg,n_cg)},
              {'label':'External','n':int(n_ext),'show_rate':round(p_ext,4),'ci':wilson_ci(p_ext,n_ext)}],
    'or':OR2,'or_ci':[OR2l,OR2h],'z':z2,'pval':pv2,
    'note':'Clinic-generated appointments show ~1.8× rate of external'})

# 3 — Patient type
correlations.append({'feature':'Patient Type','type':'categorical',
    'levels':cat_breakdown(hist,'ptype',['New Prospect','Customer','Database Prospect']),
    'or':None,'or_ci':None,'z':None,'pval':None,
    'note':'New vs existing vs prior non-purchaser'})

# 4 — Lead time bins
lt_order = ['0 – Same Day','1–3 days','4–7 days','8–14 days','15+ days','Unknown']
lt_r, lt_pv = pearson_r_binary(hist['biz_days'].tolist(), hist['showed'].astype(float).tolist())
correlations.append({'feature':'Appointment Lead Time','type':'binned',
    'levels':cat_breakdown(hist,'lt_bin',lt_order),
    'pearson_r':lt_r,'pval':lt_pv,'or':None,'or_ci':None,
    'note':f'Pearson r={lt_r} (p={lt_pv}): longer lead time → lower show rate'})

# 5 — Day of week
dow_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
correlations.append({'feature':'Day of Week (Appt)','type':'categorical',
    'levels':cat_breakdown(hist,'dow',dow_order),
    'or':None,'or_ci':None,'z':None,'pval':None,
    'note':'Day appointment is scheduled'})

# ── Backtest ──────────────────────────────────────────────────────────────────
bt_train = hist[hist['start'] <  BACKTEST_CUTOFF].copy()
bt_test  = hist[hist['start'] >= BACKTEST_CUTOFF].copy()

bt_train['days_ago'] = (BACKTEST_CUTOFF - bt_train['start']).dt.days.clip(lower=0)
bt_train['weight']   = 0.5 ** (bt_train['days_ago'] / HALF_LIFE)

def wrate(df, mask=None, weighted=False, min_n=3):
    sub = df[mask] if mask is not None else df
    if len(sub) < min_n: return None
    if weighted:
        w = sub['weight'].sum()
        return float((sub['showed']*sub['weight']).sum()/w) if w>0 else None
    return float(sub['showed'].mean())

# Build clinic × confirmation lookup from training data (both models)
_train_lookup = {}
for clinic, grp in bt_train.groupby('clinic'):
    conf_mask = grp['confirmed']
    _train_lookup[clinic] = {
        'A': {'conf': wrate(grp, conf_mask,  False),
              'nconf':wrate(grp, ~conf_mask, False)},
        'B': {'conf': wrate(grp, conf_mask,  True),
              'nconf':wrate(grp, ~conf_mask, True)},
    }

# Training-period overall fallback
_train_fb = {
    'A': {'conf': wrate(bt_train, bt_train['confirmed'],  False) or 0.789,
          'nconf':wrate(bt_train, ~bt_train['confirmed'], False) or 0.379},
    'B': {'conf': wrate(bt_train, bt_train['confirmed'],  True)  or 0.789,
          'nconf':wrate(bt_train, ~bt_train['confirmed'], True)  or 0.379},
}

# AJ CSV rates as Model C (full-period, clinic-specific)
_aj_lookup = {c['name']: {'conf': c['show_confirm'], 'nconf': c['show_nonconfirm']}
              for c in clinics}
_aj_fb = {'conf': region_conf, 'nconf': region_nonconf}

def get_p(clinic, confirmed, model):
    key = 'conf' if confirmed else 'nconf'
    if model in ('A','B'):
        p = _train_lookup.get(clinic,{}).get(model,{}).get(key)
        return p if p is not None else _train_fb[model][key]
    # Model C — AJ CSV
    lr = _aj_lookup.get(clinic,{})
    p = lr.get(key)
    return p if p is not None else _aj_fb[key]

bt_rows = []
for _, row in bt_test.iterrows():
    c, conf, actual = row['clinic'], row['confirmed'], int(row['showed'])
    sold   = int(row['sold'])
    cat    = row['category'] if pd.notna(row['category']) else None
    is_cg  = bool(row['is_cg'])

    # Close rate: same hierarchy as JS predictAppointments
    cat_data = rates['categories'].get(cat) if cat else None
    grp = cat_data['group'] if cat_data else ('clinic' if is_cg else 'external')
    if   cat_data and cat_data['close'] is not None: p_close = cat_data['close']
    elif grp == 'clinic':                            p_close = rates['origin']['clinic']['close']
    elif grp == 'external':                          p_close = rates['origin']['external']['close']
    else:                                            p_close = rates['region']['close']

    # EV/test: same hierarchy as JS predictAppointments
    ev = ev_per_test.get(cat) if cat else None
    if ev is None:
        ev = ev_by_group.get(grp, ev_by_group['overall'])

    bt_rows.append({'clinic':c,'confirmed':bool(conf),'actual':actual,'sold':sold,
                    'p_close':round(p_close,4),'ev':ev,
                    'pA':round(get_p(c,conf,'A'),4),
                    'pB':round(get_p(c,conf,'B'),4),
                    'pC':round(get_p(c,conf,'C'),4)})

def brier(ps, ys):
    return round(sum((p-y)**2 for p,y in zip(ps,ys))/len(ps),4) if ps else None

pAs=[r['pA'] for r in bt_rows]; pBs=[r['pB'] for r in bt_rows]
pCs=[r['pC'] for r in bt_rows]; acts=[r['actual'] for r in bt_rows]

# ── Revenue backtest ──────────────────────────────────────────────────────────
# Uses same calibrated parameters as the Forecast tab for consistency
_bt_ha  = ha_per_sale_act     # calibrated to contracted rev (~1.773)
_bt_asp = round(tot_asp, 2)
_bt_tr  = aidable_rate        # 0.72 = test_rate × aidable_of_tested

actual_sales_bt     = sum(r['sold']  for r in bt_rows)
actual_rev_bt       = round(actual_sales_bt * _bt_ha * _bt_asp, 2)

def _bt_rev(model_key):
    return round(sum(r[model_key] * _bt_tr * r['p_close'] * _bt_ha * _bt_asp
                     for r in bt_rows), 2)

pred_rev_A  = _bt_rev('pA')
pred_rev_B  = _bt_rev('pB')
pred_rev_C  = _bt_rev('pC')
pred_rev_ev = round(sum(r['ev'] for r in bt_rows), 2)

backtest = {
    'n_train': len(bt_train), 'n_test': len(bt_test),
    'cutoff':  str(BACKTEST_CUTOFF.date()),
    'actual_shows':     int(sum(acts)),
    'actual_sales':     actual_sales_bt,
    'actual_rev':       actual_rev_bt,
    'asp_used':         _bt_asp,
    'ha_per_sale':      _bt_ha,
    'pred_rev_ev':      pred_rev_ev,
    'rev_mae_ev':       round(abs(pred_rev_ev - actual_rev_bt)),
    'models': {
        'A': {'label':'Equal-Weight (SF sample)',
              'pred':round(sum(pAs),1),'brier':brier(pAs,acts),
              'mae':round(abs(sum(pAs)-sum(acts)),1),
              'pred_rev':pred_rev_A,
              'rev_mae': round(abs(pred_rev_A - actual_rev_bt))},
        'B': {'label':f'Recency-Weighted 30d half-life (SF sample)',
              'pred':round(sum(pBs),1),'brier':brier(pBs,acts),
              'mae':round(abs(sum(pBs)-sum(acts)),1),
              'pred_rev':pred_rev_B,
              'rev_mae': round(abs(pred_rev_B - actual_rev_bt))},
        'C': {'label':'Full-Period AJ CSV rates (JAN–MAR)',
              'pred':round(sum(pCs),1),'brier':brier(pCs,acts),
              'mae':round(abs(sum(pCs)-sum(acts)),1),
              'pred_rev':pred_rev_C,
              'rev_mae': round(abs(pred_rev_C - actual_rev_bt))},
    },
    'clinic_rows': sorted([
        {'clinic':c,
         'n':       sum(1         for r in bt_rows if r['clinic']==c),
         'actual':  sum(r['actual'] for r in bt_rows if r['clinic']==c),
         'sold':    sum(r['sold']   for r in bt_rows if r['clinic']==c),
         'predA':   round(sum(r['pA'] for r in bt_rows if r['clinic']==c),1),
         'predB':   round(sum(r['pB'] for r in bt_rows if r['clinic']==c),1),
         'predC':   round(sum(r['pC'] for r in bt_rows if r['clinic']==c),1),
         'actual_rev': round(sum(r['sold'] * _bt_ha * _bt_asp for r in bt_rows if r['clinic']==c)),
         'revA':  round(sum(r['pA'] * _bt_tr * r['p_close'] * _bt_ha * _bt_asp for r in bt_rows if r['clinic']==c)),
         'revB':  round(sum(r['pB'] * _bt_tr * r['p_close'] * _bt_ha * _bt_asp for r in bt_rows if r['clinic']==c)),
         'revEV': round(sum(r['ev'] for r in bt_rows if r['clinic']==c)),
        }
        for c in sorted(set(r['clinic'] for r in bt_rows))
    ], key=lambda x: x['clinic']),
}

# ── Update rates to use recency-weighted origin rates ─────────────────────────
# Replace simple-average origin rates with exponentially-weighted version
# (weighted from reference date across the full SF sample)
adf['days_ago'] = (pd.Timestamp('2026-04-01') - adf['start']).dt.days.clip(lower=0)
adf['weight']   = 0.5 ** (adf['days_ago'] / HALF_LIFE)

def rw_show(mask):
    sub = adf[mask & adf['status'].isin(['Completed','Cancelled'])]
    if len(sub) < 3: return None
    w = sub['weight'].sum()
    return float((sub['showed']*sub['weight']).sum()/w) if w > 0 else None

rw_cg  = rw_show(adf['is_cg'])
rw_ext = rw_show(~adf['is_cg'])
if rw_cg:  rates['origin']['clinic']['show']   = round(rw_cg,  4)
if rw_ext: rates['origin']['external']['show'] = round(rw_ext, 4)

# ── Fix close rate CIs (correct aidable denominator) & flag small-n ──────────
# Wilson CI for close rate: denominator must be aidable losses, not total
# completions. Using n*show overstates denominator by ~1/aidable_rate (~19%),
# producing CIs that are 9–14% too narrow (overconfident).
# Also flag Wilson unreliability when min(n*p, n*(1-p)) < 5.
for ls in lead_sources:
    n, show, close = ls['tests'], ls['show_rate'], ls['close_rate']
    # Corrected close CI: denominator = aidable tested = shows × test_rate × aidable_of_tested
    if close is not None and show is not None and n > 0:
        aidable_n = round(n * show * aidable_rate)   # aidable_rate = 0.72
        ls['close_ci'] = wilson_ci(close, aidable_n) if aidable_n > 0 else [None, None]
    # Wilson reliability flag: requires n*p >= 5 AND n*(1-p) >= 5
    sr = ls['show_rate'] or 0
    ls['ci_reliable'] = (n * sr >= 5) and (n * (1 - sr) >= 5) and (n >= 10)

# Same flag for clinic-level show CIs
for c in clinics:
    sr = c['show_rate'] or 0
    n  = c['tests']
    c['ci_reliable'] = (n * sr >= 5) and (n * (1 - sr) >= 5) and (n >= 10)

# ── Same-week appointment creation model ──────────────────────────────────────
# Definition: a test is "same-week created" when the ISO calendar week of
# created_date matches the ISO calendar week of start date.
# This is ground-truth — no biz_days approximation needed.
# We then group by appointment day-of-week to get the per-day creation distribution.

_DOW_IDX   = {'Monday':0,'Tuesday':1,'Wednesday':2,'Thursday':3,'Friday':4}
_DOW_SHORT = ['Mon','Tue','Wed','Thu','Fri']

# Require both dates present; exclude Events category
_sw = adf[adf['start'].notna() & adf['created_date'].notna()].copy()
_sw['_cat'] = _sw['campaign'].apply(campaign_to_cat)
_sw = _sw[_sw['_cat'] != 'Events'].copy()

# Restrict to weekday appointments only
_sw['dow_name'] = _sw['start'].dt.day_name()
_sw = _sw[_sw['dow_name'].isin(_DOW_IDX)].copy()
_sw['dow_idx']  = _sw['dow_name'].map(_DOW_IDX).astype(int)

# ISO week for both dates — same year+week = same-week creation
def _iso_week_key(series):
    iso = series.dt.isocalendar()
    return iso.year.astype(str) + '_' + iso.week.astype(str).str.zfill(2)

_sw['appt_week']    = _iso_week_key(_sw['start'])
_sw['created_week'] = _iso_week_key(_sw['created_date'])
_sw['same_week']    = _sw['appt_week'] == _sw['created_week']

_n_weeks  = int(_sw['appt_week'].nunique())
_sw_same  = _sw[_sw['same_week']].copy()

def _sw_avg(mask, n_wk):
    """Count of True values in mask divided by number of weeks."""
    return round(int(mask.sum()) / max(n_wk, 1), 2)

# Regional averages per appointment DOW
sw_region = {}
for _doi, _ds in enumerate(_DOW_SHORT):
    _sub = _sw_same[_sw_same['dow_idx'] == _doi]
    sw_region[_ds] = {
        'clinic_gen':     _sw_avg(_sub['is_cg'],  _n_weeks),
        'non_clinic_gen': _sw_avg(~_sub['is_cg'], _n_weeks),
    }

# Per-clinic averages (require ≥ 4 weeks of data; else None → JS uses 50% of regional)
sw_by_clinic = {}
for _clinic, _grp in _sw_same.groupby('clinic'):
    _cl_wk = int(_sw[_sw['clinic'] == _clinic]['appt_week'].nunique())
    if _cl_wk < 4:
        sw_by_clinic[_clinic] = None   # JS fallback: 50% × regional
        continue
    _cd = {}
    for _doi, _ds in enumerate(_DOW_SHORT):
        _sub = _grp[_grp['dow_idx'] == _doi]
        _cd[_ds] = {
            'clinic_gen':     _sw_avg(_sub['is_cg'],  _cl_wk),
            'non_clinic_gen': _sw_avg(~_sub['is_cg'], _cl_wk),
        }
    sw_by_clinic[_clinic] = _cd

# ── Assemble payload ──────────────────────────────────────────────────────────
DATA = {
    'generated':  '2026-04-03',
    'period':     'JAN\u2013MAR 2026',
    'region':     'South Region',
    'totals': {
        'tests':      tot_tests,
        'show_rate':  round(tot_show,  4),
        'show_ci':    wilson_ci(tot_show,  tot_tests),
        'close_rate': round(tot_close, 4),
        'close_ci':   wilson_ci(tot_close, round(tot_tests * tot_show * aidable_rate)),
        'ha_units':   tot_units,
        'asp':        round(tot_asp, 2),
        'revenue':    round(tot_rev,  2),
    },
    'revenue_timing': {
        'contracted':          round(tot_rev, 2),
        'invoiced':            round(tot_invoiced, 2),
        'invoice_ratio':       round(tot_invoiced / tot_rev, 4) if tot_rev > 0 else None,
        'sdf_pct':             round(tot_sdf, 4) if tot_sdf is not None else None,
        'ha_per_sale':         ha_per_sale_act,
        'aidable_rate':        aidable_rate,
        'aidable_rate_observed': aidable_rate_observed,
        'categories': [
            {
                'category':    ls['category'],
                'group':       ls['group'],
                'tests':       ls['tests'],
                'contracted':  ls['revenue'],
                'invoiced':    ls['inv_revenue'],
                'inv_ratio':   ls['inv_ratio'],
                'sdf_pct':     ls['sdf_pct'],
                'ev_contracted': ev_per_test.get(ls['category']),
            }
            for ls in lead_sources
        ]
    },
    'lead_sources': lead_sources,
    'clinics':      clinics,
    'origin':       origin,
    'patients':     patients,
    'cross_check':   cross_check,
    'rates':         rates,
    'correlations':  correlations,
    'backtest':      backtest,
    'same_week_creation': {
        'region':    sw_region,
        'by_clinic': sw_by_clinic,
        'n_weeks':   _n_weeks,
    },
}

# ── HTML template ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>South Region KPI Dashboard</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;background:#f0f2f5;color:#1a1a2e;font-size:14px;min-height:100vh}

/* Header */
.hdr{background:#1a1a2e;color:#fff;padding:14px 24px;display:flex;align-items:center;justify-content:space-between}
.hdr h1{font-size:18px;font-weight:600;letter-spacing:.4px}
.hdr .meta{font-size:11px;color:#8892b0;margin-top:2px}

/* Summary cards */
.cards{display:flex;gap:12px;padding:16px 24px;flex-wrap:wrap}
.card{background:#fff;border-radius:8px;padding:14px 18px;flex:1;min-width:130px;
      box-shadow:0 1px 4px rgba(0,0,0,.08)}
.card .lbl{font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:#64748b;margin-bottom:4px}
.card .val{font-size:24px;font-weight:700;color:#1a1a2e;line-height:1.1}
.card .sub{font-size:10px;color:#94a3b8;margin-top:3px}
.card.hi .val{color:#0ea5e9}

/* Tabs */
.tabs{display:flex;gap:0;padding:0 24px;background:#e8eaf0;border-bottom:1px solid #d1d5db}
.tab{padding:10px 20px;cursor:pointer;font-size:13px;font-weight:500;color:#64748b;
     border-top:3px solid transparent;transition:all .15s;white-space:nowrap}
.tab.active{background:#fff;color:#1a1a2e;border-top-color:#0ea5e9}
.tab:hover:not(.active){background:#f0f2f5}

/* Panels */
.panel{display:none;padding:20px 24px}
.panel.active{display:block}

/* Section labels */
.sec-note{font-size:11px;color:#94a3b8;margin-bottom:14px;line-height:1.5}

/* Chart row */
.chart-row{display:flex;gap:14px;margin-bottom:18px;flex-wrap:wrap}
.cbox{background:#fff;border-radius:8px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08);flex:1;min-width:280px}
.cbox h3{font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:#64748b;margin-bottom:10px}
canvas{display:block;width:100%}

/* Tables */
.twrap{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:auto;margin-bottom:18px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:#f8fafc;padding:9px 10px;text-align:left;font-size:10px;font-weight:600;
   text-transform:uppercase;letter-spacing:.5px;color:#64748b;cursor:pointer;
   white-space:nowrap;border-bottom:2px solid #e2e8f0;user-select:none}
th:hover{background:#f0f4f8}
th::after{content:' ↕';opacity:.3;font-size:9px}
td{padding:8px 10px;border-bottom:1px solid #f1f5f9;white-space:nowrap}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f8fafc}

/* Pills */
.pill{display:inline-block;padding:2px 7px;border-radius:99px;font-size:9px;font-weight:700;letter-spacing:.3px}
.pill.clinic{background:#dcfce7;color:#15803d}
.pill.external{background:#fee2e2;color:#b91c1c}
.pill.uncertain{background:#fef9c3;color:#92400e}

/* Rate bar cell */
.rb{display:flex;align-items:center;gap:5px}
.rbw{flex:1;height:7px;background:#f1f5f9;border-radius:4px;min-width:50px;overflow:hidden}
.rbb{height:100%;border-radius:4px}
.rbv{font-size:11px;font-weight:600;min-width:38px;text-align:right}

/* Origin compare cards */
.cmp-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:18px}
.cmp-card{background:#fff;border-radius:8px;padding:18px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.cmp-card.c-clinic{border-top:4px solid #22c55e}
.cmp-card.c-ext{border-top:4px solid #f87171}
.cmp-card h3{font-size:13px;font-weight:600;margin-bottom:14px}
.stat{display:flex;justify-content:space-between;margin-bottom:7px;align-items:baseline}
.stat .sl{font-size:11px;color:#64748b}
.stat .sv{font-size:13px;font-weight:600}
.stat .sci{font-size:9px;color:#94a3b8;display:block}

/* Cross-check callout */
.xcheck{background:#fffbeb;border:1px solid #fcd34d;border-radius:6px;
        padding:10px 14px;margin-bottom:16px;font-size:11px;color:#92400e;line-height:1.5}
.xcheck strong{display:block;margin-bottom:2px}

@media(max-width:680px){
  .cmp-grid{grid-template-columns:1fr}
  .cards .card{min-width:110px}
  .tabs{overflow-x:auto}
}

/* Forecast tab */
.drop-zone{border:2px dashed #cbd5e1;border-radius:12px;padding:48px 32px;text-align:center;
           background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:18px;
           transition:border-color .2s,background .2s;cursor:pointer}
.drop-zone.over{border-color:#0ea5e9;background:#f0f9ff}
.drop-title{font-size:15px;font-weight:600;margin-bottom:6px;color:#1a1a2e}
.drop-sub{font-size:11px;color:#94a3b8;margin-bottom:16px;line-height:1.5}
.btn-up{background:#0ea5e9;color:#fff;border:none;padding:8px 22px;border-radius:6px;
        cursor:pointer;font-size:13px;font-weight:500}
.btn-up:hover{background:#0284c7}
.btn-reset{background:none;border:1px solid #cbd5e1;color:#64748b;padding:5px 14px;
           border-radius:6px;cursor:pointer;font-size:12px;margin-left:8px}
.fc-file-info{font-size:12px;color:#0ea5e9;margin-top:10px;font-weight:500}
.fc-warn{background:#fff7ed;border:1px solid #fdba74;border-radius:6px;padding:8px 12px;
         font-size:11px;color:#c2410c;margin-bottom:12px}
.fc-method{background:#f8fafc;border-radius:6px;padding:12px 16px;font-size:11px;
           color:#64748b;line-height:1.6;margin-top:14px}
.fc-method strong{color:#374151;display:block;margin-bottom:4px}
.ci-badge{font-size:10px;color:#94a3b8;display:block}

/* Analysis tab */
.corr-grid{display:flex;flex-direction:column;gap:14px;margin-bottom:20px}
.corr-card{background:#fff;border-radius:8px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.corr-card h3{font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:#64748b;margin-bottom:4px}
.corr-note{font-size:10px;color:#94a3b8;margin-bottom:8px}
.corr-stat{display:inline-block;background:#f1f5f9;border-radius:4px;padding:2px 7px;font-size:10px;
           margin-right:6px;color:#475569}
.corr-stat.sig{background:#dcfce7;color:#15803d}
.corr-stat.ns{background:#fef9c3;color:#92400e}
.bt-model-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}
.bt-card{background:#fff;border-radius:8px;padding:14px;box-shadow:0 1px 4px rgba(0,0,0,.08);text-align:center}
.bt-card .bt-lbl{font-size:10px;color:#64748b;margin-bottom:8px;text-transform:uppercase;letter-spacing:.4px}
.bt-card .bt-val{font-size:22px;font-weight:700;color:#1a1a2e}
.bt-card .bt-sub{font-size:10px;color:#94a3b8;margin-top:3px}
.bt-card.best{border-top:3px solid #22c55e}
.bt-winner{font-size:11px;color:#15803d;font-weight:600;margin-bottom:12px}
.err-pos{color:#ef4444}.err-neg{color:#22c55e}.err-zero{color:#94a3b8}
@media(max-width:680px){.bt-model-grid{grid-template-columns:1fr}}
</style>
</head>
<body>

<div class="hdr">
  <div>
    <h1 id="hdr-title"></h1>
    <div class="meta" id="hdr-meta"></div>
  </div>
</div>

<div class="cards" id="cards"></div>

<div class="tabs">
  <div class="tab active"  data-tab="lead">Lead Sources</div>
  <div class="tab"         data-tab="clinics">Clinics</div>
  <div class="tab"         data-tab="patients">Patient Types</div>
  <div class="tab"         data-tab="origin">Clinic vs External</div>
  <div class="tab"         data-tab="forecast">&#x1F4C5; Weekly Forecast</div>
  <div class="tab"         data-tab="analysis">&#x1F50D; Analysis &amp; Backtest</div>
</div>

<div class="panel active" id="p-lead"></div>
<div class="panel"        id="p-clinics"></div>
<div class="panel"        id="p-patients"></div>
<div class="panel"        id="p-origin"></div>
<div class="panel"        id="p-forecast"></div>
<div class="panel"        id="p-analysis"></div>

<script>
const D = __DATA__;

// ── Formatting ────────────────────────────────────────────────────────────────
const f = {
  pct: v => v == null ? '—' : (v*100).toFixed(1)+'%',
  n:   v => v == null ? '—' : (+v).toLocaleString(),
  $:   v => v == null ? '—' : '$'+Math.round(v).toLocaleString(),
  // ci: only render if reliable flag is true (or not provided); flag small-n with a note
  ci:  (c, reliable) => {
    if(reliable===false) return '<span style="color:#f59e0b;font-size:9px" title="n too small: min(n\xd7p, n\xd7(1-p)) < 5 \u2014 Wilson CI unreliable">n too small</span>';
    return (c && c[0]!=null) ? `[${(c[0]*100).toFixed(1)}%\u2013${(c[1]*100).toFixed(1)}%]` : '';
  },
};
const gCol  = g => g==='clinic'?'#22c55e':g==='external'?'#f87171':'#fbbf24';
const gLbl  = g => g==='clinic'?'Clinic-Gen':g==='external'?'External':'Uncertain';

// ── Header & cards ────────────────────────────────────────────────────────────
document.getElementById('hdr-title').textContent = D.region+' \u2014 Hearing Care KPI Dashboard';
document.getElementById('hdr-meta').textContent  = D.period+'  \u00b7  Generated '+D.generated;

const t = D.totals;
document.getElementById('cards').innerHTML = [
  {lbl:'Tests Scheduled', val:f.n(t.tests),        sub:'JAN\u2013MAR 2026'},
  {lbl:'Show Rate',       val:f.pct(t.show_rate),   sub:f.ci(t.show_ci), cls:'hi'},
  {lbl:'Close Rate',      val:f.pct(t.close_rate),  sub:f.ci(t.close_ci)},
  {lbl:'HA Units',        val:f.n(t.ha_units),      sub:'ASP '+f.$(t.asp)},
  {lbl:'Revenue',         val:f.$(t.revenue),        sub:''},
].map(c=>`<div class="card ${c.cls||''}">
  <div class="lbl">${c.lbl}</div>
  <div class="val">${c.val}</div>
  <div class="sub">${c.sub}</div>
</div>`).join('');

// ── Tabs ──────────────────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab=>{
  tab.addEventListener('click',()=>{
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('p-'+tab.dataset.tab).classList.add('active');
    // Redraw charts after panel becomes visible
    const t = tab.dataset.tab;
    if(t==='analysis') drawCorrCharts();
    else { drawAll(t); if(t==='forecast') renderForecastHistory(); }
  });
});

// ── Sortable tables ───────────────────────────────────────────────────────────
function makeSortable(id){
  const tbl = document.getElementById(id);
  if(!tbl) return;
  tbl.querySelectorAll('th').forEach((th,col)=>{
    th.addEventListener('click',()=>{
      const asc = tbl.dataset.sc===String(col) && tbl.dataset.sd==='asc';
      tbl.dataset.sc=col; tbl.dataset.sd=asc?'desc':'asc';
      const rows=[...tbl.querySelector('tbody').rows];
      const num = th.dataset.num!=='false';
      rows.sort((a,b)=>{
        let va=a.cells[col]?.dataset?.v ?? a.cells[col]?.textContent??'';
        let vb=b.cells[col]?.dataset?.v ?? b.cells[col]?.textContent??'';
        if(num){va=+va||0;vb=+vb||0;}
        return asc?(va>vb?1:-1):(va<vb?1:-1);
      });
      rows.forEach(r=>tbl.querySelector('tbody').appendChild(r));
    });
  });
}

// ── Rate bar helper ───────────────────────────────────────────────────────────
function rateBar(val, group){
  const w = val!=null?(val*100).toFixed(1):0;
  return `<div class="rb">
    <div class="rbw"><div class="rbb" style="width:${w}%;background:${gCol(group||'uncertain')}"></div></div>
    <span class="rbv">${f.pct(val)}</span>
  </div>`;
}

// ── LEAD SOURCES panel ────────────────────────────────────────────────────────
function renderLead(){
  const ls = D.lead_sources;
  document.getElementById('p-lead').innerHTML = `
    <p class="sec-note">
      Lead sources are never aggregated — each category shown separately with 95% Wilson confidence intervals.
      <span style="display:inline-flex;gap:10px;margin-left:8px">
        <span><span class="pill clinic">Clinic-Gen</span></span>
        <span><span class="pill external">External</span></span>
        <span><span class="pill uncertain">Uncertain</span></span>
      </span>
    </p>
    <div class="chart-row">
      <div class="cbox" style="flex:2"><h3>Show Rate by Lead Source \u2014 sorted, 95% CI</h3><canvas id="cv-sr"></canvas></div>
      <div class="cbox" style="flex:1"><h3>Test Volume by Lead Source</h3><canvas id="cv-vol"></canvas></div>
    </div>
    <div class="twrap"><table id="tbl-ls">
      <thead><tr>
        <th data-num="false">Category</th>
        <th data-num="false">Type</th>
        <th>Tests</th>
        <th>Show Rate</th>
        <th data-num="false">95% CI</th>
        <th>Close Rate</th>
        <th data-num="false">Close 95% CI</th>
        <th>Contracted Rev</th>
        <th>EV/Test</th>
      </tr></thead>
      <tbody>${ls.map(d=>`<tr>
        <td>${d.category}</td>
        <td><span class="pill ${d.group}">${gLbl(d.group)}</span></td>
        <td data-v="${d.tests}">${f.n(d.tests)}</td>
        <td data-v="${d.show_rate??0}">${rateBar(d.show_rate,d.group)}</td>
        <td style="color:#94a3b8;font-size:10px">${f.ci(d.show_ci,d.ci_reliable)}</td>
        <td data-v="${d.close_rate??0}">${rateBar(d.close_rate,d.group)}</td>
        <td style="color:#94a3b8;font-size:10px">${f.ci(d.close_ci,d.ci_reliable)}</td>
        <td data-v="${d.revenue}">${f.$(d.revenue)}</td>
        <td data-v="${d.tests>0?d.revenue/d.tests:0}">${d.tests>0?f.$(Math.round(d.revenue/d.tests)):'\u2014'}</td>
      </tr>`).join('')}</tbody>
    </table></div>

    <div class="section-title" style="font-size:13px;font-weight:600;margin:20px 0 10px">
      Revenue Timing \u2014 Contracted vs Invoiced
    </div>
    <p class="sec-note">
      Contracted = committed at point of fitting (same period as appointment).
      Invoiced = cash recognition (follows within ~4 weeks for most categories).
      Region total: contracted ${f.$(D.revenue_timing.contracted)},
      invoiced ${f.$(D.revenue_timing.invoiced)}
      (${f.pct(D.revenue_timing.invoice_ratio)} invoice ratio &middot;
      ${f.pct(D.revenue_timing.sdf_pct)} same-day fit).
      Ratios &gt;1.00 reflect prior-period carryover invoicing in this period;
      &lt;1.00 reflect end-of-period contracts not yet invoiced.
    </p>
    <div class="twrap"><table id="tbl-rev-timing">
      <thead><tr>
        <th data-num="false">Category</th>
        <th data-num="false">Type</th>
        <th>Tests</th>
        <th>Contracted</th>
        <th>Invoiced</th>
        <th>Invoice Ratio</th>
        <th>Same-Day Fit %</th>
        <th>EV/Test (contracted)</th>
      </tr></thead>
      <tbody>
        <tr style="background:#f8fafc;font-weight:600">
          <td>TOTAL</td><td></td>
          <td>${f.n(D.revenue_timing.categories.reduce((a,r)=>a+r.tests,0))}</td>
          <td>${f.$(D.revenue_timing.contracted)}</td>
          <td>${f.$(D.revenue_timing.invoiced)}</td>
          <td>${f.pct(D.revenue_timing.invoice_ratio)}</td>
          <td>${f.pct(D.revenue_timing.sdf_pct)}</td>
          <td>${f.$(Math.round(D.revenue_timing.contracted / D.revenue_timing.categories.reduce((a,r)=>a+r.tests,0)))}</td>
        </tr>
        ${D.revenue_timing.categories.filter(r=>r.contracted>0||r.invoiced>0).map(r=>`<tr>
          <td>${r.category}</td>
          <td><span class="pill ${r.group}">${gLbl(r.group)}</span></td>
          <td data-v="${r.tests}">${f.n(r.tests)}</td>
          <td data-v="${r.contracted}">${f.$(r.contracted)}</td>
          <td data-v="${r.invoiced}">${f.$(r.invoiced)}</td>
          <td data-v="${r.inv_ratio??0}" style="color:${r.inv_ratio==null?'#94a3b8':r.inv_ratio>1.05?'#0ea5e9':r.inv_ratio<0.92?'#f59e0b':'#22c55e'}">
            ${r.inv_ratio!=null?f.pct(r.inv_ratio):'\u2014'}
          </td>
          <td data-v="${r.sdf_pct??0}">${r.sdf_pct!=null?f.pct(r.sdf_pct):'\u2014'}</td>
          <td data-v="${r.ev_contracted??0}">${r.ev_contracted!=null?f.$(Math.round(r.ev_contracted)):'\u2014'}</td>
        </tr>`).join('')}
      </tbody>
    </table></div>
    <div class="fc-method">
      <strong>Funnel model calibration</strong>
      The revenue funnel uses explicit parameters confirmed by regional manager:
      ${f.pct(D.rates.test_rate)} of shows receive a hearing test &times;
      ${f.pct(D.rates.aidable_of_tested)} of tested have aidable hearing loss &times;
      close rate &times; ${D.rates.ha_per_sale.toFixed(3)} HA/sale (back-calculated) &times; ASP ${f.$(D.rates.asp)}.
      Effective aidable rate = ${f.pct(D.rates.aidable_rate)} (replaces SF-observed ${f.pct(D.revenue_timing.aidable_rate_observed)}).
      ha/sale of ${D.rates.ha_per_sale.toFixed(3)} is calibrated to match full-period contracted revenue exactly;
      lower than the 1.9 binaural target because some sales are monaural and end-of-period
      contracted units invoice in the next period.
    </div>`;
  makeSortable('tbl-ls');
  makeSortable('tbl-rev-timing');
}

// ── CLINICS panel ─────────────────────────────────────────────────────────────
function renderClinics(){
  const cl = D.clinics;
  document.getElementById('p-clinics').innerHTML = `
    <p class="sec-note">Click any column to sort. Show rate 95% CI via Wilson score interval.
    Confirmed vs. non-confirmed show rates reveal the lift from appointment confirmation.</p>
    <div class="chart-row">
      <div class="cbox"><h3>Show Rate by Clinic \u2014 95% CI</h3><canvas id="cv-csr"></canvas></div>
      <div class="cbox"><h3>HA Units by Clinic</h3><canvas id="cv-cu"></canvas></div>
    </div>
    <div class="twrap"><table id="tbl-cl">
      <thead><tr>
        <th data-num="false">Clinic</th>
        <th data-num="false">ST</th>
        <th>Tests</th>
        <th>Show Rate</th>
        <th data-num="false">95% CI</th>
        <th>Conf SR</th>
        <th>Non-Conf SR</th>
        <th>HA Units</th>
        <th>ASP</th>
        <th>Revenue</th>
      </tr></thead>
      <tbody>${cl.map(d=>`<tr>
        <td>${d.name}</td>
        <td>${d.state}</td>
        <td data-v="${d.tests}">${f.n(d.tests)}</td>
        <td data-v="${d.show_rate??0}">${rateBar(d.show_rate,'uncertain')}</td>
        <td style="color:#94a3b8;font-size:10px">${f.ci(d.show_ci,d.ci_reliable)}</td>
        <td data-v="${d.show_confirm??0}">${f.pct(d.show_confirm)}</td>
        <td data-v="${d.show_nonconfirm??0}">${f.pct(d.show_nonconfirm)}</td>
        <td data-v="${d.ha_units}">${f.n(d.ha_units)}</td>
        <td data-v="${d.asp}">${f.$(d.asp)}</td>
        <td data-v="${d.revenue}">${f.$(d.revenue)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
  makeSortable('tbl-cl');
}

// ── PATIENTS panel ────────────────────────────────────────────────────────────
function renderPatients(){
  const pts = D.patients;
  document.getElementById('p-patients').innerHTML = `
    <p class="sec-note">From appointment-level Sales Funnel (sample, n=${f.n(D.cross_check.sf_sample)}).
    New Prospect = first-time buyer. Customer = existing patient. Database Prospect = prior non-purchaser in system.</p>
    <div class="chart-row">
      <div class="cbox"><h3>Show Rate by Patient Type \u2014 95% CI</h3><canvas id="cv-ptsr" height="200"></canvas></div>
      <div class="cbox"><h3>Close Rate by Patient Type \u2014 95% CI</h3><canvas id="cv-ptcr" height="200"></canvas></div>
    </div>
    <div class="twrap"><table>
      <thead><tr>
        <th data-num="false">Patient Type</th><th>Count</th><th>Showed</th>
        <th>Show Rate</th><th data-num="false">95% CI</th>
        <th>Sold</th><th>Aidable</th>
        <th>Close Rate</th><th data-num="false">95% CI</th>
      </tr></thead>
      <tbody>${pts.map(d=>`<tr>
        <td><strong>${d.type}</strong></td>
        <td>${f.n(d.count)}</td>
        <td>${f.n(d.showed)}</td>
        <td>${f.pct(d.show_rate)}</td>
        <td style="color:#94a3b8;font-size:10px">${f.ci(d.show_ci)}</td>
        <td>${f.n(d.sold)}</td>
        <td>${f.n(d.aidable)}</td>
        <td>${f.pct(d.close_rate)}</td>
        <td style="color:#94a3b8;font-size:10px">${f.ci(d.close_ci)}</td>
      </tr>`).join('')}</tbody>
    </table></div>`;
}

// ── ORIGIN panel ──────────────────────────────────────────────────────────────
function renderOrigin(){
  const cc = D.cross_check;
  const orig = D.origin;
  document.getElementById('p-origin').innerHTML = `
    <div class="xcheck">
      <strong>Cross-check: two independent classification signals</strong>
      ${cc.alignment_note}
      A large divergence between these proportions would indicate category misclassification and warrant review.
    </div>
    <div class="cmp-grid">
      ${Object.entries(orig).map(([name,d],i)=>`
      <div class="cmp-card ${i===0?'c-clinic':'c-ext'}">
        <h3>${name}</h3>
        <div class="stat"><span class="sl">Appointments (sample)</span><span class="sv">${f.n(d.count)}</span></div>
        <div class="stat"><span class="sl">Showed</span><span class="sv">${f.n(d.showed)}</span></div>
        <div class="stat">
          <span class="sl">Show Rate</span>
          <span class="sv">${f.pct(d.show_rate)}<span class="sci">${f.ci(d.show_ci)}</span></span>
        </div>
        <div class="stat"><span class="sl">Sold</span><span class="sv">${f.n(d.sold)}</span></div>
        <div class="stat"><span class="sl">Aidable Losses</span><span class="sv">${f.n(d.aidable)}</span></div>
        <div class="stat">
          <span class="sl">Close Rate</span>
          <span class="sv">${f.pct(d.close_rate)}<span class="sci">${f.ci(d.close_ci)}</span></span>
        </div>
      </div>`).join('')}
    </div>
    <div class="chart-row">
      <div class="cbox"><h3>Show Rate \u2014 Clinic-Generated vs External, 95% CI</h3><canvas id="cv-osr" height="200"></canvas></div>
      <div class="cbox"><h3>Close Rate \u2014 Clinic-Generated vs External, 95% CI</h3><canvas id="cv-ocr" height="200"></canvas></div>
    </div>`;
}

// ── Canvas charts ─────────────────────────────────────────────────────────────

function hbarCI(id, items){
  // items: [{label, value, ci, color, group}]
  const cv = document.getElementById(id);
  if(!cv) return;
  const ROW=20, PL=155, PR=70, PT=8, PB=22;
  const W = cv.parentElement.clientWidth - 32;
  const H = items.length*ROW + PT + PB;
  cv.width=W; cv.height=H; cv.style.height=H+'px';
  const cx = cv.getContext('2d');
  const CW = W - PL - PR;

  // Grid & x-axis labels
  cx.lineWidth=1;
  for(let v=0;v<=1;v+=0.2){
    const x = PL + v*CW;
    cx.strokeStyle='#f1f5f9';
    cx.beginPath(); cx.moveTo(x,PT); cx.lineTo(x,H-PB); cx.stroke();
    cx.fillStyle='#94a3b8'; cx.font='9px Segoe UI'; cx.textAlign='center';
    cx.fillText((v*100).toFixed(0)+'%', x, H-PB+12);
  }

  items.forEach((item,i)=>{
    const y=PT+i*ROW, bh=ROW-5, by=y+2;
    // Label
    cx.fillStyle='#374151'; cx.font='11px Segoe UI'; cx.textAlign='right';
    const lbl=item.label.length>22?item.label.slice(0,21)+'\u2026':item.label;
    cx.fillText(lbl, PL-5, by+bh/2+4);
    if(item.value==null) return;
    // Bar
    const bw=item.value*CW;
    cx.fillStyle=item.color+'44';
    cx.fillRect(PL,by,bw,bh);
    cx.fillStyle=item.color;
    cx.fillRect(PL,by,bw,bh);
    // CI whiskers
    if(item.ci && item.ci[0]!=null){
      const x0=PL+item.ci[0]*CW, x1=PL+item.ci[1]*CW, my=by+bh/2;
      cx.strokeStyle='#1e293b'; cx.lineWidth=1.5;
      cx.beginPath(); cx.moveTo(x0,my-3); cx.lineTo(x0,my+3); cx.stroke();
      cx.beginPath(); cx.moveTo(x1,my-3); cx.lineTo(x1,my+3); cx.stroke();
      cx.beginPath(); cx.moveTo(x0,my);   cx.lineTo(x1,my);   cx.stroke();
    }
    // Value text
    cx.fillStyle='#374151'; cx.font='bold 10px Segoe UI'; cx.textAlign='left';
    cx.fillText((item.value*100).toFixed(1)+'%', PL+bw+4, by+bh/2+4);
  });
}

function hbar(id, items){
  // items: [{label, value, color}]
  const cv = document.getElementById(id);
  if(!cv) return;
  const ROW=20, PL=155, PR=55, PT=8, PB=22;
  const W = cv.parentElement.clientWidth - 32;
  const H = items.length*ROW + PT + PB;
  cv.width=W; cv.height=H; cv.style.height=H+'px';
  const cx = cv.getContext('2d');
  const CW = W - PL - PR;
  const max = Math.max(...items.map(d=>d.value||0));

  items.forEach((item,i)=>{
    const y=PT+i*ROW, bh=ROW-5, by=y+2;
    cx.fillStyle='#374151'; cx.font='11px Segoe UI'; cx.textAlign='right';
    const lbl=item.label.length>22?item.label.slice(0,21)+'\u2026':item.label;
    cx.fillText(lbl, PL-5, by+bh/2+4);
    if(!item.value) return;
    const bw=(item.value/max)*CW;
    cx.fillStyle=item.color+'44';
    cx.fillRect(PL,by,bw,bh);
    cx.fillStyle=item.color;
    cx.fillRect(PL,by,bw,bh);
    cx.fillStyle='#374151'; cx.font='10px Segoe UI'; cx.textAlign='left';
    cx.fillText(item.value.toLocaleString(), PL+bw+4, by+bh/2+4);
  });
}

function vbarCI(id, items, palette){
  // items: [{label/type, show/close value, ci}]  vertical grouped
  const cv = document.getElementById(id);
  if(!cv) return;
  const W=cv.parentElement.clientWidth-32, H=cv.height||220;
  cv.width=W;
  const cx=cv.getContext('2d');
  cx.clearRect(0,0,W,H);
  const PT=24,PB=36,PL=30,PR=10;
  const CW=W-PL-PR, CH=H-PT-PB;
  const bw=Math.min(70, CW/items.length*0.55);
  const slot=CW/items.length;

  // Grid
  for(let v=0;v<=1;v+=0.2){
    const y=PT+CH*(1-v);
    cx.strokeStyle='#f1f5f9'; cx.lineWidth=1;
    cx.beginPath(); cx.moveTo(PL,y); cx.lineTo(W-PR,y); cx.stroke();
    cx.fillStyle='#94a3b8'; cx.font='9px Segoe UI'; cx.textAlign='right';
    cx.fillText((v*100).toFixed(0)+'%', PL-3, y+3);
  }

  items.forEach((item,i)=>{
    const vk = item.show_rate!==undefined?'show_rate':'close_rate';
    const ck = item.show_ci!==undefined?'show_ci':'close_ci';
    const val=item[vk], ci=item[ck];
    if(val==null) return;
    const x=PL+i*slot+slot/2;
    const bh=CH*val, by=PT+CH-bh;
    cx.fillStyle=palette[i]||'#0ea5e9';
    cx.fillRect(x-bw/2,by,bw,bh);
    // CI
    if(ci&&ci[0]!=null){
      const y0=PT+CH*(1-ci[1]), y1=PT+CH*(1-ci[0]);
      cx.strokeStyle='#1e293b'; cx.lineWidth=1.5;
      cx.beginPath(); cx.moveTo(x-5,y0); cx.lineTo(x+5,y0); cx.stroke();
      cx.beginPath(); cx.moveTo(x-5,y1); cx.lineTo(x+5,y1); cx.stroke();
      cx.beginPath(); cx.moveTo(x,y0);   cx.lineTo(x,y1);   cx.stroke();
    }
    cx.fillStyle='#1e293b'; cx.font='bold 10px Segoe UI'; cx.textAlign='center';
    cx.fillText((val*100).toFixed(1)+'%', x, by-5);
    const lbl=(item.label||item.type||'').replace('Database Prospect','DB Prospect').replace('-Generated','-Gen');
    cx.fillStyle='#64748b'; cx.font='10px Segoe UI';
    cx.fillText(lbl, x, H-PB+14);
  });
}

// ── Draw all charts for a tab ─────────────────────────────────────────────────
function drawAll(tab){
  const ls = D.lead_sources;
  const cl = D.clinics;
  const pt = D.patients;
  const orig = D.origin;

  if(tab==='lead'){
    const srSorted=[...ls].filter(d=>d.show_rate!=null).sort((a,b)=>b.show_rate-a.show_rate);
    hbarCI('cv-sr', srSorted.map(d=>({label:d.category,value:d.show_rate,ci:d.show_ci,color:gCol(d.group)})));
    const vSorted=[...ls].sort((a,b)=>b.tests-a.tests);
    hbar('cv-vol', vSorted.map(d=>({label:d.category,value:d.tests,color:gCol(d.group)})));
  }
  if(tab==='clinics'){
    const cSorted=[...cl].filter(d=>d.show_rate!=null).sort((a,b)=>b.show_rate-a.show_rate);
    hbarCI('cv-csr', cSorted.map(d=>({label:d.name,value:d.show_rate,ci:d.show_ci,color:'#0ea5e9'})));
    const uSorted=[...cl].sort((a,b)=>b.ha_units-a.ha_units);
    hbar('cv-cu', uSorted.map(d=>({label:d.name,value:d.ha_units,color:'#8b5cf6'})));
  }
  if(tab==='patients'){
    vbarCI('cv-ptsr', pt.map(d=>({...d,show_rate:d.show_rate,show_ci:d.show_ci})), ['#0ea5e9','#22c55e','#f59e0b']);
    vbarCI('cv-ptcr', pt.map(d=>({...d,close_rate:d.close_rate,close_ci:d.close_ci})), ['#0ea5e9','#22c55e','#f59e0b']);
  }
  if(tab==='origin'){
    const oa = Object.entries(orig).map(([k,v],i)=>({...v,label:k,color:i===0?'#22c55e':'#f87171'}));
    vbarCI('cv-osr', oa.map(d=>({...d,show_rate:d.show_rate,show_ci:d.show_ci})), oa.map(d=>d.color));
    vbarCI('cv-ocr', oa.map(d=>({...d,close_rate:d.close_rate,close_ci:d.close_ci})), oa.map(d=>d.color));
  }
}

// ── ANALYSIS & BACKTEST ───────────────────────────────────────────────────────

function renderAnalysis(){
  const corr = D.correlations;
  const bt   = D.backtest;
  const models = bt.models;

  // Which model won? (lowest Brier score)
  const winner = Object.entries(models).sort((a,b)=>a[1].brier-b[1].brier)[0];

  document.getElementById('p-analysis').innerHTML = `
    <p class="sec-note">
      Correlation analysis uses appointment-level Sales Funnel data (n=${f.n(bt.n_train+bt.n_test)} resolved appointments, sample).
      Backtest: train on appointments before ${bt.cutoff}, test on ${f.n(bt.n_test)} appointments on/after.
      Actual shows in test period: <strong>${bt.actual_shows}</strong>.
    </p>

    <div class="section-title" style="font-size:13px;font-weight:600;margin-bottom:12px">
      Show Rate Drivers &mdash; 95% Wilson CI
    </div>
    <div class="corr-grid" id="corr-charts"></div>

    <div class="section-title" style="font-size:13px;font-weight:600;margin:20px 0 10px">
      Backtest &mdash; Model Comparison
    </div>
    <p class="sec-note">
      Train: ${f.n(bt.n_train)} appointments (before ${bt.cutoff}) &nbsp;&middot;&nbsp;
      Test: ${f.n(bt.n_test)} appointments &nbsp;&middot;&nbsp;
      Actual shows: ${bt.actual_shows}<br>
      Model A = equal-weight from training sample &nbsp;|&nbsp;
      Model B = recency-weighted (30-day half-life) from training sample &nbsp;|&nbsp;
      Model C = full JAN&ndash;MAR AJ CSV rates (held out from sample-based training)
    </p>

    <div style="display:flex;gap:16px;align-items:baseline;flex-wrap:wrap;margin-bottom:10px">
      <div class="bt-winner">\u2713 Best show model (Brier): ${winner[1].label}</div>
      ${(()=>{
        const revEntries = [...Object.entries(models).map(([k,m])=>[k,m.rev_mae]),
                           ['EV',bt.rev_mae_ev]];
        const revWinner = revEntries.sort((a,b)=>a[1]-b[1])[0];
        const revLabel  = revWinner[0]==='EV' ? 'EV/Test' : `Model ${revWinner[0]} (${models[revWinner[0]].label})`;
        return `<div class="bt-winner" style="background:#ecfdf5;border-left-color:#10b981">\u2713 Best revenue model (MAE): ${revLabel}</div>`;
      })()}
    </div>

    <p style="font-size:11px;color:#64748b;margin-bottom:12px">
      Actual revenue proxy: <strong>${f.$(bt.actual_rev)}</strong>
      &nbsp;(${bt.actual_sales} sales &times; ${bt.ha_per_sale} HA/sale &times; ASP ${f.$(bt.asp_used)})
      &nbsp;&middot;&nbsp; Revenue proxy uses global JAN&ndash;MAR ASP &mdash; clinic-level ASP variation not captured.
    </p>

    <div class="bt-model-grid">
      ${Object.entries(models).map(([key,m])=>{
        const isBest = key===winner[0];
        return `<div class="bt-card ${isBest?'best':''}">
          <div class="bt-lbl">Model ${key}${isBest?' \u2605':''}</div>
          <div style="font-size:11px;color:#475569;margin-bottom:8px">${m.label}</div>
          <div class="bt-val">${m.pred}</div>
          <div class="bt-sub">predicted shows</div>
          <div style="margin-top:8px;font-size:11px">
            <span class="corr-stat">Brier: ${m.brier}</span>
            <span class="corr-stat">Show MAE: ${m.mae}</span>
          </div>
          <div style="margin-top:6px;border-top:1px solid #e2e8f0;padding-top:6px;font-size:11px">
            <span class="corr-stat" style="color:#6366f1">Rev Pred: ${f.$(m.pred_rev)}</span>
            <span class="corr-stat" style="color:#e11d48">Rev MAE: ${f.$(m.rev_mae)}</span>
          </div>
        </div>`;
      }).join('')}
      <div class="bt-card">
        <div class="bt-lbl">EV/Test</div>
        <div style="font-size:11px;color:#475569;margin-bottom:8px">Contracted $/test &times; appointments (no show model)</div>
        <div class="bt-val">${f.$(bt.pred_rev_ev)}</div>
        <div class="bt-sub">predicted revenue</div>
        <div style="margin-top:6px;font-size:11px">
          <span class="corr-stat" style="color:#e11d48">Rev MAE: ${f.$(bt.rev_mae_ev)}</span>
        </div>
      </div>
    </div>

    <div class="section-title" style="font-size:13px;font-weight:600;margin:16px 0 8px">Show Accuracy \u2014 By Clinic</div>
    <div class="twrap">
      <table id="tbl-bt">
        <thead><tr>
          <th data-num="false">Clinic</th><th>Test Appts</th><th>Actual Shows</th>
          <th>Model A Pred</th><th>Model A Err</th>
          <th>Model B Pred</th><th>Model B Err</th>
          <th>Model C Pred</th><th>Model C Err</th>
        </tr></thead>
        <tbody>
          <tr style="background:#f8fafc;font-weight:600">
            <td>TOTAL</td>
            <td>${f.n(bt.clinic_rows.reduce((a,r)=>a+r.n,0))}</td>
            <td>${bt.actual_shows}</td>
            <td>${models.A.pred}</td>
            <td>${errCell(models.A.pred-bt.actual_shows)}</td>
            <td>${models.B.pred}</td>
            <td>${errCell(models.B.pred-bt.actual_shows)}</td>
            <td>${models.C.pred}</td>
            <td>${errCell(models.C.pred-bt.actual_shows)}</td>
          </tr>
          ${bt.clinic_rows.map(r=>`<tr>
            <td>${r.clinic}</td>
            <td data-v="${r.n}">${r.n}</td>
            <td data-v="${r.actual}">${r.actual}</td>
            <td data-v="${r.predA}">${r.predA}</td>
            <td>${errCell(r.predA-r.actual)}</td>
            <td data-v="${r.predB}">${r.predB}</td>
            <td>${errCell(r.predB-r.actual)}</td>
            <td data-v="${r.predC}">${r.predC}</td>
            <td>${errCell(r.predC-r.actual)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>

    <div class="section-title" style="font-size:13px;font-weight:600;margin:16px 0 8px">Revenue Accuracy \u2014 By Clinic</div>
    <div class="twrap">
      <table id="tbl-bt-rev">
        <thead><tr>
          <th data-num="false">Clinic</th>
          <th>Actual Sales</th><th>Actual Rev (proxy)</th>
          <th>Model A Rev</th><th>Model A Rev Err</th>
          <th>Model B Rev</th><th>Model B Rev Err</th>
          <th>EV/Test Rev</th><th>EV/Test Err</th>
        </tr></thead>
        <tbody>
          <tr style="background:#f8fafc;font-weight:600">
            <td>TOTAL</td>
            <td>${bt.actual_sales}</td>
            <td>${f.$(bt.actual_rev)}</td>
            <td>${f.$(models.A.pred_rev)}</td>
            <td>${errRevCell(models.A.pred_rev - bt.actual_rev)}</td>
            <td>${f.$(models.B.pred_rev)}</td>
            <td>${errRevCell(models.B.pred_rev - bt.actual_rev)}</td>
            <td>${f.$(bt.pred_rev_ev)}</td>
            <td>${errRevCell(bt.pred_rev_ev - bt.actual_rev)}</td>
          </tr>
          ${bt.clinic_rows.map(r=>`<tr>
            <td>${r.clinic}</td>
            <td data-v="${r.sold}">${r.sold}</td>
            <td data-v="${r.actual_rev}">${f.$(r.actual_rev)}</td>
            <td data-v="${r.revA}">${f.$(r.revA)}</td>
            <td>${errRevCell(r.revA - r.actual_rev)}</td>
            <td data-v="${r.revB}">${f.$(r.revB)}</td>
            <td>${errRevCell(r.revB - r.actual_rev)}</td>
            <td data-v="${r.revEV}">${f.$(r.revEV)}</td>
            <td>${errRevCell(r.revEV - r.actual_rev)}</td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>

    <div class="fc-method">
      <strong>Backtest methodology</strong>
      Predictions use only information available before the test-period cutoff (${bt.cutoff}).
      Model A: simple show rate by (clinic &times; confirmation status) from training data.
      Model B: same split but exponentially weighted toward recent training appointments
      (30-day half-life from cutoff date &mdash; appointments 30 days prior to cutoff get &frac12; the weight of cutoff-day appointments).
      Model C: full JAN&ndash;MAR clinic-level confirmed/non-confirmed rates from appointment
      journey CSV &mdash; not trained on this sample, used as the &ldquo;production prior&rdquo; benchmark.
      Brier score: mean squared error of predicted P(show) vs actual 0/1 (lower = better, max 0.25 = random).
      <br><br>
      <strong>Revenue backtest:</strong>
      Funnel models (A/B/C) predict revenue as p_show &times; ${D.rates.test_rate*100}% tested &times; p_close &times; ${D.rates.ha_per_sale} HA/sale &times; ASP ${f.$(bt.asp_used)},
      where p_close comes from the same category &rarr; creator-role &rarr; region hierarchy used in the Forecast tab.
      EV/Test uses contracted $/test from JAN&ndash;MAR Reporting Dimension applied to each test appointment &mdash; no show model required.
      Actual revenue proxy = actual sales &times; ${bt.ha_per_sale} HA/sale &times; ${f.$(bt.asp_used)} ASP (global average; clinic ASP variation not captured).
      Revenue accuracy at the clinic level is noisier than region total due to small sale counts.
    </div>`;

  makeSortable('tbl-bt');
  makeSortable('tbl-bt-rev');
  drawCorrCharts();
}

function errCell(err){
  const cls = err>0.3?'err-pos':err<-0.3?'err-neg':'err-zero';
  const sign = err>0?'+':'';
  return `<span class="${cls}">${sign}${err.toFixed(1)}</span>`;
}
function errRevCell(err){
  // Revenue errors in dollars — threshold ±$500 for colour coding
  const cls = err>500?'err-pos':err<-500?'err-neg':'err-zero';
  const sign = err>0?'+':'';
  return `<span class="${cls}">${sign}${f.$(Math.round(err))}</span>`;
}

function drawCorrCharts(){
  document.getElementById('corr-charts').innerHTML = D.correlations.map((corr,ci)=>{
    const sig = corr.pval!=null && corr.pval<0.05;
    const stats = [];
    if(corr.or!=null)        stats.push(`<span class="corr-stat">OR: ${corr.or} [${corr.or_ci[0]}\u2013${corr.or_ci[1]}]</span>`);
    if(corr.z!=null)         stats.push(`<span class="corr-stat">z: ${corr.z}</span>`);
    if(corr.pval!=null)      stats.push(`<span class="corr-stat ${sig?'sig':'ns'}">${sig?'p < 0.05':'p \u2265 0.05'} (p=${corr.pval})</span>`);
    if(corr.pearson_r!=null) stats.push(`<span class="corr-stat">r: ${corr.pearson_r}</span>`);
    return `<div class="corr-card">
      <h3>${corr.feature}</h3>
      <div class="corr-note">${corr.note}</div>
      <div style="margin-bottom:6px">${stats.join('')}</div>
      <canvas id="cv-corr-${ci}"></canvas>
    </div>`;
  }).join('');

  // Draw each chart after DOM is ready
  D.correlations.forEach((corr,ci)=>{
    const items = corr.levels.map(lv=>({
      label: lv.label,
      value: lv.show_rate,
      ci:    lv.ci,
      color: '#0ea5e9',
      n:     lv.n,
    }));
    hbarCI(`cv-corr-${ci}`, items);
  });
}

// ── FORECAST ──────────────────────────────────────────────────────────────────

function renderForecast(){
  document.getElementById('p-forecast').innerHTML = `
    <p class="sec-note">Upload next week's scheduled appointments (CSV, same Looker export format as Sales Funnel).
    Predictions use JAN\u2013MAR historical show & close rates by lead source and confirmation status.</p>
    <div class="drop-zone" id="drop-zone"
         ondragover="event.preventDefault();this.classList.add('over')"
         ondragleave="this.classList.remove('over')"
         ondrop="handleDrop(event)">
      <div class="drop-title">Drop weekly appointments CSV here</div>
      <div class="drop-sub">
        Export from Looker: same columns as Sales Funnel, filtered to scheduled/active appointments only.<br>
        Confirmation status and lead source will sharpen the prediction.
      </div>
      <button class="btn-up" onclick="document.getElementById('fc-input').click()">Choose File</button>
      <input type="file" id="fc-input" accept=".csv" style="display:none" onchange="handleFile(this.files[0])">
      <div class="fc-file-info" id="fc-file-info"></div>
    </div>
    <div id="fc-results"></div>
    <div id="fc-history"></div>`;
}

function handleDrop(e){
  e.preventDefault();
  document.getElementById('drop-zone').classList.remove('over');
  const file = e.dataTransfer.files[0];
  if(file) handleFile(file);
}

function handleFile(file){
  if(!file || !file.name.endsWith('.csv')){
    alert('Please upload a CSV file.'); return;
  }
  document.getElementById('fc-file-info').textContent = '\u2714 ' + file.name;
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const rows = parseCSV(e.target.result);
      const preds = predictAppointments(rows);
      showForecastResults(preds, file.name);
    } catch(err) {
      document.getElementById('fc-results').innerHTML =
        `<div class="fc-warn">Error parsing file: ${err.message}. Check that the file is a CSV with the Sales Funnel column headers.</div>`;
    }
  };
  reader.readAsText(file);
}

// ── CSV parser ────────────────────────────────────────────────────────────────
function parseCSVLine(line){
  const cells=[]; let cell='', inQ=false;
  for(let i=0;i<line.length;i++){
    const c=line[i];
    if(c==='"'){inQ=!inQ;}
    else if(c===','&&!inQ){cells.push(cell.trim());cell='';}
    else{cell+=c;}
  }
  cells.push(cell.trim());
  return cells;
}

// ── PHI column patterns — never mapped to row objects ────────────────────────
// Columns whose headers match these patterns are silently ignored during parsing.
// This ensures patient names, chart numbers, DOBs, and contact info never enter
// JS memory, even if present in the uploaded CSV.
function parseCSV(text){
  const lines = text.replace(/\r\n/g,'\n').replace(/\r/g,'\n').split('\n').filter(l=>l.trim());
  if(lines.length<2) throw new Error('File appears empty');
  const raw_headers = parseCSVLine(lines[0]);
  const headers = raw_headers.map(h=>h.toLowerCase().replace(/[\[\]#$%]/g,'').trim());

  const find = (...pats) => {
    for(const p of pats){
      const i = headers.findIndex(h=>h.includes(p));
      if(i>=0) return i;
    }
    return -1;
  };

  const idx = {
    clinic:         find('clinic name'),
    patient_type:   find('historical contact type code 2','contact type code 2'),
    appt_status:    find('appointment status'),
    outcome:        find('appointment outcome'),
    created_by:     find('created-by logged role','created by logged role'),
    campaign:       find('campaign activity code and name','campaign activity'),
    confirmed_24hr: find('24hr confirmation date','24hr confirmation'),
    confirmed_72hr: find('72hr confirmation date','72hr confirmation'),
    start_date:     find('start date'),
    created_date:   find('created date'),
    biz_days:       find('business days duration','business days'),
  };

  const rows=[];
  for(let i=1;i<lines.length;i++){
    if(!lines[i].trim()) continue;
    const cells=parseCSVLine(lines[i]);
    const row={};
    for(const [key,col] of Object.entries(idx)){
      row[key] = col>=0 ? (cells[col]||'') : '';
    }
    rows.push(row);
  }
  return rows;
}

// ── Campaign → category (JS mirror of Python keyword rules) ──────────────────
function campaignToCategory(name){
  if(!name) return null;
  const n=name.toLowerCase();
  const rules=[
    [/walk.?in|walk in|clinic only|general.*clinic/,'Walk In'],
    [/service(?!.*conv)/,'Walk In'],
    [/physician|medical ref/,'Medical Referral'],
    [/patient.?ref|referral.*(patient|wom)|word of mouth/,'WOM Referral'],
    [/wellness/,'Wellness Referral'],
    [/fitting fee/,'Fitting Fee'],
    [/\bevent/,'Events'],
    [/service.?conv|conversion/,'Service Conversion'],
    [/direct.?mail|oow|out.?of.?warrant|dm follow|tns.*direct/,'DM'],
    [/google|paid.?search/,'Paid Search'],
    [/social media|facebook|instagram/,'Social'],
    [/organic.?search|\bseo\b/,'SEO'],
    [/verified digital|hearinglife.*website|digital.*website|internet search|digital.*search|ag7z|\bweb\b/,'Direct Web'],
    [/insurance|inbound/,'Inbound Call'],
    [/psc|tns(?!.*direct)|recall|proactive|follow.?up|no.?show.*cancel|outbound/,'Outbound Call'],
    [/\bdisplay\b/,'Display'],
    [/\bemail\b/,'Email'],
    [/\btv\b|television|cable/,'TV'],
    [/affiliate/,'Affiliate'],
    [/sms|text message/,'SMS / Text Message'],
    [/door drop/,'Door Drop'],
    [/print|newspaper|magazine/,'Print Ad'],
  ];
  for(const [re,cat] of rules) if(re.test(n)) return cat;
  return null;
}

// ── Date helpers ─────────────────────────────────────────────────────────────
// new Date("2026-04-07") parses as UTC midnight → local getDay() shifts by timezone offset.
// Always parse date-only strings as LOCAL time by constructing from parts.
function parseDateLocal(s){
  if(!s) return null;
  // Handle "2026-04-07", "2026-04-07 09:00", "2026-04-07T09:00:00", "04/07/2026" etc.
  const iso = /^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})/;
  const mdy = /^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})/;
  let m;
  if((m=iso.exec(s)))   return new Date(+m[1], +m[2]-1, +m[3]);
  if((m=mdy.exec(s)))   return new Date(+m[3], +m[1]-1, +m[2]);
  return new Date(s); // fallback (for ISO datetime strings with time zone info)
}

// ── Prediction engine ─────────────────────────────────────────────────────────
const ACTIVE_STATUSES = new Set(['scheduled','confirmed','confirmed 24 hours','confirmed 72 hours',
  'not confirmed - attempted','welcome call - completed','arrived','active']);

function predictAppointments(rows){
  // Pre-compute SDF% lookup by category (built once, used per appointment)
  const _sdfMap = {};
  for(const c of (D.revenue_timing?.categories||[])){
    if(c.sdf_pct != null) _sdfMap[c.category] = c.sdf_pct;
  }
  const _defSdf = D.revenue_timing?.sdf_pct ?? 0.49;

  // Include all non-completed appointments.
  // Cancelled tests still count in the EV/test denominator — the rate was calibrated on ALL
  // scheduled tests (active + cancelled). Exclude only Completed (already happened this period).
  const eligible = rows.filter(r=>{
    const s=(r.appt_status||'').toLowerCase().trim();
    if(!s) return true; // unknown status — include
    return !s.startsWith('complet');  // keep active AND cancelled; drop completed only
  });

  return eligible.map(row=>{
    const clinic     = row.clinic||'';
    const created_by = row.created_by||'';
    const campaign   = row.campaign||'';
    const start_date = row.start_date||'';
    const created_date = row.created_date||'';
    const biz_days   = row.biz_days||'';

    const status     = (row.appt_status||'').toLowerCase().trim();
    const is_cancelled = status.startsWith('cancel');
    // Cancelled can't be confirmed; short-circuit to avoid false positive
    const confirmed  = !is_cancelled &&
                   ((row.confirmed_24hr&&row.confirmed_24hr.trim()!==''&&row.confirmed_24hr.trim()!=='NaT')
                   || (row.confirmed_72hr&&row.confirmed_72hr.trim()!==''&&row.confirmed_72hr.trim()!=='NaT'));

    // ── Show probability ──────────────────────────────────────────────────────
    // Cancelled: p_show = 0 (won't show). EV/test still applies at full rate because
    // the historical rate was computed with all scheduled tests in the denominator.
    const cr = D.rates.clinics[clinic];
    let p_show;
    if(is_cancelled){
      p_show = 0;
    } else if(cr && cr.confirmed!=null && cr.nonconfirmed!=null){
      p_show = confirmed ? cr.confirmed : cr.nonconfirmed;
    } else {
      p_show = confirmed ? D.rates.region.confirmed : D.rates.region.nonconfirmed;
    }

    // ── Close probability ─────────────────────────────────────────────────────
    // Use most specific available: campaign → category → creator role → region
    const cat = D.rates.campaign_map[campaign] || campaignToCategory(campaign);
    const catData = cat ? D.rates.categories[cat] : null;
    const is_clinic_gen = ['Dispenser','Clinic Assistant'].includes(created_by);
    const group = catData ? catData.group : (is_clinic_gen ? 'clinic' : (created_by ? 'external' : 'unknown'));

    let p_close;
    if(catData && catData.close!=null)           p_close = catData.close;
    else if(group==='clinic')                    p_close = D.rates.origin.clinic.close;
    else if(group==='external')                  p_close = D.rates.origin.external.close;
    else                                         p_close = D.rates.region.close;

    const p_sale = p_show * D.rates.aidable_rate * p_close;

    // ── Revenue — Funnel method ───────────────────────────────────────────────
    // If this appointment shows: E[revenue | show] = test_rate × aidable_of_tested × close × ha × ASP
    // = aidable_rate × close × ha × ASP  (aidable_rate = test_rate × aidable_of_tested = 0.72)
    // Propagates show-rate uncertainty through to a revenue CI in showForecastResults.
    const rev_per_show = D.rates.aidable_rate * p_close * D.rates.ha_per_sale * D.rates.asp;

    // ── Revenue — EV/test method ──────────────────────────────────────────────
    // Directly-observed contracted_revenue ÷ tests_scheduled from historical RD.
    // More robust: collapses full funnel to a single $/test without compounding
    // test_rate × close_rate × ha_per_sale uncertainty.
    const _ev = D.rates.ev_per_test;
    const _eg = D.rates.ev_per_group;
    const ev_test = (cat && _ev[cat] != null) ? _ev[cat]
                  : (_eg[group] != null)       ? _eg[group]
                  :                              _eg.overall;

    // ── Day of week ───────────────────────────────────────────────────────────
    const _d = parseDateLocal(start_date);
    const dow = (!_d || isNaN(_d.getTime())) ? 'Unknown'
              : ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][_d.getDay()];

    // ── SDF% for this appointment's category ─────────────────────────────────
    const _sdfEntry = _sdfMap[cat||''];
    const sdf_pct = _sdfEntry != null ? _sdfEntry : _defSdf;

    return {
      clinic, group, cat: cat||'Unknown', confirmed, is_cancelled,
      patient_type: row.patient_type||'',
      start_date, created_date, biz_days,
      p_show:      Math.min(1, Math.max(0, p_show)),
      p_sale:      Math.min(1, Math.max(0, p_sale)),
      p_close,
      rev_per_show,   // E[revenue | shows] — used for funnel revenue CI
      ev_test,        // historical $/test — EV/test method
      dow,            // 'Mon'|'Tue'|'Wed'|'Thu'|'Fri'|other
      sdf_pct,        // category-weighted same-day-fit rate
    };
  });
}

// ── Poisson-binomial CI ───────────────────────────────────────────────────────
// Correct CI for a sum of independent Bernoulli trials with different p's
function pbCI(ps){
  const mean = ps.reduce((a,p)=>a+p, 0);
  const std  = Math.sqrt(ps.reduce((a,p)=>a+p*(1-p), 0));
  return {
    mean: Math.round(mean*10)/10,
    lo:   Math.max(0, Math.round((mean-1.96*std)*10)/10),
    hi:   Math.round((mean+1.96*std)*10)/10,
  };
}

// ── Daily revenue forecast ────────────────────────────────────────────────────
const WEEK_DAYS = ['Mon','Tue','Wed','Thu','Fri'];

function computeDailyRevenue(preds){
  // Close-rate uncertainty: scale contracted mean up/down by regional close CI bounds
  const closeMean = D.totals.close_rate || 0.425;
  const closeLo   = D.totals.close_ci?.[0] ?? closeMean * 0.88;
  const closeHi   = D.totals.close_ci?.[1] ?? closeMean * 1.12;
  const clScLo = closeLo / closeMean;
  const clScHi = closeHi / closeMean;

  // ── Same-week creation expected adds per appointment day ─────────────────────
  // For each clinic in the uploaded file, use clinic-specific rate if available;
  // fall back to 50% of regional average for low-volume clinics (None in data).
  // Then sum across all clinics in the file and add regional for any clinic
  // not in the historical data at all.
  const swData = D.same_week_creation || {};
  const swReg  = swData.region || {};
  const swClin = swData.by_clinic || {};
  // Unique clinics in the file
  const fileClinics = [...new Set(preds.map(p=>p.clinic).filter(Boolean))];

  const daily = {};
  for(const day of WEEK_DAYS){
    const dp = preds.filter(p => p.dow === day);
    // Revenue mean & show-uncertainty SD
    const mean    = dp.reduce((a,p)=>a + p.p_show * p.rev_per_show, 0);
    const showVar = dp.reduce((a,p)=>a + p.p_show*(1-p.p_show)*p.rev_per_show*p.rev_per_show, 0);
    const showSD  = Math.sqrt(showVar);
    // Revenue-weighted SDF% for this day's mix
    const sdf = mean > 0
      ? dp.reduce((a,p)=>a + p.p_show * p.rev_per_show * p.sdf_pct, 0) / mean
      : (D.revenue_timing?.sdf_pct ?? 0.49);

    // Same-week expected test additions for this appointment day
    // Sum clinic-level estimates; use 50% of regional where clinic not in history
    let sw_cg = 0, sw_ext = 0;
    for(const cl of fileClinics){
      const clData = swClin[cl];
      if(clData === null){
        // Low-volume clinic: 50% of regional average
        sw_cg  += (swReg[day]?.clinic_gen     || 0) * 0.5;
        sw_ext += (swReg[day]?.non_clinic_gen || 0) * 0.5;
      } else if(clData && clData[day]){
        sw_cg  += clData[day].clinic_gen     || 0;
        sw_ext += clData[day].non_clinic_gen || 0;
      } else {
        // Clinic not in historical data at all — use 50% of regional
        sw_cg  += (swReg[day]?.clinic_gen     || 0) * 0.5;
        sw_ext += (swReg[day]?.non_clinic_gen || 0) * 0.5;
      }
    }
    // If no clinics in file yet, fall back to full regional
    if(!fileClinics.length){
      sw_cg  = swReg[day]?.clinic_gen     || 0;
      sw_ext = swReg[day]?.non_clinic_gen || 0;
    }

    // EV revenue from expected same-week creates
    const sw_ev_cg  = Math.round(sw_cg  * (D.rates.ev_per_group?.clinic   || 0));
    const sw_ev_ext = Math.round(sw_ext * (D.rates.ev_per_group?.external || 0));

    daily[day] = {
      n: dp.filter(p=>!p.is_cancelled).length,
      n_cancelled: dp.filter(p=>p.is_cancelled).length,
      contracted: {
        lo:   Math.max(0, Math.round((mean - 1.96*showSD) * clScLo)),
        mean: Math.round(mean),
        hi:   Math.round((mean + 1.96*showSD) * clScHi),
      },
      sdf,
      sw_cg:  Math.round(sw_cg  * 10) / 10,   // expected clinic-gen additions
      sw_ext: Math.round(sw_ext * 10) / 10,   // expected non-clinic additions
      sw_ev_cg, sw_ev_ext,                     // EV revenue from those adds
    };
  }
  return daily;
}

function renderDailyRevSection(preds){
  const daily = computeDailyRevenue(preds);
  window._dailyContracted = daily;

  const closeMean = D.totals.close_rate || 0.425;
  const closeLo   = D.totals.close_ci?.[0] ?? closeMean * 0.88;
  const closeHi   = D.totals.close_ci?.[1] ?? closeMean * 1.12;

  document.getElementById('fc-daily').innerHTML = `
    <div class="section-title" style="font-size:13px;font-weight:600;margin:20px 0 8px">
      Daily Revenue Forecast
    </div>
    <p class="sec-note">
      <strong>Contracted</strong> = new sales committed on each appointment day (funnel model).
      <strong>Invoiced</strong> = contracted &times; SDF% (same-day fit) + delivery of prior-period orders entered below.
      Range uses show-rate Poisson-binomial uncertainty (&plusmn;1.96&sigma;) combined with
      close-rate CI [${f.pct(closeLo)}&ndash;${f.pct(closeHi)}] vs mean ${f.pct(closeMean)}.
    </p>

    <div style="background:#fff;border-radius:8px;padding:14px 18px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:14px">
      <div style="font-size:10px;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">
        Prior-Period Fitting Deliveries &mdash; enter exact invoiced revenue ($) per day
      </div>
      <div style="display:flex;gap:16px;flex-wrap:wrap;align-items:flex-end">
        ${WEEK_DAYS.map(d=>`
          <div style="display:flex;flex-direction:column;gap:3px;align-items:center">
            <label style="font-size:10px;color:#64748b;font-weight:600">${d}</label>
            <div style="display:flex;align-items:center;gap:2px">
              <span style="font-size:12px;color:#64748b;font-weight:600">$</span>
              <input id="fit-${d}" type="number" min="0" step="100" value="0"
                style="width:72px;padding:5px;border:1px solid #d1d5db;border-radius:6px;text-align:right;font-size:13px;font-weight:600"
                oninput="updateDailyInvoiced()">
            </div>
          </div>`).join('')}
        <div style="width:1px;height:36px;background:#e2e8f0;align-self:center"></div>
        <div style="display:flex;flex-direction:column;gap:3px;align-items:center">
          <label style="font-size:10px;color:#64748b;font-weight:600">Week&nbsp;(unscheduled&nbsp;fittings)</label>
          <div style="display:flex;align-items:center;gap:2px">
            <span style="font-size:12px;color:#64748b;font-weight:600">$</span>
            <input id="fit-week" type="number" min="0" step="100" value="0"
              style="width:90px;padding:5px;border:1px solid #d1d5db;border-radius:6px;text-align:right;font-size:13px;font-weight:600"
              oninput="updateDailyInvoiced()">
          </div>
        </div>
      </div>
    </div>

    <div class="twrap" style="overflow-x:auto;margin-bottom:6px">
      <table style="min-width:640px">
        <thead>
          <tr style="background:#f8fafc">
            <th data-num="false" style="min-width:140px"></th>
            ${WEEK_DAYS.map(d=>{
              const can = daily[d].n_cancelled>0 ? `<span style="color:#dc2626"> +${daily[d].n_cancelled}✗</span>` : '';
              const sw  = daily[d].sw_cg + daily[d].sw_ext;
              const swNote = sw>0 ? `<div style="font-size:9px;color:#7c3aed;margin-top:1px">+${sw.toFixed(1)} same-wk</div>` : '';
              return `<th>${d}
                <div style="font-size:9px;font-weight:400;color:#94a3b8;margin-top:1px">${daily[d].n} active${can}</div>
                ${swNote}
              </th>`;
            }).join('')}
            <th>Week&nbsp;Total</th>
          </tr>
        </thead>
        <tbody>
          <!-- Same-week creation rows -->
          <tr>
            <td colspan="${WEEK_DAYS.length+2}"
                style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
                       color:#7c3aed;background:#faf5ff;padding:5px 10px;border-bottom:none">
              Expected Same-Week Test Additions
              <span style="font-weight:400;font-size:9px">(historical avg · not yet in file)</span>
            </td>
          </tr>
          <tr style="background:#faf5ff">
            <td style="font-size:10px;color:#7c3aed;padding-left:18px">Clinic-gen</td>
            ${WEEK_DAYS.map(d=>`<td style="font-size:11px;color:#7c3aed">
              ${daily[d].sw_cg.toFixed(1)}
              <div style="font-size:8px;color:#a78bfa">${f.$(daily[d].sw_ev_cg)}</div>
            </td>`).join('')}
            <td id="wk-sw-cg" style="font-size:11px;color:#7c3aed"></td>
          </tr>
          <tr style="background:#faf5ff">
            <td style="font-size:10px;color:#7c3aed;padding-left:18px">Non-clinic</td>
            ${WEEK_DAYS.map(d=>`<td style="font-size:11px;color:#7c3aed">
              ${daily[d].sw_ext.toFixed(1)}
              <div style="font-size:8px;color:#a78bfa">${f.$(daily[d].sw_ev_ext)}</div>
            </td>`).join('')}
            <td id="wk-sw-ext" style="font-size:11px;color:#7c3aed"></td>
          </tr>
          <tr><td colspan="${WEEK_DAYS.length+2}" style="padding:0;height:6px;background:#f1f5f9"></td></tr>

          <!-- Contracted rows -->
          <tr>
            <td colspan="${WEEK_DAYS.length+2}"
                style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
                       color:#15803d;background:#f0fdf4;padding:5px 10px;border-bottom:none">
              Contracted Revenue &nbsp;<span style="font-weight:400;font-size:9px">(scheduled appointments only)</span>
            </td>
          </tr>
          <tr style="background:#f0fdf4">
            <td style="font-size:11px;color:#15803d;padding-left:18px">&#9650; High</td>
            ${WEEK_DAYS.map(d=>`<td style="color:#15803d">${f.$(daily[d].contracted.hi)}</td>`).join('')}
            <td id="wk-con-hi" style="font-weight:600;color:#15803d"></td>
          </tr>
          <tr style="background:#f0fdf4">
            <td style="font-weight:700;padding-left:18px">Most Likely</td>
            ${WEEK_DAYS.map(d=>`<td style="font-weight:700">${f.$(daily[d].contracted.mean)}</td>`).join('')}
            <td id="wk-con-mean" style="font-weight:700"></td>
          </tr>
          <tr style="background:#f0fdf4">
            <td style="font-size:11px;color:#94a3b8;padding-left:18px">&#9660; Low</td>
            ${WEEK_DAYS.map(d=>`<td style="color:#94a3b8">${f.$(daily[d].contracted.lo)}</td>`).join('')}
            <td id="wk-con-lo" style="color:#94a3b8"></td>
          </tr>
          <tr><td colspan="${WEEK_DAYS.length+2}" style="padding:0;height:6px;background:#f1f5f9"></td></tr>

          <!-- Invoiced rows -->
          <tr>
            <td colspan="${WEEK_DAYS.length+2}"
                style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
                       color:#1d4ed8;background:#eff6ff;padding:5px 10px;border-bottom:none">
              Invoiced Revenue &nbsp;<span style="font-weight:400;font-size:9px">(contracted &times; SDF% + fitting inputs)</span>
            </td>
          </tr>
          <tr style="background:#eff6ff">
            <td style="font-size:11px;color:#1d4ed8;padding-left:18px">&#9650; High</td>
            ${WEEK_DAYS.map(d=>`<td id="inv-${d}-hi" style="color:#1d4ed8"></td>`).join('')}
            <td id="wk-inv-hi" style="font-weight:600;color:#1d4ed8"></td>
          </tr>
          <tr style="background:#eff6ff">
            <td style="font-weight:700;padding-left:18px">Most Likely</td>
            ${WEEK_DAYS.map(d=>`<td id="inv-${d}-mean" style="font-weight:700"></td>`).join('')}
            <td id="wk-inv-mean" style="font-weight:700"></td>
          </tr>
          <tr style="background:#eff6ff">
            <td style="font-size:11px;color:#94a3b8;padding-left:18px">&#9660; Low</td>
            ${WEEK_DAYS.map(d=>`<td id="inv-${d}-lo" style="color:#94a3b8"></td>`).join('')}
            <td id="wk-inv-lo" style="color:#94a3b8"></td>
          </tr>
          <tr><td colspan="${WEEK_DAYS.length+2}" style="padding:0;height:6px;background:#f1f5f9"></td></tr>

          <!-- Cancellation / return adjustments — week total only -->
          <tr>
            <td colspan="${WEEK_DAYS.length+2}"
                style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
                       color:#b45309;background:#fffbeb;padding:5px 10px;border-bottom:none">
              Week Adjustments &nbsp;<span style="font-weight:400;font-size:9px">(applied to totals only — too much day-level variance)</span>
            </td>
          </tr>
          <tr style="background:#fffbeb">
            <td style="font-size:10px;color:#b45309;padding-left:18px"
                title="Contracted × (1 − invoice_ratio) — historical rate of contracted revenue that doesn't convert to invoiced (${f.pct(PIPELINE_NONINVOICE_RATE)} YTD)">
              &minus; Non-invoiced contracted (${f.pct(PIPELINE_NONINVOICE_RATE)} of contracted)
            </td>
            ${WEEK_DAYS.map(()=>`<td style="color:#94a3b8;font-size:9px;text-align:center">&mdash;</td>`).join('')}
            <td id="wk-adj-cancel" style="color:#b45309;font-weight:600"></td>
          </tr>
          <tr style="background:#fffbeb">
            <td style="font-size:10px;color:#b45309;padding-left:18px" title="Invoiced × 6% — post-delivery returns/cancellations">
              &minus; Post-delivery returns (6% of invoiced)
            </td>
            ${WEEK_DAYS.map(()=>`<td style="color:#94a3b8;font-size:9px;text-align:center">&mdash;</td>`).join('')}
            <td id="wk-adj-returns" style="color:#b45309;font-weight:600"></td>
          </tr>
          <tr style="background:#fef3c7;border-top:2px solid #fcd34d">
            <td style="font-weight:700;padding-left:18px;font-size:12px">Net Invoiced (adj.)</td>
            ${WEEK_DAYS.map(()=>`<td style="color:#94a3b8;font-size:9px;text-align:center">&mdash;</td>`).join('')}
            <td id="wk-inv-net" style="font-weight:700;font-size:13px;color:#1a1a2e"></td>
          </tr>

          <!-- SDF row -->
          <tr>
            <td style="font-size:10px;color:#94a3b8;padding-left:18px">SDF% (revenue-wtd by appt mix)</td>
            ${WEEK_DAYS.map(d=>`<td style="font-size:10px;color:#94a3b8">${f.pct(daily[d].sdf)}</td>`).join('')}
            <td style="font-size:10px;color:#94a3b8">${f.pct(D.revenue_timing?.sdf_pct)}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <p class="sec-note">
      <strong>Same-week adds:</strong> historical average tests created during the week for each appointment day
      (clinic-gen / non-clinic-gen split). Per-clinic rates used where &ge;4 weeks of history; otherwise 50% of regional average.
      EV revenue shown in grey below each count.<br>
      <strong>Adjustments (week totals only):</strong>
      Non-invoiced = contracted &times; (1 &minus; invoice_ratio) — historical rate of contracted that doesn't convert to invoiced within the period (~${f.pct(PIPELINE_NONINVOICE_RATE)} YTD).
      Returns = invoiced &times; 6% (post-delivery YTD return rate).
      Net Invoiced = invoiced &minus; returns.
    </p>`;

  updateDailyInvoiced();

  // ── Clinic weekly drill-down ─────────────────────────────────────────────────
  // Build per-clinic, per-day breakdown
  const _cdd = {};
  for(const p of preds){
    if(!_cdd[p.clinic]){
      _cdd[p.clinic] = {};
      for(const d of WEEK_DAYS) _cdd[p.clinic][d] = {n:0,active:0,ev:0,fMean:0,fVar:0};
    }
    if(WEEK_DAYS.includes(p.dow)){
      const cd = _cdd[p.clinic][p.dow];
      cd.n++;
      if(!p.is_cancelled) cd.active++;
      cd.ev    += p.ev_test;
      cd.fMean += p.p_show * p.rev_per_show;
      cd.fVar  += p.p_show * (1-p.p_show) * p.rev_per_show**2;
    }
  }
  const _clinicNames = Object.keys(_cdd).sort((a,b)=>{
    return WEEK_DAYS.reduce((s,d)=>s+_cdd[b][d].n,0) - WEEK_DAYS.reduce((s,d)=>s+_cdd[a][d].n,0);
  });

  const _clinicDrillRows = _clinicNames.map(clinic => {
    const dayCells = WEEK_DAYS.map(day => {
      const cd = _cdd[clinic][day];
      if(!cd.n) return `<td style="color:#e2e8f0;text-align:center">—</td>`;
      const cancel = cd.n - cd.active;
      const cNote  = cancel>0 ? `<span style="color:#dc2626;font-size:8px"> ${cancel}✗</span>` : '';
      const ev     = Math.round(cd.ev);
      return `<td style="text-align:center;font-size:11px">
        <span style="font-weight:600">${cd.active}${cNote}</span>
        <div style="font-size:9px;color:#64748b">${f.$(ev)}</div>
      </td>`;
    }).join('');
    const totActive = WEEK_DAYS.reduce((s,d)=>s+_cdd[clinic][d].active,0);
    const totCan    = WEEK_DAYS.reduce((s,d)=>s+_cdd[clinic][d].n-_cdd[clinic][d].active,0);
    const totEV     = Math.round(WEEK_DAYS.reduce((s,d)=>s+_cdd[clinic][d].ev,0));
    const totFM     = Math.round(WEEK_DAYS.reduce((s,d)=>s+_cdd[clinic][d].fMean,0));
    const totFS     = Math.sqrt(WEEK_DAYS.reduce((s,d)=>s+_cdd[clinic][d].fVar,0));
    const totFL     = Math.max(0,Math.round(totFM-1.96*totFS));
    const totFH     = Math.round(totFM+1.96*totFS);
    const cNote     = totCan>0 ? `<span style="color:#dc2626;font-size:8px"> ${totCan}✗</span>` : '';
    return `<tr>
      <td style="font-size:11px;font-weight:600;white-space:nowrap">${clinic}</td>
      ${dayCells}
      <td style="font-weight:600;border-left:2px solid #e2e8f0;text-align:center;font-size:11px">
        ${totActive}${cNote}
        <div style="font-size:9px;color:#0284c7">${f.$(totEV)}</div>
        <div style="font-size:8px;color:#94a3b8">${f.$(totFL)}\u2013${f.$(totFH)}</div>
      </td>
    </tr>`;
  }).join('');

  const _drillDiv = document.createElement('div');
  _drillDiv.style.marginTop = '14px';
  _drillDiv.innerHTML = `
    <div class="section-title" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display==='none'?'':'none'"
      style="font-size:13px;font-weight:600;cursor:pointer;display:flex;align-items:center;gap:6px;margin-bottom:4px;user-select:none">
      <span id="fc-drill-arrow" style="color:#0ea5e9;font-size:10px">&#9654;</span>
      Clinic Weekly Breakdown
      <span style="font-size:10px;color:#94a3b8;font-weight:400">(click to expand)</span>
    </div>
    <div id="fc-drill-body" style="display:none">
      <p class="sec-note" style="margin-bottom:8px">
        Active tests per day (red = cancelled), EV/test revenue below count.
        Weekly total: blue = EV/test, grey range = funnel 95% CI.
        Less reliable for small-volume clinics.
      </p>
      <div class="twrap" style="overflow-x:auto">
        <table>
          <thead><tr>
            <th data-num="false">Clinic</th>
            ${WEEK_DAYS.map(d=>`<th>${d}</th>`).join('')}
            <th>Weekly Total</th>
          </tr></thead>
          <tbody>${_clinicDrillRows}</tbody>
        </table>
      </div>
    </div>`;
  _drillDiv.querySelector('.section-title').addEventListener('click', ()=>{
    const body  = document.getElementById('fc-drill-body');
    const arrow = document.getElementById('fc-drill-arrow');
    const open  = body.style.display !== 'none';
    body.style.display  = open ? 'none' : '';
    arrow.innerHTML     = open ? '&#9654;' : '&#9660;';
  });
  document.getElementById('fc-daily').appendChild(_drillDiv);
}

// Return rate (YTD average, applied at week level only)
const INVOICED_RETURN_RATE = 0.06;  // post-delivery returns on invoiced revenue
// Pipeline not-invoiced rate: derived from historical contracted → invoiced ratio.
// contracted × (1 − invoice_ratio) = expected contracted revenue that won't convert to invoiced.
// ~2.7% at current YTD rates (invoice_ratio ≈ 0.973).
const PIPELINE_NONINVOICE_RATE = 1 - (D.revenue_timing?.invoice_ratio ?? 0.973);

function updateDailyInvoiced(){
  if(!window._dailyContracted) return;
  // Fitting inputs are exact dollar amounts (prior-period orders already invoiced)
  let wCLo=0, wCMean=0, wCHi=0, wILo=0, wIMean=0, wIHi=0;
  let wSwCg=0, wSwExt=0;

  const set = (id,v) => { const el=document.getElementById(id); if(el) el.textContent=f.$(v); };

  for(const day of WEEK_DAYS){
    const dc = window._dailyContracted[day];
    if(!dc) continue;
    const fitRev  = Math.max(0, Math.round(parseFloat(document.getElementById('fit-'+day)?.value)||0));
    const iLo     = Math.round(dc.contracted.lo   * dc.sdf) + fitRev;
    const iMean   = Math.round(dc.contracted.mean  * dc.sdf) + fitRev;
    const iHi     = Math.round(dc.contracted.hi   * dc.sdf) + fitRev;
    set('inv-'+day+'-lo',   iLo);
    set('inv-'+day+'-mean', iMean);
    set('inv-'+day+'-hi',   iHi);
    wCLo   += dc.contracted.lo;   wCMean += dc.contracted.mean; wCHi   += dc.contracted.hi;
    wILo   += iLo;                wIMean += iMean;              wIHi   += iHi;
    wSwCg  += dc.sw_ev_cg  || 0;
    wSwExt += dc.sw_ev_ext || 0;
  }

  // Weekly unscheduled fittings — invoiced only (exact $ input)
  const wFitRev = Math.max(0, Math.round(parseFloat(document.getElementById('fit-week')?.value)||0));
  wILo += wFitRev; wIMean += wFitRev; wIHi += wFitRev;

  set('wk-con-lo',  wCLo);  set('wk-con-mean', wCMean); set('wk-con-hi',  wCHi);
  set('wk-inv-lo',  wILo);  set('wk-inv-mean', wIMean); set('wk-inv-hi',  wIHi);

  // Same-week creation totals
  const swCgTot  = Object.values(window._dailyContracted).reduce((a,d)=>a+(d.sw_cg||0),0);
  const swExtTot = Object.values(window._dailyContracted).reduce((a,d)=>a+(d.sw_ext||0),0);
  const swCgEl  = document.getElementById('wk-sw-cg');
  const swExtEl = document.getElementById('wk-sw-ext');
  if(swCgEl)  swCgEl.innerHTML  = `${swCgTot.toFixed(1)}<div style="font-size:8px;color:#a78bfa">${f.$(wSwCg)}</div>`;
  if(swExtEl) swExtEl.innerHTML = `${swExtTot.toFixed(1)}<div style="font-size:8px;color:#a78bfa">${f.$(wSwExt)}</div>`;

  // ── Cancellation / return adjustments (applied at week level only) ────────────
  // Pipeline cancels: contracted × (1−avg_SDF) × 93% — non-SDF orders that won't be invoiced
  // Use revenue-weighted avg SDF across all days
  const totalContracted = wCMean;
  const avgSdf = totalContracted > 0
    ? WEEK_DAYS.reduce((a,d)=>a + (window._dailyContracted[d]?.contracted.mean||0)*(window._dailyContracted[d]?.sdf||0), 0) / totalContracted
    : (D.revenue_timing?.sdf_pct ?? 0.49);

  // Pipeline not-invoiced: contracted × (1 − historical invoice_ratio)
  // Represents contracted that historically doesn't convert to invoiced this period.
  const pipelineCancelRev = Math.round(totalContracted * PIPELINE_NONINVOICE_RATE);

  // Post-delivery returns: 6% of invoiced (Most Likely)
  const invoicedReturns = Math.round(wIMean * INVOICED_RETURN_RATE);

  // Net invoiced (Most Likely, adjusted) = invoiced − post-delivery returns − pipeline non-invoiced
  const netInvoiced = Math.max(0, wIMean - invoicedReturns - pipelineCancelRev);

  // These cells need raw text, not a second pass through f.$ — use textContent directly
  const rawSet = (id, v) => { const el=document.getElementById(id); if(el) el.textContent=v; };
  rawSet('wk-adj-cancel',  pipelineCancelRev > 0 ? '-'+f.$(pipelineCancelRev) : f.$(0));
  rawSet('wk-adj-returns', invoicedReturns    > 0 ? '-'+f.$(invoicedReturns)   : f.$(0));
  rawSet('wk-inv-net', f.$(netInvoiced));
}

// ── Forecast History (localStorage) ──────────────────────────────────────────
const HIST_KEY = 'kpi_fc_history_v1';
function getHistory(){
  try { return JSON.parse(localStorage.getItem(HIST_KEY)||'[]'); } catch(e){ return []; }
}
function setHistory(h){ localStorage.setItem(HIST_KEY, JSON.stringify(h)); }

function saveToHistory(preds, filename){
  const n_cancelled = preds.filter(p=>p.is_cancelled).length;
  const n_active    = preds.length - n_cancelled;
  const testsByDay={}, activeByDay={}, evByDay={}, fByDay={};
  for(const day of WEEK_DAYS){
    const dp = preds.filter(p=>p.dow===day);
    testsByDay[day]  = dp.length;
    activeByDay[day] = dp.filter(p=>!p.is_cancelled).length;
    evByDay[day]     = Math.round(dp.reduce((a,p)=>a+p.ev_test,0));
    const fM = Math.round(dp.reduce((a,p)=>a+p.p_show*p.rev_per_show,0));
    const fS = Math.sqrt(dp.reduce((a,p)=>a+p.p_show*(1-p.p_show)*p.rev_per_show**2,0));
    fByDay[day] = {lo:Math.max(0,Math.round(fM-1.96*fS)), mean:fM, hi:Math.round(fM+1.96*fS)};
  }
  const fM = Math.round(preds.reduce((a,p)=>a+p.p_show*p.rev_per_show,0));
  const fS = Math.sqrt(preds.reduce((a,p)=>a+p.p_show*(1-p.p_show)*p.rev_per_show**2,0));
  const evT = Math.round(preds.reduce((a,p)=>a+p.ev_test,0));
  const cats={};
  for(const p of preds){
    if(!cats[p.cat]) cats[p.cat]={n:0,cancelled:0,ev:0,group:p.group};
    cats[p.cat].n++;
    if(p.is_cancelled) cats[p.cat].cancelled++;
    cats[p.cat].ev = Math.round((cats[p.cat].ev||0)+p.ev_test);
  }
  const clin={};
  for(const p of preds){
    if(!clin[p.clinic]) clin[p.clinic]={n:0,cancelled:0,ev:0,fMean:0};
    clin[p.clinic].n++;
    if(p.is_cancelled) clin[p.clinic].cancelled++;
    clin[p.clinic].ev    = Math.round((clin[p.clinic].ev||0)+p.ev_test);
    clin[p.clinic].fMean = Math.round((clin[p.clinic].fMean||0)+p.p_show*p.rev_per_show);
  }
  const record = {
    id: Date.now(),
    saved_at: new Date().toISOString(),
    filename,
    label: '',
    n_total: preds.length,
    n_active, n_cancelled,
    tests_by_day: testsByDay,
    active_by_day: activeByDay,
    forecast: {
      funnel: {lo:Math.max(0,Math.round(fM-1.96*fS)), mean:fM, hi:Math.round(fM+1.96*fS)},
      ev_test: evT,
      daily_ev: evByDay,
      daily_funnel: fByDay,
    },
    categories: cats,
    by_clinic: clin,
    actual: null,
  };
  const h = getHistory();
  const idx = h.findIndex(r=>r.filename===filename);
  if(idx>=0){
    record.label  = h[idx].label  || '';
    record.actual = h[idx].actual || null;
    h[idx] = record;
  } else {
    h.unshift(record);
  }
  if(h.length>52) h.length=52;
  setHistory(h);
  renderForecastHistory();
}

function saveForecastToHistory(){
  if(!window._fc_preds) return;
  saveToHistory(window._fc_preds, window._fc_filename);
  const btn = event.currentTarget;
  const orig = btn.innerHTML;
  btn.innerHTML = '&#x2713; Saved';
  btn.style.cssText += ';background:#f0fdf4;border-color:#16a34a;color:#16a34a';
  setTimeout(()=>{ btn.innerHTML=orig; btn.style.background=''; btn.style.borderColor=''; btn.style.color=''; },2000);
}

function deleteHistoryRecord(id){
  setHistory(getHistory().filter(r=>r.id!==id));
  renderForecastHistory();
}

function viewHistoryRecord(id){
  const detailRow = document.getElementById('hist-detail-'+id);
  if(!detailRow) return;
  const isOpen = detailRow.style.display !== 'none';
  detailRow.style.display = isOpen ? 'none' : 'table-row';
  const btn = document.getElementById('hv-'+id);
  if(btn) btn.innerHTML = isOpen ? '&#x1F441;' : '&#x25B2;';
}

function buildHistoryDetail(rec){
  const days = ['Mon','Tue','Wed','Thu','Fri'];
  const dayRows = days.map(d=>{
    const n   = rec.tests_by_day?.[d]??0;
    const act = rec.active_by_day?.[d]??0;
    const ev  = rec.forecast?.daily_ev?.[d]??0;
    const df  = rec.forecast?.daily_funnel?.[d];
    return `<tr>
      <td style="font-size:11px;padding:3px 8px">${d}</td>
      <td style="text-align:right;font-size:11px;padding:3px 8px">${act}<span style="color:#dc2626;font-size:9px"> +${n-act}✗</span></td>
      <td style="text-align:right;font-size:11px;padding:3px 8px;font-weight:600">${f.$(ev)}</td>
      <td style="text-align:right;font-size:10px;color:#64748b;padding:3px 8px">${df?f.$(df.lo)+'\u2013'+f.$(df.hi):'\u2014'}</td>
    </tr>`;
  }).join('');

  const clinicRows = Object.entries(rec.by_clinic||{})
    .sort((a,b)=>b[1].fMean-a[1].fMean)
    .map(([name,c])=>`<tr>
      <td style="font-size:11px;padding:3px 8px">${name}</td>
      <td style="text-align:right;font-size:11px;padding:3px 8px">${c.n-c.cancelled}<span style="color:#dc2626;font-size:9px"> +${c.cancelled}✗</span></td>
      <td style="text-align:right;font-size:11px;padding:3px 8px;font-weight:600">${f.$(c.ev)}</td>
      <td style="text-align:right;font-size:11px;padding:3px 8px">${f.$(c.fMean)}</td>
    </tr>`).join('');

  const catRows = Object.entries(rec.categories||{})
    .sort((a,b)=>b[1].ev-a[1].ev)
    .map(([cat,c])=>`<tr>
      <td style="font-size:11px;padding:3px 8px">${cat}</td>
      <td style="font-size:10px;color:#94a3b8;padding:3px 8px">${c.group||''}</td>
      <td style="text-align:right;font-size:11px;padding:3px 8px">${c.n-c.cancelled}<span style="color:#dc2626;font-size:9px"> +${c.cancelled}✗</span></td>
      <td style="text-align:right;font-size:11px;padding:3px 8px;font-weight:600">${f.$(c.ev)}</td>
    </tr>`).join('');

  return `<td colspan="10" style="padding:10px 16px;background:#f8fafc;border-top:1px dashed #e2e8f0">
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px">
      <div>
        <div style="font-size:10px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase;letter-spacing:.05em">Daily Breakdown</div>
        <table style="border-collapse:collapse;width:100%">
          <thead><tr>
            <th data-num="false" style="font-size:10px;text-align:left;padding:2px 8px;color:#94a3b8">Day</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">Active</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">EV/Test</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">Funnel CI</th>
          </tr></thead>
          <tbody>${dayRows}</tbody>
        </table>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase;letter-spacing:.05em">By Clinic</div>
        <table style="border-collapse:collapse;width:100%">
          <thead><tr>
            <th data-num="false" style="font-size:10px;text-align:left;padding:2px 8px;color:#94a3b8">Clinic</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">Active</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">EV/Test</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">Funnel</th>
          </tr></thead>
          <tbody>${clinicRows}</tbody>
        </table>
      </div>
      <div>
        <div style="font-size:10px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase;letter-spacing:.05em">By Category</div>
        <table style="border-collapse:collapse;width:100%">
          <thead><tr>
            <th data-num="false" style="font-size:10px;text-align:left;padding:2px 8px;color:#94a3b8">Category</th>
            <th data-num="false" style="font-size:10px;text-align:left;padding:2px 8px;color:#94a3b8">Group</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">Active</th>
            <th style="font-size:10px;text-align:right;padding:2px 8px;color:#94a3b8">EV/Test</th>
          </tr></thead>
          <tbody>${catRows}</tbody>
        </table>
      </div>
    </div>
  </td>`;
}

function updateHistoryLabel(id){
  const h = getHistory();
  const rec = h.find(r=>r.id===id);
  if(!rec) return;
  const el = document.getElementById('hl-'+id);
  if(el) rec.label = el.value.trim();
  setHistory(h);
}

function updateHistoryActual(id){
  const h = getHistory();
  const rec = h.find(r=>r.id===id);
  if(!rec) return;
  const conEl = document.getElementById('ha-con-'+id);
  const contracted = parseFloat(conEl?.value)||null;
  rec.actual = contracted ? {contracted} : null;
  setHistory(h);
  renderForecastHistory();
}

function exportHistoryCSV(){
  const h = getHistory();
  if(!h.length){ alert('No history to export.'); return; }
  const cols = ['label','saved_at','filename','n_active','n_cancelled',
                'ev_test','funnel_lo','funnel_mean','funnel_hi',
                'actual_contracted','err_ev_pct','err_funnel_pct'];
  const rows = [cols.join(',')];
  for(const r of h){
    const ac  = r.actual?.contracted ?? '';
    const ee  = ac!=='' ? (((ac-r.forecast.ev_test)/ac)*100).toFixed(1) : '';
    const ef  = ac!=='' ? (((ac-r.forecast.funnel.mean)/ac)*100).toFixed(1) : '';
    rows.push(['"'+(r.label||'')+'"', r.saved_at, '"'+r.filename+'"',
               r.n_active, r.n_cancelled,
               r.forecast.ev_test, r.forecast.funnel.lo,
               r.forecast.funnel.mean, r.forecast.funnel.hi,
               ac, ee, ef].join(','));
  }
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([rows.join('\n')],{type:'text/csv'}));
  a.download = 'kpi_forecast_history.csv';
  a.click();
}

function renderForecastHistory(){
  const el = document.getElementById('fc-history');
  if(!el) return;
  const h = getHistory();
  if(!h.length){
    el.innerHTML = `<div class="fc-method" style="margin-top:18px">
      <strong>Forecast History</strong><br>
      No saved forecasts yet. Load a forecast CSV and click
      <em>&#x1F4BE; Save Forecast</em> to begin tracking accuracy week over week.
    </div>`;
    return;
  }

  // ── MAE summary (weeks with actuals entered) ────────────────────────────────
  const withAct = h.filter(r=>r.actual?.contracted!=null);
  let maeBadge = '';
  if(withAct.length>=1){
    const mae_ev  = withAct.reduce((a,r)=>a+Math.abs(r.actual.contracted-r.forecast.ev_test),0)/withAct.length;
    const mae_fun = withAct.reduce((a,r)=>a+Math.abs(r.actual.contracted-r.forecast.funnel.mean),0)/withAct.length;
    const bias_ev = withAct.reduce((a,r)=>a+(r.actual.contracted-r.forecast.ev_test),0)/withAct.length;
    maeBadge = `<span style="font-size:10px;color:#64748b;margin-left:10px;font-weight:400">
      MAE — EV/test: ${f.$(Math.round(mae_ev))} &nbsp;|&nbsp; Funnel: ${f.$(Math.round(mae_fun))}
      &nbsp;|&nbsp; EV bias: ${bias_ev>=0?'+':''}${f.$(Math.round(bias_ev))}
      &nbsp;(${withAct.length} wk${withAct.length>1?'s':''} with actuals)
    </span>`;
  }

  // ── History rows ────────────────────────────────────────────────────────────
  const errFmt = (pct) => {
    if(pct==null) return '<span style="color:#94a3b8">\u2014</span>';
    const col = pct>=0 ? '#15803d' : '#dc2626';
    return `<span style="color:${col};font-weight:600">${pct>=0?'+':''}${(pct*100).toFixed(1)}%</span>`;
  };
  const tableRows = h.map(rec=>{
    const ac    = rec.actual?.contracted ?? null;
    const ee    = ac!=null ? (ac-rec.forecast.ev_test)/ac   : null;
    const ef    = ac!=null ? (ac-rec.forecast.funnel.mean)/ac : null;
    const saved = new Date(rec.saved_at).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'2-digit'});
    return `<tr>
      <td><input id="hl-${rec.id}" type="text" value="${rec.label||''}" placeholder="Week of…"
            style="width:110px;border:1px solid #d1d5db;border-radius:4px;padding:3px 6px;font-size:11px"
            onblur="updateHistoryLabel(${rec.id})"></td>
      <td style="font-size:10px;color:#64748b;white-space:nowrap">${saved}</td>
      <td style="font-size:10px;color:#94a3b8;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"
          title="${rec.filename}">${rec.filename.replace(/\.csv$/i,'')}</td>
      <td style="text-align:right">${rec.n_active}<span style="color:#dc2626;font-size:9px"> +${rec.n_cancelled}✗</span></td>
      <td style="text-align:right;font-weight:600">${f.$(rec.forecast.ev_test)}</td>
      <td style="text-align:right;font-size:10px;color:#64748b">${f.$(rec.forecast.funnel.lo)}&ndash;${f.$(rec.forecast.funnel.hi)}</td>
      <td>
        <div style="display:flex;align-items:center;gap:3px">
          <span style="font-size:11px;color:#64748b">$</span>
          <input id="ha-con-${rec.id}" type="number" min="0" step="1000"
            value="${rec.actual?.contracted||''}" placeholder="Contracted"
            style="width:88px;border:1px solid #d1d5db;border-radius:4px;padding:3px 5px;font-size:11px;text-align:right"
            onblur="updateHistoryActual(${rec.id})">
        </div>
      </td>
      <td style="text-align:right">${errFmt(ee)}</td>
      <td style="text-align:right">${errFmt(ef)}</td>
      <td style="white-space:nowrap">
        <button id="hv-${rec.id}" onclick="viewHistoryRecord(${rec.id})"
              style="border:1px solid #d1d5db;background:#f8fafc;color:#475569;cursor:pointer;font-size:12px;padding:2px 6px;border-radius:4px;margin-right:2px"
              title="View details">&#x1F441;</button>
        <button onclick="deleteHistoryRecord(${rec.id})"
              style="border:none;background:none;color:#94a3b8;cursor:pointer;font-size:15px;padding:2px 4px"
              title="Delete">&#x2715;</button>
      </td>
    </tr>
    <tr id="hist-detail-${rec.id}" style="display:none">${buildHistoryDetail(rec)}</tr>`;
  }).join('');

  // ── Category trend table (last 4 saved, if enough data) ────────────────────
  const recent4 = h.filter(r=>r.categories&&Object.keys(r.categories).length>0).slice(0,4);
  let trendHtml = '';
  if(recent4.length>=2){
    const allCats = [...new Set(recent4.flatMap(r=>Object.keys(r.categories)))].sort();
    const hdrCells = recent4.map(r=>`<th style="font-size:9px;text-align:right;white-space:nowrap">
      ${r.label||new Date(r.saved_at).toLocaleDateString('en-US',{month:'short',day:'numeric'})}
    </th>`).join('');
    const tRows = allCats.map(cat=>{
      const vals = recent4.map(r=>r.categories[cat]?.n||0);
      const delta = vals[0]-vals[1];
      const arrow = delta>2?'<span style="color:#15803d">\u25b2</span>':delta<-2?'<span style="color:#dc2626">\u25bc</span>':'<span style="color:#94a3b8">\u2014</span>';
      const grp   = recent4[0].categories[cat]?.group||'';
      return `<tr>
        <td style="font-size:10px">${cat}</td>
        <td style="font-size:9px;color:#94a3b8">${grp}</td>
        ${vals.map(v=>`<td style="text-align:right;font-size:10px">${v}</td>`).join('')}
        <td style="text-align:center;width:24px">${arrow}</td>
      </tr>`;
    }).join('');
    trendHtml = `
      <div class="section-title" style="font-size:12px;font-weight:600;margin:16px 0 6px">
        Test Creation Trends &mdash; by category (last ${recent4.length} saved forecasts)
      </div>
      <div class="twrap" style="overflow-x:auto">
        <table><thead><tr>
          <th data-num="false">Category</th>
          <th data-num="false" style="font-size:9px;color:#94a3b8">Group</th>
          ${hdrCells}
          <th>Trend</th>
        </tr></thead><tbody>${tRows}</tbody></table>
      </div>`;
  }

  el.innerHTML = `
    <div style="display:flex;align-items:center;gap:8px;margin:24px 0 8px;flex-wrap:wrap">
      <div class="section-title" style="font-size:13px;font-weight:600;margin:0">
        Forecast History ${maeBadge}
      </div>
      <button onclick="exportHistoryCSV()"
        style="margin-left:auto;font-size:10px;padding:4px 10px;border:1px solid #d1d5db;border-radius:6px;background:#f8fafc;cursor:pointer">
        &#x2B07; Export CSV
      </button>
    </div>
    <p class="sec-note" style="margin-bottom:8px">
      Enter a week label and actual contracted revenue to track accuracy.
      Positive error = actual exceeded forecast; negative = forecast overshot actual.
    </p>
    <div class="twrap" style="overflow-x:auto;margin-bottom:4px">
      <table><thead><tr>
        <th data-num="false" style="min-width:120px">Week Label</th>
        <th data-num="false">Saved</th>
        <th data-num="false">File</th>
        <th>Active/Cancel</th>
        <th>EV/Test Forecast</th>
        <th data-num="false">Funnel 95% CI</th>
        <th data-num="false">Actual Contracted ($)</th>
        <th>EV Error</th>
        <th>Funnel Error</th>
        <th></th>
      </tr></thead><tbody>${tableRows}</tbody></table>
    </div>
    ${trendHtml}`;
}

// ── Render forecast results ───────────────────────────────────────────────────
function showForecastResults(preds, filename){
  if(!preds.length){
    document.getElementById('fc-results').innerHTML =
      '<div class="fc-warn">No active/scheduled appointments found. Check that appointment status column is present and not all rows are Completed/Cancelled.</div>';
    return;
  }

  const n          = preds.length;
  const n_cancelled = preds.filter(p=>p.is_cancelled).length;
  const n_active   = n - n_cancelled;
  const n_conf     = preds.filter(p=>p.confirmed).length;
  // Cancelled have p_show=0, so they contribute 0 to mean and variance — CI is unchanged by inclusion
  const showCI     = pbCI(preds.map(p=>p.p_show));
  const saleCI     = pbCI(preds.map(p=>p.p_sale));

  // ── Revenue CI — Funnel method ─────────────────────────────────────────────
  // Each appointment is a Bernoulli trial (shows / doesn't show).
  // If shows: revenue = test_rate × p_close × ha_per_sale × ASP (= rev_per_show).
  // E[revenue_i] = p_show_i × rev_per_show_i
  // Var[revenue_i] = p_show_i × (1 − p_show_i) × rev_per_show_i²
  // Aggregate via CLT: mean ± 1.96 × √(Σ Var_i)
  const fRevMean = Math.round(preds.reduce((a,p)=>a + p.p_show * p.rev_per_show, 0));
  const fRevStd  = Math.sqrt(preds.reduce((a,p)=>a + p.p_show*(1-p.p_show)*p.rev_per_show*p.rev_per_show, 0));
  const fRevLo   = Math.max(0, Math.round(fRevMean - 1.96*fRevStd));
  const fRevHi   = Math.round(fRevMean + 1.96*fRevStd);

  // ── Revenue — EV/test method ───────────────────────────────────────────────
  // Sum of historically-observed contracted_revenue ÷ tests_scheduled per category.
  // Deterministic point estimate (no further distributional uncertainty modelled).
  const evTotal = Math.round(preds.reduce((a,p)=>a + p.ev_test, 0));

  // Group by clinic
  const byClinic = {};
  for(const p of preds){
    if(!byClinic[p.clinic]) byClinic[p.clinic]={n:0,conf:0,ps:[],psa:[],frevItems:[],evSum:0};
    byClinic[p.clinic].n++;
    if(p.confirmed) byClinic[p.clinic].conf++;
    byClinic[p.clinic].ps.push(p.p_show);
    byClinic[p.clinic].psa.push(p.p_sale);
    byClinic[p.clinic].frevItems.push({ps:p.p_show, rpx:p.rev_per_show});
    byClinic[p.clinic].evSum += p.ev_test;
  }
  const clinicRows = Object.entries(byClinic)
    .map(([name,d])=>{
      const sc  = pbCI(d.ps), sa = pbCI(d.psa);
      const frm = Math.round(d.frevItems.reduce((a,x)=>a+x.ps*x.rpx, 0));
      const frs = Math.sqrt(d.frevItems.reduce((a,x)=>a+x.ps*(1-x.ps)*x.rpx*x.rpx, 0));
      const frl = Math.max(0, Math.round(frm - 1.96*frs));
      const frh = Math.round(frm + 1.96*frs);
      const ev  = Math.round(d.evSum);
      return {name, n:d.n, conf:d.conf, sc, sa, frm, frl, frh, ev};
    })
    .sort((a,b)=>b.sc.mean-a.sc.mean);

  // Group by lead source group
  const byGroup = {clinic:{n:0,ps:[],psa:[],frevItems:[],evSum:0},
                   external:{n:0,ps:[],psa:[],frevItems:[],evSum:0},
                   unknown:{n:0,ps:[],psa:[],frevItems:[],evSum:0}};
  for(const p of preds){
    const g=byGroup[p.group]||byGroup.unknown;
    g.n++; g.ps.push(p.p_show); g.psa.push(p.p_sale);
    g.frevItems.push({ps:p.p_show,rpx:p.rev_per_show}); g.evSum+=p.ev_test;
  }

  // Avg show rates for methodology note
  const avg_show_conf    = preds.filter(p=>p.confirmed).reduce((a,p)=>a+p.p_show,0)/(n_conf||1);
  // Exclude cancelled from non-confirmed avg (their p_show=0 would distort the average)
  const avg_show_noconf  = preds.filter(p=>!p.confirmed&&!p.is_cancelled).reduce((a,p)=>a+p.p_show,0)/((n_active-n_conf)||1);

  // Store for Save Forecast button access
  window._fc_preds    = preds;
  window._fc_filename = filename;

  document.getElementById('fc-results').innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
      <div style="font-size:12px;color:#64748b">
        <strong style="color:#1a1a2e">${filename}</strong>
        &nbsp;&middot;&nbsp;${f.n(n_active)} active · <span style="color:#dc2626">${f.n(n_cancelled)} cancelled</span>
        &nbsp;&middot;&nbsp;${f.n(n_conf)} confirmed (${f.pct(n_conf/(n_active||1))})
        <button class="btn-reset" onclick="resetForecast()">&#x2715; Clear</button>
        <button onclick="saveForecastToHistory()"
          style="margin-left:6px;font-size:11px;padding:3px 10px;border:1px solid #0ea5e9;border-radius:6px;background:#eff6ff;color:#0284c7;cursor:pointer">
          &#x1F4BE; Save Forecast
        </button>
      </div>
    </div>

    <div class="cards" style="padding:0;margin-bottom:18px">
      <div class="card"><div class="lbl">Scheduled</div><div class="val">${f.n(n)}</div>
        <div class="sub">${f.n(n_active)} active · ${f.n(n_cancelled)} cancelled</div></div>
      <div class="card hi"><div class="lbl">Expected Shows</div>
        <div class="val">${showCI.mean.toFixed(1)}</div>
        <div class="sub">${showCI.lo.toFixed(1)}\u2013${showCI.hi.toFixed(1)} &nbsp;95% CI</div></div>
      <div class="card"><div class="lbl">Expected Sales</div>
        <div class="val">${saleCI.mean.toFixed(1)}</div>
        <div class="sub">${saleCI.lo.toFixed(1)}\u2013${saleCI.hi.toFixed(1)} &nbsp;95% CI</div></div>
      <div class="card hi"><div class="lbl">Rev — Funnel Model</div>
        <div class="val">${f.$(fRevMean)}</div>
        <div class="sub">${f.$(fRevLo)}\u2013${f.$(fRevHi)} &nbsp;95% CI</div></div>
      <div class="card"><div class="lbl">Rev — EV/Test</div>
        <div class="val">${f.$(evTotal)}</div>
        <div class="sub">Contracted $/test &times; tests scheduled</div></div>
    </div>

    <div class="chart-row">
      <div style="flex:2">
        <div class="twrap"><table id="fc-clinic-tbl">
          <thead><tr>
            <th data-num="false">Clinic</th>
            <th>Scheduled</th><th>Confirmed</th>
            <th>Exp Shows</th><th data-num="false">Shows 95% CI</th>
            <th>Exp Sales</th><th data-num="false">Sales 95% CI</th>
            <th>Rev (Funnel)</th><th data-num="false">Funnel 95% CI</th>
            <th>Rev (EV/Test)</th>
          </tr></thead>
          <tbody>${clinicRows.map(r=>`<tr>
            <td>${r.name}</td>
            <td data-v="${r.n}">${f.n(r.n)}</td>
            <td data-v="${r.conf}">${f.n(r.conf)} <span style="color:#94a3b8;font-size:10px">(${f.pct(r.conf/r.n)})</span></td>
            <td data-v="${r.sc.mean}" style="font-weight:600">${r.sc.mean.toFixed(1)}</td>
            <td style="color:#94a3b8;font-size:10px">${r.sc.lo.toFixed(1)}\u2013${r.sc.hi.toFixed(1)}</td>
            <td data-v="${r.sa.mean}" style="font-weight:600">${r.sa.mean.toFixed(1)}</td>
            <td style="color:#94a3b8;font-size:10px">${r.sa.lo.toFixed(1)}\u2013${r.sa.hi.toFixed(1)}</td>
            <td data-v="${r.frm}" style="font-weight:600">${f.$(r.frm)}</td>
            <td style="color:#94a3b8;font-size:10px">${f.$(r.frl)}\u2013${f.$(r.frh)}</td>
            <td data-v="${r.ev}">${f.$(r.ev)}</td>
          </tr>`).join('')}</tbody>
        </table></div>
      </div>

      <div style="flex:1">
        <div class="twrap"><table>
          <thead><tr>
            <th data-num="false">Lead Source Group</th>
            <th>Scheduled</th><th>Exp Shows</th><th>Exp Sales</th>
            <th>Rev (Funnel)</th><th>Rev (EV/Test)</th>
          </tr></thead>
          <tbody>${Object.entries(byGroup).filter(([,d])=>d.n>0).map(([grp,d])=>{
            const sc=pbCI(d.ps), sa=pbCI(d.psa);
            const gfm=Math.round(d.frevItems.reduce((a,x)=>a+x.ps*x.rpx,0));
            const gfs=Math.sqrt(d.frevItems.reduce((a,x)=>a+x.ps*(1-x.ps)*x.rpx*x.rpx,0));
            const gfl=Math.max(0,Math.round(gfm-1.96*gfs)), gfh=Math.round(gfm+1.96*gfs);
            const gev=Math.round(d.evSum);
            return `<tr>
              <td><span class="pill ${grp}">${gLbl(grp)}</span></td>
              <td>${f.n(d.n)}</td>
              <td>${sc.mean.toFixed(1)} <span style="color:#94a3b8;font-size:10px">[${sc.lo.toFixed(1)}\u2013${sc.hi.toFixed(1)}]</span></td>
              <td>${sa.mean.toFixed(1)} <span style="color:#94a3b8;font-size:10px">[${sa.lo.toFixed(1)}\u2013${sa.hi.toFixed(1)}]</span></td>
              <td>${f.$(gfm)} <span style="color:#94a3b8;font-size:10px">[${f.$(gfl)}\u2013${f.$(gfh)}]</span></td>
              <td>${f.$(gev)}</td>
            </tr>`;}).join('')}</tbody>
        </table></div>
      </div>
    </div>

    <div class="fc-method">
      <strong>Prediction methodology</strong>
      Show rate: clinic-specific historical rates for confirmed (avg ${f.pct(avg_show_conf)})
      and unconfirmed (avg ${f.pct(avg_show_noconf)}) appointments, from JAN\u2013MAR 2026 actuals.
      Close rate: by lead source category where campaign maps to a known category, otherwise by creator role
      (clinic-gen / external). Aidable loss rate: ${f.pct(D.rates.aidable_rate)} of shows.
      All show/sale intervals are 95% Poisson-binomial CI (correct for sums of Bernoulli trials with different p\u2019s).
      <br><br>
      <strong>Revenue \u2014 Funnel model:</strong>
      E[revenue\u1d62] = p_show\u1d62 \u00d7 ${f.pct(D.rates.test_rate)} tested \u00d7 ${f.pct(D.rates.aidable_of_tested)} aidable \u00d7 p_close\u1d62 \u00d7 ${D.rates.ha_per_sale.toFixed(3)} HA/sale \u00d7 ASP (${f.$(D.rates.asp)}/HA).
      Effective aidable rate = ${f.pct(D.rates.aidable_rate)}. ha/sale calibrated to full-period contracted revenue.
      Revenue CI propagates show-rate uncertainty: Var[revenue\u1d62] = p_show\u1d62(1\u2212p_show\u1d62) \u00d7 rev_if_shows\u1d62\u00b2,
      aggregated via CLT. Close-rate variability is not separately modelled in the CI \u2014
      the true interval is somewhat wider for weeks with unusual category mix.
      Caution: this model overpredicted JAN\u2013MAR actuals by ~19% vs contracted revenue
      (\$${f.$(Math.round(fRevMean))} forecast vs actual \u2014 validate against EV/test).
      <br><br>
      <strong>Revenue \u2014 EV/test model:</strong>
      Contracted revenue \u00f7 tests scheduled from JAN\u2013MAR Reporting Dimension, by category.
      Falls back to group average (clinic-gen ${f.$(D.rates.ev_per_group.clinic)}/test,
      external ${f.$(D.rates.ev_per_group.external)}/test,
      overall ${f.$(D.rates.ev_per_group.overall)}/test) when category is unmapped.
      Single point estimate \u2014 no CI modelled (the uncertainty is in the realized show and close rates,
      already captured in the funnel model). Historically more accurate than funnel model.
      <br><br>
      <strong>CI reliability note:</strong>
      Poisson-binomial normal approximation is well-justified for n \u2265 100 (skewness \u2248 0).
      For individual clinics with fewer than ~50 scheduled appointments, treat the CI as
      approximate \u2014 the true interval is slightly wider.
      All CIs assume appointment independence; intra-clinic correlation (shared provider effects)
      would widen intervals further if accounted for.
      <br><br>
      <strong>Note: this predicts outcomes for currently-scheduled appointments only.</strong>
      It does not yet account for appointments that will be booked during the week
      (walk-ins, same-day referrals, short lead-time external). That layer is coming next.
    </div>
    <div id="fc-daily"></div>`;

  makeSortable('fc-clinic-tbl');
  renderDailyRevSection(preds);
}

function resetForecast(){
  document.getElementById('fc-file-info').textContent='';
  document.getElementById('fc-results').innerHTML='';
  document.getElementById('fc-input').value='';
}

// ── Init ──────────────────────────────────────────────────────────────────────
renderLead();
renderClinics();
renderPatients();
renderOrigin();
renderForecast();
renderForecastHistory();
renderAnalysis();
drawAll('lead');
window.addEventListener('resize', ()=>drawAll(
  document.querySelector('.tab.active')?.dataset?.tab||'lead'
));
</script>
</body>
</html>
"""

out = BASE / 'dashboard.html'
out.write_text(HTML.replace('__DATA__', json.dumps(DATA, ensure_ascii=False)))
print(f"Dashboard generated \u2192 {out}")
print(f"Open with:  open '{out}'")

# ── GitHub Pages auto-push ────────────────────────────────────────────────────
def git_push_dashboard():
    try:
        git_dir = BASE
        # Verify this is a git repo
        check = subprocess.run(
            ['git', 'rev-parse', '--git-dir'],
            cwd=git_dir, capture_output=True
        )
        if check.returncode != 0:
            print("\n⚠️  GitHub Pages: not a git repo yet — run setup steps first (see README).")
            return

        subprocess.run(['git', 'add', 'dashboard.html'], cwd=git_dir, check=True)

        # Only commit if there are staged changes
        diff = subprocess.run(
            ['git', 'diff', '--cached', '--quiet'],
            cwd=git_dir
        )
        if diff.returncode == 0:
            print("GitHub Pages: dashboard unchanged, nothing to push.")
            return

        msg = f"dashboard update {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(['git', 'commit', '-m', msg], cwd=git_dir, check=True)
        subprocess.run(['git', 'push', 'origin', 'main'], cwd=git_dir, check=True)
        print("✓  Dashboard pushed → GitHub Pages will update in ~30 seconds.")

    except subprocess.CalledProcessError as e:
        print(f"⚠️  Git push failed: {e}")
    except Exception as e:
        print(f"⚠️  Git error: {e}")

git_push_dashboard()
