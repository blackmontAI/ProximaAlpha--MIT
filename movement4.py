import os
import math
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio
from sklearn.decomposition import PCA
from send_message_backend import send_message_backend

warnings.filterwarnings('default')

base_path = '/mnt/z/B011'
if not os.path.isdir(base_path):
 os.makedirs(base_path, exist_ok=True)

epsilon_norm = 1e-6

progress = 0.0
total_progress = 100.0
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

phase_setup_weight = 5.0
phase_read_weight = 10.0
phase_properties_weight = 15.0
phase_schedule_weight = 5.0
phase_sim_weight = 50.0
phase_postproc_weight = 15.0

movement1_path = os.path.join(base_path, 'movement1_tables.xlsx')
movement2_path = os.path.join(base_path, 'movement2_tables.xlsx')
assert os.path.isfile(movement1_path), 'movement1_tables.xlsx not found in /mnt/z/B011'
assert os.path.isfile(movement2_path), 'movement2_tables.xlsx not found in /mnt/z/B011'

progress = phase_setup_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

def read_validated_sheet(path, sheet_name, expected_cols):
 xls = pd.ExcelFile(path)
 assert sheet_name in xls.sheet_names, 'Sheet ' sheet_name ' not found in ' path
 raw = pd.read_excel(path, sheet_name=sheet_name, header=None)
 assert raw.shape[0] >= 1, 'Raw sheet ' sheet_name ' is empty in ' path
 header = raw.iloc[0].astype(str).tolist()
 for col in expected_cols:
 assert col in header, 'Expected column ' col ' not found in header of sheet ' sheet_name
 df = pd.read_excel(path, sheet_name=sheet_name, header=0)
 for col in expected_cols:
 assert col in df.columns, 'Column ' col ' missing in sheet ' sheet_name
 assert not df[expected_cols].isna().all().any(), 'All-NaN column in key variables for sheet ' sheet_name
 return df

layers_expected_cols = ['Species', 'Layer', 'L_mm', 'rho', 'm_l_kg_per_m2']
drying_expected_cols = ['Species', 'Layer', 'w0', 'w_eq', 'w_tdry', 'Loss_pct']
pools_expected_cols = ['Species', 'Layer', 'P_l', 'C_l', 'P_nc_l', 'AA_l', 'AA_S_l', 'F_l', 'F_unsat_l', 'W_l', 'S_glaze_l']
kinetics_expected_cols = ['Canal', 'Proceso', 'E_k_kJmol', 'A_k_rel', 'beta_k', 'gamma_k', 'T_range_C', 'Fuente']

layers_df = read_validated_sheet(movement1_path, 'Layers', layers_expected_cols)
try:
 drying_df = read_validated_sheet(movement1_path, 'Drying', drying_expected_cols)
except Exception:
 try:
 drying_df = read_validated_sheet(movement1_path, 'Drying_envelope', drying_expected_cols)
 except Exception:
 drying_df = read_validated_sheet(movement1_path, 'DryingSummary', drying_expected_cols)
pools_df = read_validated_sheet(movement2_path, 'Pools', pools_expected_cols)
kinetics_df = read_validated_sheet(movement2_path, 'Kinetics', kinetics_expected_cols)

progress = phase_setup_weight phase_read_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

pig_layers = layers_df[layers_df['Species'].astype(str) == 'Pig'].copy()
pig_pools = pools_df[pools_df['Species'].astype(str) == 'Pig'].copy()
pig_drying = drying_df[drying_df['Species'].astype(str) == 'Pig'].copy()
assert pig_layers.shape[0] >= 3, 'Not enough Pig layer rows in Layers sheet'
assert pig_pools.shape[0] >= 3, 'Not enough Pig layer rows in Pools sheet'
assert pig_drying.shape[0] >= 2, 'Not enough Pig layer rows in Drying sheet'
assert not pig_layers.isna().any().any(), 'NaN values in pig_layers'
assert not pig_pools.isna().any().any(), 'NaN values in pig_pools'
assert not pig_drying.isna().any().any(), 'NaN values in pig_drying'

def get_layer_row(df, layer_name):
 mask = df['Layer'].astype(str).str.lower() == layer_name
 sub = df[mask]
 assert sub.shape[0] >= 1, 'Layer ' layer_name ' not found'
 return sub.iloc[0]

skin_layer_row = get_layer_row(pig_layers, 'skin')
fat_layer_row = get_layer_row(pig_layers, 'fat')
muscle_layer_row = get_layer_row(pig_layers, 'muscle')

L_skin_mm = float(skin_layer_row['L_mm'])
L_fat_mm = float(fat_layer_row['L_mm'])
L_muscle_mm = float(muscle_layer_row['L_mm'])
rho_skin_layer = float(skin_layer_row['rho'])
rho_fat_layer = float(fat_layer_row['rho'])
rho_muscle_layer = float(muscle_layer_row['rho'])

L_skin = L_skin_mm * 1e-3
L_fat = L_fat_mm * 1e-3
L_muscle = L_muscle_mm * 1e-3
assert L_skin > 0.0 and L_fat > 0.0 and L_muscle > 0.0, 'Layer thicknesses must be positive'

skin_pool_row = get_layer_row(pig_pools, 'skin')
fat_pool_row = get_layer_row(pig_pools, 'fat')
muscle_pool_row = get_layer_row(pig_pools, 'muscle')

P_l_skin = float(skin_pool_row['P_l'])
C_l_skin = float(skin_pool_row['C_l'])
P_nc_l_skin = float(skin_pool_row['P_nc_l'])
W_l_skin = float(skin_pool_row['W_l'])
F_l_skin = float(skin_pool_row['F_l'])
F_unsat_skin = float(skin_pool_row['F_unsat_l'])

P_l_fat = float(fat_pool_row['P_l'])
C_l_fat = float(fat_pool_row['C_l'])
P_nc_l_fat = float(fat_pool_row['P_nc_l'])
W_l_fat = float(fat_pool_row['W_l'])
F_l_fat = float(fat_pool_row['F_l'])
F_unsat_fat = float(fat_pool_row['F_unsat_l'])

P_l_muscle = float(muscle_pool_row['P_l'])
C_l_muscle = float(muscle_pool_row['C_l'])
P_nc_l_muscle = float(muscle_pool_row['P_nc_l'])
W_l_muscle = float(muscle_pool_row['W_l'])
F_l_muscle = float(muscle_pool_row['F_l'])
F_unsat_muscle = float(muscle_pool_row['F_unsat_l'])

rho_layers = np.array([rho_skin_layer, rho_fat_layer, rho_muscle_layer], dtype=float)
L_layers = np.array([L_skin, L_fat, L_muscle], dtype=float)
m_layers_geom = rho_layers * L_layers

m_skin = float(m_layers_geom[0])
m_fat = float(m_layers_geom[1])
m_muscle = float(m_layers_geom[2])
assert m_skin > 0.0 and m_fat > 0.0 and m_muscle > 0.0, 'Layer masses must be positive'

w_skin = W_l_skin / m_skin
p_skin = P_l_skin / m_skin
f_skin = F_l_skin / m_skin
w_fat = W_l_fat / m_fat
p_fat = P_l_fat / m_fat
f_fat = F_l_fat / m_fat
w_muscle = W_l_muscle / m_muscle
p_muscle = P_l_muscle / m_muscle
f_muscle = F_l_muscle / m_muscle

wpf_tolerance = 1e-3
wpf_skin_sum = w_skin p_skin f_skin
wpf_fat_sum = w_fat p_fat f_fat
wpf_muscle_sum = w_muscle p_muscle f_muscle
if abs(wpf_skin_sum - 1.0) > wpf_tolerance:
 w_skin = w_skin / wpf_skin_sum
 p_skin = p_skin / wpf_skin_sum
 f_skin = f_skin / wpf_skin_sum
if abs(wpf_fat_sum - 1.0) > wpf_tolerance:
 w_fat = w_fat / wpf_fat_sum
 p_fat = p_fat / wpf_fat_sum
 f_fat = f_fat / wpf_fat_sum
if abs(wpf_muscle_sum - 1.0) > wpf_tolerance:
 w_muscle = w_muscle / wpf_muscle_sum
 p_muscle = p_muscle / wpf_muscle_sum
 f_muscle = f_muscle / wpf_muscle_sum

rho_water = 1000.0
cp_water = 4180.0
k_water = 0.6
cp_fat = 2000.0
k_fat = 0.2
rho_protein = 1320.0
cp_protein = 1700.0
k_protein = 0.3

cp_skin = w_skin * cp_water f_skin * cp_fat p_skin * cp_protein
cp_fat_eff = w_fat * cp_water f_fat * cp_fat p_fat * cp_protein
cp_muscle = w_muscle * cp_water f_muscle * cp_fat p_muscle * cp_protein

k_skin_eff = w_skin * k_water f_skin * k_fat p_skin * k_protein
k_fat_eff = w_fat * k_water f_fat * k_fat p_fat * k_protein
k_muscle_eff = w_muscle * k_water f_muscle * k_fat p_muscle * k_protein

D_w_skin = 5e-11
D_w_fat = 5e-12
D_w_muscle = 1e-10

cp_layers = np.array([cp_skin, cp_fat_eff, cp_muscle], dtype=float)
k_layers = np.array([k_skin_eff, k_fat_eff, k_muscle_eff], dtype=float)
D_w_layers = np.array([D_w_skin, D_w_fat, D_w_muscle], dtype=float)

dry_skin_row = get_layer_row(pig_drying, 'skin')
dry_fat_row = get_layer_row(pig_drying, 'fat')
assert 'w_tdry' in dry_skin_row.index and 'w_tdry' in dry_fat_row.index, 'w_tdry column missing in Drying sheet'
w_tdry_skin = float(dry_skin_row['w_tdry'])
w_tdry_fat = float(dry_fat_row['w_tdry'])
assert w_tdry_skin > 0.0 and w_tdry_fat > 0.0, 'w_tdry must be positive for skin and fat'

w0_skin_layer = w_tdry_skin
w0_fat_layer = w_tdry_fat
w0_muscle_layer = w_muscle

n_skin = 0.85
n_fat = 0.82
n_muscle = 0.90
n_layers = np.array([n_skin, n_fat, n_muscle], dtype=float)

P_nc_layers = np.array([P_nc_l_skin, P_nc_l_fat, P_nc_l_muscle], dtype=float)
P_nc_per_volume_layers = P_nc_layers / L_layers
assert np.all(P_nc_per_volume_layers >= 0.0), 'P_nc per volume must be non-negative'

kinetics_M_row = kinetics_df[kinetics_df['Canal'].astype(str) == 'M']
assert kinetics_M_row.shape[0] >= 1, 'Canal M not found in Kinetics sheet'
kinetics_M_row = kinetics_M_row.iloc[0]
E_M_kJmol = float(kinetics_M_row['E_k_kJmol'])
beta_M = float(kinetics_M_row['beta_k'])
gamma_M = float(kinetics_M_row['gamma_k'])
E_M_Jmol = E_M_kJmol * 1000.0
R_gas = 8.314

L_total = float(L_skin L_fat L_muscle)
N_nodes = 101
x_nodes = np.linspace(0.0, L_total, N_nodes)
dx = x_nodes[1] - x_nodes[0]

layer_index_nodes = np.zeros(N_nodes, dtype=int)
for i in range(N_nodes):
 xi = x_nodes[i]
 if xi < L_skin:
 layer_index_nodes[i] = 0
 elif xi < L_skin L_fat:
 layer_index_nodes[i] = 1
 else:
 layer_index_nodes[i] = 2

rho_nodes = np.zeros(N_nodes, dtype=float)
cp_nodes = np.zeros(N_nodes, dtype=float)
k_nodes_base = np.zeros(N_nodes, dtype=float)
D_w_nodes_base = np.zeros(N_nodes, dtype=float)
w0_nodes = np.zeros(N_nodes, dtype=float)
n_iso_nodes = np.zeros(N_nodes, dtype=float)
P_nc_nodes = np.zeros(N_nodes, dtype=float)

for i in range(N_nodes):
 li = layer_index_nodes[i]
 rho_nodes[i] = rho_layers[li]
 cp_nodes[i] = cp_layers[li]
 k_nodes_base[i] = k_layers[li]
 D_w_nodes_base[i] = D_w_layers[li]
 if li == 0:
 w0_nodes[i] = w0_skin_layer
 elif li == 1:
 w0_nodes[i] = w0_fat_layer
 else:
 w0_nodes[i] = w0_muscle_layer
 n_iso_nodes[i] = n_layers[li]
 P_nc_nodes[i] = P_nc_per_volume_layers[li]

assert np.all(rho_nodes > 0.0), 'All rho_nodes must be positive'
assert np.all(cp_nodes > 0.0), 'All cp_nodes must be positive'
assert np.all(k_nodes_base > 0.0), 'All k_nodes_base must be positive'
assert np.all(D_w_nodes_base > 0.0), 'All D_w_nodes_base must be positive'
assert np.all(w0_nodes > 0.0), 'All w0_nodes must be positive'
assert np.all(P_nc_nodes >= 0.0), 'All P_nc_nodes must be non-negative'

alpha_nodes_base = k_nodes_base / (rho_nodes * cp_nodes)

skin_nodes = np.where(layer_index_nodes == 0)[0]
fat_nodes = np.where(layer_index_nodes == 1)[0]
muscle_nodes = np.where(layer_index_nodes == 2)[0]
assert skin_nodes.size > 0 and fat_nodes.size > 0 and muscle_nodes.size > 0, 'Empty layer node sets detected'

T_C_grid = np.linspace(80.0, 260.0, 300)
T_K_grid = T_C_grid 273.15
r_M_grid = np.exp(-E_M_Jmol / (R_gas * T_K_grid))
max_r_M = float(np.max(r_M_grid))
assert max_r_M > 0.0, 'Maximum Maillard rate is non-positive'
r_tilde_M_grid = r_M_grid / max_r_M

def interp_rtilde(T_array_C):
 return np.interp(T_array_C, T_C_grid, r_tilde_M_grid)

def g_M_func(aw_array):
 aw_clip = np.clip(aw_array, 0.0, 1.0)
 return np.power(aw_clip, beta_M) * np.power(1.0 - aw_clip, gamma_M)

F_tot = F_l_skin F_l_fat F_l_muscle
F_unsat_tot = F_unsat_skin F_unsat_fat F_unsat_muscle
assert F_tot > 0.0, 'Total fat must be positive'
f_unsat = F_unsat_tot / F_tot
f_SFA = 1.0 - f_unsat
if f_SFA < 0.0:
 f_SFA = 0.0
if f_SFA > 1.0:
 f_SFA = 1.0

T_low_SFA = 40.0
T_high_SFA = 80.0
T_low_unsat = 20.0
T_high_unsat = 70.0
T_low = T_low_SFA * f_SFA T_low_unsat * (1.0 - f_SFA)
T_high = T_high_SFA * f_SFA T_high_unsat * (1.0 - f_SFA)
if T_high <= T_low:
 T_high = T_low 1.0
T50 = 0.5 * (T_low T_high)
den_T = T50 - T_low
if abs(den_T) < 1e-6:
 den_T = 1e-6
a_param = math.log(19.0) / den_T

def f_melt_from_T(T_array_C):
 z = (T_array_C - T50) * a_param
 z = np.clip(z, -50.0, 50.0)
 f_val = 1.0 / (1.0 np.exp(-z))
 f_val[f_val < 0.0] = 0.0
 f_val[f_val > 1.0] = 1.0
 return f_val

lambda_J = 1.0
f_melt_star = 0.7
T_safe = 70.0
T_init = 4.0

progress = phase_setup_weight phase_read_weight phase_properties_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

def run_schedule(par):
 T3 = float(par['T3'])
 T4 = float(par['T4'])
 T5 = float(par['T5'])
 T6 = float(par['T6'])
 T7 = float(par['T7'])
 T8 = float(par['T8'])
 t3 = float(par['t3'])
 t4 = float(par['t4'])
 t5 = float(par['t5'])
 t6 = float(par['t6'])
 t7 = float(par['t7'])
 t8 = float(par['t8'])
 RH3 = float(par['RH3'])
 RH4 = float(par['RH4'])
 RH5 = float(par['RH5'])
 phases_T = np.array([T3, T4, T5, T6, T7, T8], dtype=float)
 phases_t_min = np.array([t3, t4, t5, t6, t7, t8], dtype=float)
 phases_RH = np.array([RH3, RH4, RH5, 0.0, 0.0, 0.0], dtype=float)
 h_phase_base = np.array([20.0, 25.0, 30.0, 35.0, 40.0, 45.0], dtype=float)
 dur_s = phases_t_min * 60.0
 t_tot = float(np.sum(dur_s))
 assert t_tot > 0.0, 'Total time must be positive'
 dt_try = 1.0
 Fo_max = float(np.max(alpha_nodes_base * dt_try / (dx * dx)))
 while Fo_max > 0.2 and dt_try > 1e-4:
 dt_try *= 0.5
 Fo_max = float(np.max(alpha_nodes_base * dt_try / (dx * dx)))
 dt = dt_try
 if dt <= 0.0:
 dt = 1e-3
 n_steps = int(math.ceil(t_tot / dt)) 1
 times = np.linspace(0.0, t_tot, n_steps)
 T_air_t = np.zeros(n_steps, dtype=float)
 RH_t = np.zeros(n_steps, dtype=float)
 h_t = np.zeros(n_steps, dtype=float)
 cum = 0.0
 for p in range(6):
 t_start = cum
 t_end = cum float(dur_s[p])
 if p < 5:
 mask = (times >= t_start) & (times < t_end)
 else:
 mask = (times >= t_start) & (times <= t_end 1e-9)
 T_air_t[mask] = phases_T[p]
 RH_t[mask] = phases_RH[p]
 h_t[mask] = h_phase_base[p]
 cum = t_end
 T_curr = np.full(N_nodes, T_init, dtype=float)
 w_curr = w0_nodes.copy().astype(float)
 M_curr = np.zeros(N_nodes, dtype=float)
 T_core_hist = np.zeros(n_steps, dtype=float)
 k_nodes = k_nodes_base.copy()
 rho_nodes_loc = rho_nodes.copy()
 cp_nodes_loc = cp_nodes.copy()
 alpha_nodes = k_nodes / (rho_nodes_loc * cp_nodes_loc)
 k_face = 0.5 * (k_nodes[:-1] k_nodes[1:])
 dx2 = dx * dx
 for n in range(n_steps - 1):
 Tn = T_curr.copy()
 k_ip = k_face[1:]
 k_im = k_face[:-1]
 T_curr[1:-1] = Tn[1:-1] (dt / (rho_nodes_loc[1:-1] * cp_nodes_loc[1:-1] * dx2)) * (k_ip * (Tn[2:] - Tn[1:-1]) - k_im * (Tn[1:-1] - Tn[0:-2]))
 T_curr[0] = Tn[0] 2.0 * alpha_nodes[0] * dt / dx2 * (Tn[1] - Tn[0] (h_t[n] * dx / k_nodes[0]) * (T_air_t[n] - Tn[0]))
 T_curr[-1] = Tn[-1] 2.0 * alpha_nodes[-1] * dt / dx2 * (Tn[-2] - Tn[-1])
 wn = w_curr.copy()
 w_curr[1:-1] = wn[1:-1] D_w_nodes_base[1:-1] * dt / dx2 * (wn[2:] - 2.0 * wn[1:-1] wn[0:-2])
 RH_n = float(RH_t[n])
 if RH_n < 0.0:
 RH_n = 0.0
 if RH_n > 1.0:
 RH_n = 1.0
 w_eq = w0_skin_layer * (RH_n ** (1.0 / n_skin))
 w_curr[0] = min(wn[1], w_eq)
 w_curr[-1] = wn[-1] 2.0 * D_w_nodes_base[-1] * dt / dx2 * (wn[-2] - wn[-1])
 w_curr[w_curr < 1e-6] = 1e-6
 with np.errstate(divide='ignore', invalid='ignore'):
 aw_local = np.where(w0_nodes > 0.0, (w_curr / w0_nodes) ** n_iso_nodes, 0.0)
 aw_local = np.clip(aw_local, 0.0, 1.0)
 T_local_C = T_curr.copy()
 r_tilde_local = interp_rtilde(T_local_C)
 g_M_local = g_M_func(aw_local)
 S_M_local = P_nc_nodes * r_tilde_local * g_M_local
 M_curr = M_curr dt * S_M_local
 T_core_hist[n] = T_curr[-1]
 T_core_hist[-1] = T_curr[-1]
 assert np.all(np.isfinite(T_curr)), 'Non-finite T_curr in run_schedule'
 assert np.all(np.isfinite(w_curr)), 'Non-finite w_curr in run_schedule'
 assert np.all(np.isfinite(M_curr)), 'Non-finite M_curr in run_schedule'
 w_skin_0 = float(np.mean(w0_nodes[skin_nodes]))
 w_skin_f = float(np.mean(w_curr[skin_nodes]))
 M_skin_bar = float(np.mean(M_curr[skin_nodes]))
 C_gross = M_skin_bar * (1.0 - w_skin_f / max(w_skin_0, 1e-8))
 w_muscle_0 = float(np.mean(w0_nodes[muscle_nodes]))
 w_muscle_f = float(np.mean(w_curr[muscle_nodes]))
 R_w = w_muscle_f / max(w_muscle_0, 1e-8)
 T_muscle_final = T_curr[muscle_nodes]
 f_melt_muscle_bar = float(np.mean(f_melt_from_T(T_muscle_final)))
 J_gross = R_w * (1.0 - lambda_J * (f_melt_muscle_bar - f_melt_star) ** 2)
 if times[-1] <= times[0]:
 R_raw = 0.0
 else:
 R_raw = float((1.0 / (times[-1] - times[0])) * np.sum(0.5 * (((T_core_hist[:-1] - T_safe) ** 2) ((T_core_hist[1:] - T_safe) ** 2)) * (times[1:] - times[:-1])))
 return C_gross, J_gross, R_raw

np.random.seed(123)

base_t = {'t3': 30.0, 't4': 60.0, 't5': 25.0, 't6': 25.0, 't7': 5.0, 't8': 5.0}

schedules = []

params_mario = {'name': 'Mario', 't2': 6.0, 'T3': 80.0, 'T4': 90.0, 'T5': 130.0, 'T6': 230.0, 'T7': 240.0, 'T8': 250.0, 't3': base_t['t3'], 't4': base_t['t4'], 't5': base_t['t5'], 't6': base_t['t6'], 't7': base_t['t7'], 't8': base_t['t8'], 'RH3': 1.0, 'RH4': 0.50, 'RH5': 0.50}
schedules.append(params_mario)

N_total = 300
for k in range(N_total - 1):
 p = {}
 p['name'] = 'cand_' str(k 1)
 p['t2'] = 2.0 5.0 * np.random.rand()
 p['T3'] = 75.0 10.0 * np.random.rand()
 p['T4'] = 80.0 20.0 * np.random.rand()
 p['T5'] = 130.0 40.0 * np.random.rand()
 p['T6'] = 220.0 40.0 * np.random.rand()
 p['T7'] = 220.0 40.0 * np.random.rand()
 p['T8'] = 220.0 40.0 * np.random.rand()
 for key in ['t3', 't4', 't5', 't6', 't7', 't8']:
 delta = -0.5 np.random.rand()
 tval = base_t[key] * (1.0 delta)
 tval = max(0.1 * base_t[key], tval)
 p[key] = tval
 p['RH3'] = 1.0
 p['RH4'] = 0.30 0.40 * np.random.rand()
 p['RH5'] = 0.30 0.40 * np.random.rand()
 schedules.append(p)

progress = phase_setup_weight phase_read_weight phase_properties_weight phase_schedule_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

rows = []
N_sched = len(schedules)
sim_progress_start = progress

for idx, par in enumerate(schedules):
 Cg, Jg, Rr = run_schedule(par)
 rec = {}
 for key, val in par.items():
 rec[key] = float(val) if isinstance(val, (int, float, np.floating)) else val
 rec['C_gross'] = float(Cg)
 rec['J_gross'] = float(Jg)
 rec['R_raw'] = float(Rr)
 rows.append(rec)
 frac = float(idx 1) / float(N_sched)
 prog_local = sim_progress_start phase_sim_weight * frac
 if prog_local > total_progress:
 prog_local = total_progress
 msgLog = f'Executed: {prog_local:.1f}%'
 send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

candidates = pd.DataFrame(rows)
assert not candidates[['C_gross', 'J_gross', 'R_raw']].isna().any().any(), 'NaN in C_gross, J_gross or R_raw'

C_min = float(candidates['C_gross'].min())
C_max = float(candidates['C_gross'].max())
J_min = float(candidates['J_gross'].min())
J_max = float(candidates['J_gross'].max())
R_min = float(candidates['R_raw'].min())
R_max = float(candidates['R_raw'].max())

candidates['C_norm'] = (candidates['C_gross'] - C_min) / (C_max - C_min epsilon_norm)
candidates['J_norm'] = (candidates['J_gross'] - J_min) / (J_max - J_min epsilon_norm)
candidates['R_norm'] = (R_max - candidates['R_raw']) / (R_max - R_min epsilon_norm)

assert np.isfinite(candidates[['C_norm', 'J_norm', 'R_norm']].to_numpy(dtype=float)).all(), 'Non-finite normalized indices'

N = candidates.shape[0]
is_pareto = np.ones(N, dtype=bool)
C_arr = candidates['C_norm'].to_numpy(dtype=float)
J_arr = candidates['J_norm'].to_numpy(dtype=float)
R_arr = candidates['R_norm'].to_numpy(dtype=float)
for i in range(N):
 if not is_pareto[i]:
 continue
 Ci = C_arr[i]
 Ji = J_arr[i]
 Ri = R_arr[i]
 for j in range(N):
 if i == j:
 continue
 if C_arr[j] >= Ci and J_arr[j] >= Ji and R_arr[j] >= Ri and (C_arr[j] > Ci or J_arr[j] > Ji or R_arr[j] > Ri):
 is_pareto[i] = False
 break

candidates['is_pareto'] = is_pareto
candidates['Phi'] = (candidates['C_norm'] candidates['J_norm'] candidates['R_norm']) / 3.0
candidates['is_mario'] = candidates['name'].astype(str) == 'Mario'

X_pca = candidates[['C_norm', 'J_norm', 'R_norm']].to_numpy(dtype=float)
assert np.isfinite(X_pca).all(), 'Non-finite values in PCA inputs'
pca = PCA(n_components=2)
PC = pca.fit_transform(X_pca)
candidates['PC1'] = PC[:, 0]
candidates['PC2'] = PC[:, 1]

pareto_df = candidates[candidates['is_pareto']].copy()
pareto_df = pareto_df.sort_values('C_norm').reset_index(drop=True)

n_pareto = pareto_df.shape[0]
assert n_pareto >= 1, 'No Pareto points found'

q_vals = np.linspace(0.0, 1.0, 15)
selected_names = set()
for q in q_vals:
 idx_p = int(round(q * float(n_pareto - 1)))
 if idx_p < 0:
 idx_p = 0
 if idx_p > n_pareto - 1:
 idx_p = n_pareto - 1
 selected_names.add(str(pareto_df.loc[idx_p, 'name']))
selected_names.add('Mario')
candidates['is_test'] = candidates['name'].astype(str).isin(selected_names)

progress = phase_setup_weight phase_read_weight phase_properties_weight phase_schedule_weight phase_sim_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

ranges_records = []
ranges_records.append({'var': 't2', 'min': 2.0, 'max': 7.0})
ranges_records.append({'var': 'T3', 'min': 75.0, 'max': 85.0})
ranges_records.append({'var': 'T4', 'min': 80.0, 'max': 100.0})
ranges_records.append({'var': 'T5', 'min': 130.0, 'max': 170.0})
ranges_records.append({'var': 'T6_8', 'min': 220.0, 'max': 260.0})
ranges_records.append({'var': 'RH4_5', 'min': 0.30, 'max': 0.70})
DesignRanges_df = pd.DataFrame(ranges_records)

Pareto_out_df = candidates[candidates['is_pareto']].copy()
Experimental_df = candidates[candidates['is_test']].copy()

movement4_excel = 'movement4_tables.xlsx'
movement4_excel_path = os.path.join(base_path, movement4_excel)
with pd.ExcelWriter(movement4_excel_path) as writer:
 DesignRanges_df.to_excel(writer, sheet_name='DesignRanges', index=False)
 candidates.to_excel(writer, sheet_name='Candidates', index=False)
 Pareto_out_df.to_excel(writer, sheet_name='Pareto', index=False)
 Experimental_df.to_excel(writer, sheet_name='ExperimentalTests', index=False)

assert not candidates.isna().any().any(), 'NaN values detected in candidates before plotting/export'

pareto_mask = candidates['is_pareto'].to_numpy(dtype=bool)
tests_df = candidates[candidates['is_test']].copy()
mario_df = candidates[candidates['is_mario']].copy()
assert mario_df.shape[0] == 1, 'Mario reference schedule not found or not unique'
mario_row = mario_df.iloc[0]

image_files = []
html_files = []

fig_CJ, ax_CJ = plt.subplots(figsize=(6, 5))
ax_CJ.scatter(candidates['C_norm'].to_numpy(dtype=float), candidates['J_norm'].to_numpy(dtype=float), s=15.0, c='lightgray', alpha=0.4, label='All candidates')
sc_CJ = ax_CJ.scatter(candidates.loc[pareto_mask, 'C_norm'].to_numpy(dtype=float), candidates.loc[pareto_mask, 'J_norm'].to_numpy(dtype=float), c=candidates.loc[pareto_mask, 'R_norm'].to_numpy(dtype=float), cmap='viridis', s=35.0, edgecolors='k', label='Pareto front')
cbar_CJ = plt.colorbar(sc_CJ, ax=ax_CJ, label='R_norm')
ax_CJ.scatter(tests_df['C_norm'].to_numpy(dtype=float), tests_df['J_norm'].to_numpy(dtype=float), s=80.0, facecolors='none', edgecolors='black', marker='D', label='Experimental tests')
ax_CJ.scatter(float(mario_row['C_norm']), float(mario_row['J_norm']), s=140.0, marker='*', color='red', edgecolors='k', label='Mario (reference)')
ax_CJ.set_xlabel('C_norm')
ax_CJ.set_ylabel('J_norm')
ax_CJ.set_title('C_norm vs J_norm (color = R_norm)')
ax_CJ.legend(loc='best', fontsize=8)
plt.tight_layout()
CJ_png = 'movement4_CJ_scatter.png'
CJ_png_path = os.path.join(base_path, CJ_png)
plt.savefig(CJ_png_path, dpi=200)
plt.close(fig_CJ)
image_files.append(CJ_png)

fig_CJ_px = px.scatter(candidates, x='C_norm', y='J_norm', color='R_norm', color_continuous_scale='Viridis', hover_data=['name', 't2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8'])
fig_CJ_px.add_scatter(x=tests_df['C_norm'], y=tests_df['J_norm'], mode='markers', marker=dict(symbol='diamond-open', size=11.0, color='black', line=dict(width=1.0, color='white')), name='Experimental tests')
fig_CJ_px.add_scatter(x=[float(mario_row['C_norm'])], y=[float(mario_row['J_norm'])], mode='markers', marker=dict(symbol='star', size=14.0, color='red', line=dict(width=1.0, color='black')), name='Mario (reference)')
fig_CJ_px.update_layout(title='C_norm vs J_norm (Pig schedules)', xaxis_title='C_norm', yaxis_title='J_norm')
CJ_html = 'movement4_CJ_scatter.html'
CJ_html_path = os.path.join(base_path, CJ_html)
pio.write_html(fig_CJ_px, CJ_html_path, include_plotlyjs='cdn')
html_files.append(CJ_html)

fig_CR, ax_CR = plt.subplots(figsize=(6, 5))
ax_CR.scatter(candidates['C_norm'].to_numpy(dtype=float), candidates['R_norm'].to_numpy(dtype=float), s=15.0, c='lightgray', alpha=0.4, label='All candidates')
sc_CR = ax_CR.scatter(candidates.loc[pareto_mask, 'C_norm'].to_numpy(dtype=float), candidates.loc[pareto_mask, 'R_norm'].to_numpy(dtype=float), c=candidates.loc[pareto_mask, 'J_norm'].to_numpy(dtype=float), cmap='plasma', s=35.0, edgecolors='k', label='Pareto front')
cbar_CR = plt.colorbar(sc_CR, ax=ax_CR, label='J_norm')
ax_CR.scatter(tests_df['C_norm'].to_numpy(dtype=float), tests_df['R_norm'].to_numpy(dtype=float), s=80.0, facecolors='none', edgecolors='black', marker='D', label='Experimental tests')
ax_CR.scatter(float(mario_row['C_norm']), float(mario_row['R_norm']), s=140.0, marker='*', color='red', edgecolors='k', label='Mario (reference)')
ax_CR.set_xlabel('C_norm')
ax_CR.set_ylabel('R_norm')
ax_CR.set_title('C_norm vs R_norm (color = J_norm)')
ax_CR.legend(loc='best', fontsize=8)
plt.tight_layout()
CR_png = 'movement4_CR_scatter.png'
CR_png_path = os.path.join(base_path, CR_png)
plt.savefig(CR_png_path, dpi=200)
plt.close(fig_CR)
image_files.append(CR_png)

fig_CR_px = px.scatter(candidates, x='C_norm', y='R_norm', color='J_norm', color_continuous_scale='Plasma', hover_data=['name', 't2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8'])
fig_CR_px.add_scatter(x=tests_df['C_norm'], y=tests_df['R_norm'], mode='markers', marker=dict(symbol='diamond-open', size=11.0, color='black', line=dict(width=1.0, color='white')), name='Experimental tests')
fig_CR_px.add_scatter(x=[float(mario_row['C_norm'])], y=[float(mario_row['R_norm'])], mode='markers', marker=dict(symbol='star', size=14.0, color='red', line=dict(width=1.0, color='black')), name='Mario (reference)')
fig_CR_px.update_layout(title='C_norm vs R_norm (Pig schedules)', xaxis_title='C_norm', yaxis_title='R_norm')
CR_html = 'movement4_CR_scatter.html'
CR_html_path = os.path.join(base_path, CR_html)
pio.write_html(fig_CR_px, CR_html_path, include_plotlyjs='cdn')
html_files.append(CR_html)

fig_JR, ax_JR = plt.subplots(figsize=(6, 5))
ax_JR.scatter(candidates['J_norm'].to_numpy(dtype=float), candidates['R_norm'].to_numpy(dtype=float), s=15.0, c='lightgray', alpha=0.4, label='All candidates')
sc_JR = ax_JR.scatter(candidates.loc[pareto_mask, 'J_norm'].to_numpy(dtype=float), candidates.loc[pareto_mask, 'R_norm'].to_numpy(dtype=float), c=candidates.loc[pareto_mask, 'C_norm'].to_numpy(dtype=float), cmap='inferno', s=35.0, edgecolors='k', label='Pareto front')
cbar_JR = plt.colorbar(sc_JR, ax=ax_JR, label='C_norm')
ax_JR.scatter(tests_df['J_norm'].to_numpy(dtype=float), tests_df['R_norm'].to_numpy(dtype=float), s=80.0, facecolors='none', edgecolors='black', marker='D', label='Experimental tests')
ax_JR.scatter(float(mario_row['J_norm']), float(mario_row['R_norm']), s=140.0, marker='*', color='red', edgecolors='k', label='Mario (reference)')
ax_JR.set_xlabel('J_norm')
ax_JR.set_ylabel('R_norm')
ax_JR.set_title('J_norm vs R_norm (color = C_norm)')
ax_JR.legend(loc='best', fontsize=8)
plt.tight_layout()
JR_png = 'movement4_JR_scatter.png'
JR_png_path = os.path.join(base_path, JR_png)
plt.savefig(JR_png_path, dpi=200)
plt.close(fig_JR)
image_files.append(JR_png)

fig_JR_px = px.scatter(candidates, x='J_norm', y='R_norm', color='C_norm', color_continuous_scale='Inferno', hover_data=['name', 't2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8'])
fig_JR_px.add_scatter(x=tests_df['J_norm'], y=tests_df['R_norm'], mode='markers', marker=dict(symbol='diamond-open', size=11.0, color='black', line=dict(width=1.0, color='white')), name='Experimental tests')
fig_JR_px.add_scatter(x=[float(mario_row['J_norm'])], y=[float(mario_row['R_norm'])], mode='markers', marker=dict(symbol='star', size=14.0, color='red', line=dict(width=1.0, color='black')), name='Mario (reference)')
fig_JR_px.update_layout(title='J_norm vs R_norm (Pig schedules)', xaxis_title='J_norm', yaxis_title='R_norm')
JR_html = 'movement4_JR_scatter.html'
JR_html_path = os.path.join(base_path, JR_html)
pio.write_html(fig_JR_px, JR_html_path, include_plotlyjs='cdn')
html_files.append(JR_html)

progress = total_progress
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

N_total_eff = int(candidates.shape[0])
N_pareto_eff = int(candidates['is_pareto'].sum())
N_tests_eff = int(candidates['is_test'].sum())

metrics = {}
metrics['N_total'] = float(f'{N_total_eff:.2f}')
metrics['N_pareto'] = float(f'{N_pareto_eff:.2f}')
metrics['N_tests'] = float(f'{N_tests_eff:.2f}')
metrics['Mario_C_norm'] = float(f'{float(mario_row["C_norm"]):.2f}')
metrics['Mario_J_norm'] = float(f'{float(mario_row["J_norm"]):.2f}')
metrics['Mario_R_norm'] = float(f'{float(mario_row["R_norm"]):.2f}')
metrics['Mario_Phi'] = float(f'{float(mario_row["Phi"]):.2f}')

tables = {}
tables['DesignRanges'] = DesignRanges_df.to_dict(orient='list')

result = {}
result['status'] = 'ok'
result['description'] = 'Executed Movement 4 multiobjective optimization for Jhon Dallas suckling pig using the 1D multilayer PDE solver as a direct simulator, generated a cloud of thermal schedules, built the Pareto front in (C_norm, J_norm, R_norm), and highlighted Mario’s reference schedule and ~15 experimental tests in the scatter plots.'
result['metrics'] = metrics
result['tables'] = tables
result['images'] = image_files
result['caption'] = ['C_norm vs J_norm (cloud, Pareto, Mario, tests)', 'C_norm vs R_norm (cloud, Pareto, Mario, tests)', 'J_norm vs R_norm (cloud, Pareto, Mario, tests)']
result['files'] = [movement4_excel] html_files
result['excel_file'] = movement4_excel
result['image_files'] = image_files
result['html_files'] = html_files
summary = {}
summary['N_total'] = N_total_eff
summary['N_pareto'] = N_pareto_eff
summary['N_tests'] = N_tests_eff
summary['Mario_C_norm'] = float(mario_row['C_norm'])
summary['Mario_J_norm'] = float(mario_row['J_norm'])
summary['Mario_R_norm'] = float(mario_row['R_norm'])
summary['Mario_Phi'] = float(mario_row['Phi'])
summary['Experimental_names'] = list(sorted(selected_names))
result['summary'] = summary