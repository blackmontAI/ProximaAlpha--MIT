import os
import math
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from send_message_backend import send_message_backend

warnings.filterwarnings('default')

base_path = '/mnt/z/B011'
if not os.path.isdir(base_path):
 os.makedirs(base_path, exist_ok=True)

progress = 0.0
total_progress = 100.0
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# === Parameters from data_string and previous movements ===
N_nodes = 121
T_init_C = 4.0
n_skin = 0.85
n_fat = 0.82
n_muscle = 0.90
lambda_J = 1.0
f_melt_star = 0.7
T_ref_core_C = 75.0
epsilon_norm = 1e-6
L_v = 2.4e6

phase_setup_weight = 5.0
phase_read_weight = 10.0
phase_properties_weight = 10.0
phase_dt_weight = 5.0
phase_baseline_pde_weight = 40.0
phase_sensitivity_weight = 15.0
phase_tables_weight = 7.5
phase_figures_weight = 7.5

progress = phase_setup_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

movement1_path = os.path.join(base_path, 'movement1_tables.xlsx')
movement2_path = os.path.join(base_path, 'movement2_tables.xlsx')
assert os.path.isfile(movement1_path), 'movement1_tables.xlsx not found in /mnt/z/B011'
assert os.path.isfile(movement2_path), 'movement2_tables.xlsx not found in /mnt/z/B011'

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
indices_expected_cols = ['Species', 'A_M', 'A_L', 'A_G', 'A_S', 'A_M_rel', 'A_L_rel', 'A_G_rel', 'A_S_rel', 'R_aroma_norm', 'R_aroma_PCA']

layers_df = read_validated_sheet(movement1_path, 'Layers', layers_expected_cols)
try:
 drying_df = read_validated_sheet(movement1_path, 'Drying', drying_expected_cols)
except Exception:
 drying_df = read_validated_sheet(movement1_path, 'DryingSummary', drying_expected_cols)

pools_df = read_validated_sheet(movement2_path, 'Pools', pools_expected_cols)
kinetics_df = read_validated_sheet(movement2_path, 'Kinetics', kinetics_expected_cols)
indices2_df = read_validated_sheet(movement2_path, 'Indices', indices_expected_cols)

progress = phase_setup_weight phase_read_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

pig_layers = layers_df[layers_df['Species'].astype(str) == 'Pig'].copy()
assert pig_layers.shape[0] >= 3, 'Not enough Pig layer rows in Layers sheet'
pig_pools = pools_df[pools_df['Species'].astype(str) == 'Pig'].copy()
assert pig_pools.shape[0] >= 3, 'Not enough Pig layer rows in Pools sheet'
pig_drying = drying_df[drying_df['Species'].astype(str) == 'Pig'].copy()
assert pig_drying.shape[0] >= 2, 'Not enough Pig layer rows in Drying sheet'

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

assert not math.isnan(F_unsat_skin) and not math.isnan(F_unsat_fat) and not math.isnan(F_unsat_muscle), 'F_unsat_l values contain NaN for Pig layers'

F_tot = F_l_skin F_l_fat F_l_muscle
F_unsat_tot = F_unsat_skin F_unsat_fat F_unsat_muscle
assert F_tot > 0.0, 'Total fat F_tot must be positive to compute f_SFA'
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

def f_melt_from_T(T_array):
 z = (T_array - T50) * a_param
 z = np.clip(z, -50.0, 50.0)
 f = 1.0 / (1.0 np.exp(-z))
 f[f < 0.0] = 0.0
 f[f > 1.0] = 1.0
 return f

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

wpf_skin_sum = w_skin p_skin f_skin
wpf_fat_sum = w_fat p_fat f_fat
wpf_muscle_sum = w_muscle p_muscle f_muscle
wpf_tolerance = 0.05
if not (abs(wpf_skin_sum - 1.0) < wpf_tolerance and abs(wpf_fat_sum - 1.0) < wpf_tolerance and abs(wpf_muscle_sum - 1.0) < wpf_tolerance):
 if wpf_skin_sum > 0.0:
 w_skin = w_skin / wpf_skin_sum
 p_skin = p_skin / wpf_skin_sum
 f_skin = f_skin / wpf_skin_sum
 wpf_skin_sum = w_skin p_skin f_skin
 if wpf_fat_sum > 0.0:
 w_fat = w_fat / wpf_fat_sum
 p_fat = p_fat / wpf_fat_sum
 f_fat = f_fat / wpf_fat_sum
 wpf_fat_sum = w_fat p_fat f_fat
 if wpf_muscle_sum > 0.0:
 w_muscle = w_muscle / wpf_muscle_sum
 p_muscle = p_muscle / wpf_muscle_sum
 f_muscle = f_muscle / wpf_muscle_sum
 wpf_muscle_sum = w_muscle p_muscle f_muscle
 warnings.warn('w p f were not approximately 1; components normalized per layer', UserWarning)

rho_water = 1000.0
cp_water = 4180.0
k_water = 0.6
rho_fat_pure = 900.0
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

cp_layers = np.array([cp_skin, cp_fat_eff, cp_muscle], dtype=float)
k_layers = np.array([k_skin_eff, k_fat_eff, k_muscle_eff], dtype=float)

D_w_skin = 5e-11
D_w_fat = 5e-12
D_w_muscle = 1e-10
D_w_layers = np.array([D_w_skin, D_w_fat, D_w_muscle], dtype=float)

dry_skin_row = get_layer_row(pig_drying, 'skin')
dry_fat_row = get_layer_row(pig_drying, 'fat')
w_tdry_skin = float(dry_skin_row['w_tdry'])
w_tdry_fat = float(dry_fat_row['w_tdry'])
assert w_tdry_skin > 0.0 and w_tdry_fat > 0.0, 'w_tdry must be positive for skin and fat'

w0_skin_layer = w_tdry_skin
w0_fat_layer = w_tdry_fat
w0_muscle_layer = w_muscle

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

indices_pig_row = indices2_df[indices2_df['Species'].astype(str) == 'Pig']
assert indices_pig_row.shape[0] >= 1, 'Pig row not found in Movement2 Indices sheet'
indices_pig_row = indices_pig_row.iloc[0]
R_aroma_norm_pig = float(indices_pig_row['R_aroma_norm'])

species_name = 'Pig'
layer_names = ['skin', 'fat', 'muscle']
L_layers_vec = np.array([L_skin, L_fat, L_muscle], dtype=float)
rho_layers_vec = rho_layers.copy()
cp_layers_vec = cp_layers.copy()
k_layers_vec = k_layers.copy()
D_w_layers_vec = D_w_layers.copy()
w_layers_vec = np.array([w_skin, w_fat, w_muscle], dtype=float)
f_layers_vec = np.array([f_skin, f_fat, f_muscle], dtype=float)

L_total = float(L_skin L_fat L_muscle)
dx = L_total / float(N_nodes - 1)
x_nodes = np.linspace(0.0, L_total, N_nodes)
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
 rho_nodes[i] = rho_layers_vec[li]
 cp_nodes[i] = cp_layers_vec[li]
 k_nodes_base[i] = k_layers_vec[li]
 D_w_nodes_base[i] = D_w_layers_vec[li]
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
dx2 = dx * dx
dt_init = 0.5
dt_candidate = dt_init
for _ in range(20):
 Fo_max_candidate = float(np.max(alpha_nodes_base * dt_candidate / dx2))
 if Fo_max_candidate <= 0.2:
 break
 dt_candidate *= 0.5
dt_temp = dt_candidate

durations_min = [30.0, 60.0, 25.0, 25.0, 5.0, 5.0]
durations_sec = np.array(durations_min, dtype=float) * 60.0
total_time_sec = float(np.sum(durations_sec))

for _ in range(20):
 N_steps = int(math.ceil(total_time_sec / dt_temp)) 1
 dt_eff = total_time_sec / float(N_steps - 1)
 Fo_max_used = float(np.max(alpha_nodes_base * dt_eff / dx2))
 if Fo_max_used <= 0.2 or dt_eff < 1e-4:
 dt = dt_eff
 break
 dt_temp *= 0.5

times = np.linspace(0.0, total_time_sec, N_steps)
assert times.shape[0] == N_steps, 'Time grid size mismatch'

skin_nodes = np.where(layer_index_nodes == 0)[0]
fat_nodes = np.where(layer_index_nodes == 1)[0]
muscle_nodes = np.where(layer_index_nodes == 2)[0]
assert skin_nodes.size > 0 and fat_nodes.size > 0 and muscle_nodes.size > 0, 'Empty layer node sets detected'

idx_skin_fat_interface = int(np.argmin(np.abs(x_nodes - L_skin)))

T_min_C = 80.0
T_max_C = 260.0
n_T_points = 300
T_C_grid = np.linspace(T_min_C, T_max_C, int(n_T_points))
T_K_grid = T_C_grid 273.15
r_M_grid = np.exp(-E_M_Jmol / (R_gas * T_K_grid))
max_r_M = float(np.max(r_M_grid))
assert max_r_M > 0.0, 'Maximum Maillard rate is non-positive'
r_tilde_M_grid = r_M_grid / max_r_M

T_air_phase_base_C = np.array([80.0, 90.0, 130.0, 230.0, 240.0, 250.0], dtype=float)
RH_phase_base = np.array([0.95, 0.85, 0.70, 0.60, 0.50, 0.40], dtype=float)
h_phase_base = np.array([20.0, 25.0, 30.0, 35.0, 40.0, 45.0], dtype=float)
phase_labels = ['F3', 'F4', 'F5', 'F6', 'F7', 'F8']
n_phases = len(durations_sec)
assert T_air_phase_base_C.shape[0] == n_phases and RH_phase_base.shape[0] == n_phases and h_phase_base.shape[0] == n_phases, 'Phase arrays length mismatch'

alpha_layers = k_layers_vec / (rho_layers_vec * cp_layers_vec)
alpha_eff = float(np.sum(alpha_layers * L_layers_vec) / L_total)

progress = phase_setup_weight phase_read_weight phase_properties_weight phase_dt_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)
baseline_progress_start = progress

N_save = 200
if N_save > N_steps:
 N_save = N_steps
save_indices = np.linspace(0, N_steps - 1, N_save, dtype=int)

def build_schedule(T_phase_C, RH_phase, h_phase):
 N_steps_local = times.shape[0]
 T_air_t = np.empty(N_steps_local, dtype=float)
 RH_t = np.empty(N_steps_local, dtype=float)
 h_t = np.empty(N_steps_local, dtype=float)
 phase_idx_time = np.empty(N_steps_local, dtype=int)
 cum_durations = np.cumsum(durations_sec)
 for p in range(n_phases):
 t_start = 0.0 if p == 0 else float(cum_durations[p - 1])
 t_end = float(cum_durations[p])
 if p < n_phases - 1:
 mask_array = (times >= t_start) & (times < t_end)
 else:
 mask_array = (times >= t_start) & (times <= t_end 1e-9)
 T_air_t[mask_array] = T_phase_C[p]
 RH_t[mask_array] = RH_phase[p]
 h_t[mask_array] = h_phase[p]
 phase_idx_time[mask_array] = p
 return T_air_t, RH_t, h_t, phase_idx_time

def run_pde_scenario(scen_name, k_skin_factor, D_w_skin_factor, h_factor, T_air_shift_6to8_C, store_time_series, store_maps, compute_phase_metrics):
 k_nodes = k_nodes_base.copy()
 if k_skin_factor != 1.0:
 k_nodes[skin_nodes] = k_nodes[skin_nodes] * k_skin_factor
 D_w_nodes = D_w_nodes_base.copy()
 if D_w_skin_factor != 1.0:
 D_w_nodes[skin_nodes] = D_w_nodes[skin_nodes] * D_w_skin_factor
 alpha_nodes = k_nodes / (rho_nodes * cp_nodes)
 k_face = 0.5 * (k_nodes[:-1] k_nodes[1:])
 T_phase = T_air_phase_base_C.copy()
 if abs(T_air_shift_6to8_C) > 0.0:
 T_phase[3] = T_phase[3] T_air_shift_6to8_C
 T_phase[4] = T_phase[4] T_air_shift_6to8_C
 T_phase[5] = T_phase[5] T_air_shift_6to8_C
 h_phase = h_phase_base.copy() * h_factor
 T_air_t, RH_t, h_t, phase_idx_time = build_schedule(T_phase, RH_phase_base, h_phase)
 T_curr = np.full(N_nodes, T_init_C, dtype=float)
 w_curr = w0_nodes.copy()
 M_curr = np.zeros(N_nodes, dtype=float)
 if store_time_series:
 N_steps_local = times.shape[0]
 T_surf_time = np.zeros(N_steps_local, dtype=float)
 T_core_time = np.zeros(N_steps_local, dtype=float)
 T_if_time = np.zeros(N_steps_local, dtype=float)
 w_skin_time = np.zeros(N_steps_local, dtype=float)
 w_muscle_time = np.zeros(N_steps_local, dtype=float)
 M_skin_time = np.zeros(N_steps_local, dtype=float)
 f_melt_musc_time = np.zeros(N_steps_local, dtype=float)
 dryness_front_time = np.zeros(N_steps_local, dtype=float)
 flux_conv_time = np.zeros(N_steps_local, dtype=float)
 U_internal_time = np.zeros(N_steps_local, dtype=float)
 water_mass_time = np.zeros(N_steps_local, dtype=float)
 M_total_time = np.zeros(N_steps_local, dtype=float)
 else:
 T_surf_time = None
 T_core_time = None
 T_if_time = None
 w_skin_time = None
 w_muscle_time = None
 M_skin_time = None
 f_melt_musc_time = None
 dryness_front_time = None
 flux_conv_time = None
 U_internal_time = None
 water_mass_time = None
 M_total_time = None
 if store_maps:
 T_save = np.zeros((N_save, N_nodes), dtype=float)
 w_save = np.zeros((N_save, N_nodes), dtype=float)
 M_save = np.zeros((N_save, N_nodes), dtype=float)
 times_save = np.zeros(N_save, dtype=float)
 else:
 T_save = None
 w_save = None
 M_save = None
 times_save = None
 R_integral = 0.0
 N_steps_local = times.shape[0]
 if store_time_series:
 T_surf_time[0] = T_curr[0]
 T_core_time[0] = T_curr[-1]
 T_if_time[0] = T_curr[idx_skin_fat_interface]
 w_skin_time[0] = float(np.mean(w_curr[skin_nodes]))
 w_muscle_time[0] = float(np.mean(w_curr[muscle_nodes]))
 M_skin_time[0] = float(np.mean(M_curr[skin_nodes]))
 f_melt_all_0 = f_melt_from_T(T_curr)
 assert np.all(np.isfinite(f_melt_all_0)), 'Non-finite f_melt at initial state'
 f_melt_musc_time[0] = float(np.mean(f_melt_all_0[muscle_nodes]))
 dryness_mask0 = (w_curr <= 0.8 * w0_nodes) & (x_nodes <= L_skin 1e-9)
 if np.any(dryness_mask0):
 dryness_front_time[0] = float(x_nodes[np.where(dryness_mask0)[0][-1]])
 else:
 dryness_front_time[0] = 0.0
 flux_conv_time[0] = h_t[0] * (T_air_t[0] - T_curr[0])
 U_internal_time[0] = float(np.sum(rho_nodes * cp_nodes * T_curr) * dx)
 water_mass_time[0] = float(np.sum(w_curr * rho_nodes) * dx)
 M_total_time[0] = float(np.sum(M_curr) * dx)
 save_ptr = 0
 if store_maps:
 T_save[save_ptr, :] = T_curr
 w_save[save_ptr, :] = w_curr
 M_save[save_ptr, :] = M_curr
 times_save[save_ptr] = times[0]
 save_ptr = 1 if store_maps and N_save > 1 else 0
 max_progress_reports = 20
 report_interval = max(1, (N_steps_local - 1) // max_progress_reports)
 for n in range(N_steps_local - 1):
 T_air_n = T_air_t[n]
 RH_n = RH_t[n]
 h_n = h_t[n]
 T_core_prev = T_curr[-1]
 flux_n = h_n * (T_air_n - T_curr[0])
 if store_time_series:
 flux_conv_time[n] = flux_n
 T_next = T_curr.copy()
 w_next = w_curr.copy()
 M_next = M_curr.copy()
 for i in range(1, N_nodes - 1):
 k_right = k_face[i]
 k_left = k_face[i - 1]
 flux_term = k_right * (T_curr[i 1] - T_curr[i]) - k_left * (T_curr[i] - T_curr[i - 1])
 T_next[i] = T_curr[i] (dt / (rho_nodes[i] * cp_nodes[i] * dx2)) * flux_term
 T_next[0] = T_curr[0] 2.0 * alpha_nodes[0] * dt / dx2 * (T_curr[1] - T_curr[0] (h_n * dx / k_nodes[0]) * (T_air_n - T_curr[0]))
 T_next[-1] = T_curr[-1] 2.0 * alpha_nodes[-1] * dt / dx2 * (T_curr[-2] - T_curr[-1])
 for i in range(1, N_nodes - 1):
 w_next[i] = w_curr[i] D_w_nodes[i] * dt / dx2 * (w_curr[i 1] - 2.0 * w_curr[i] w_curr[i - 1])
 w_eq = w0_nodes[0] * (RH_n ** (1.0 / n_skin))
 w_next[0] = min(w_curr[1], w_eq)
 w_next[-1] = w_curr[-1] 2.0 * D_w_nodes[-1] * dt / dx2 * (w_curr[-2] - w_curr[-1])
 w_next[w_next < 1e-6] = 1e-6
 ratio_w = w_next / w0_nodes
 aw_local = np.power(ratio_w, n_iso_nodes)
 aw_local[aw_local < 1e-6] = 1e-6
 aw_local[aw_local > 0.999999] = 0.999999
 T_local_C = T_next.copy()
 r_tilde_local = np.interp(T_local_C, T_C_grid, r_tilde_M_grid)
 g_M_local = np.power(aw_local, beta_M) * np.power(1.0 - aw_local, gamma_M)
 S_M_local = P_nc_nodes * r_tilde_local * g_M_local
 M_next = M_curr dt * S_M_local
 f_melt_all = f_melt_from_T(T_next)
 assert np.all(np.isfinite(f_melt_all)), 'Non-finite f_melt during simulation'
 T_core_new = T_next[-1]
 R_integral = 0.5 * ((T_core_prev - T_ref_core_C) ** 2 (T_core_new - T_ref_core_C) ** 2) * dt
 if store_time_series:
 idx_tp1 = n 1
 T_surf_time[idx_tp1] = T_next[0]
 T_core_time[idx_tp1] = T_core_new
 T_if_time[idx_tp1] = T_next[idx_skin_fat_interface]
 w_skin_time[idx_tp1] = float(np.mean(w_next[skin_nodes]))
 w_muscle_time[idx_tp1] = float(np.mean(w_next[muscle_nodes]))
 M_skin_time[idx_tp1] = float(np.mean(M_next[skin_nodes]))
 f_melt_musc_time[idx_tp1] = float(np.mean(f_melt_all[muscle_nodes]))
 dryness_mask = (w_next <= 0.8 * w0_nodes) & (x_nodes <= L_skin 1e-9)
 if np.any(dryness_mask):
 dryness_front_time[idx_tp1] = float(x_nodes[np.where(dryness_mask)[0][-1]])
 else:
 dryness_front_time[idx_tp1] = 0.0
 U_internal_time[idx_tp1] = float(np.sum(rho_nodes * cp_nodes * T_next) * dx)
 water_mass_time[idx_tp1] = float(np.sum(w_next * rho_nodes) * dx)
 M_total_time[idx_tp1] = float(np.sum(M_next) * dx)
 if store_maps and save_ptr < N_save and (n 1) in save_indices:
 idx_save = int(np.where(save_indices == (n 1))[0][0])
 T_save[idx_save, :] = T_next
 w_save[idx_save, :] = w_next
 M_save[idx_save, :] = M_next
 times_save[idx_save] = times[n 1]
 save_ptr = 1
 T_curr = T_next
 w_curr = w_next
 M_curr = M_next
 if scen_name == 'baseline' and (n % report_interval == 0):
 frac = float(n) / float(max(1, N_steps_local - 1))
 prog_local = baseline_progress_start phase_baseline_pde_weight * frac
 msgLog_local = f'Executed: {prog_local:.1f}%'
 send_message_backend(msgLog_local, 'GO', 'PROGRESS', backend_args)
 R_raw_value = R_integral / total_time_sec
 assert np.all(np.isfinite(T_curr)) and np.all(np.isfinite(w_curr)) and np.all(np.isfinite(M_curr)), 'Non-finite values in final state arrays'
 f_melt_final_all = f_melt_from_T(T_curr)
 assert np.all(np.isfinite(f_melt_final_all)), 'Non-finite f_melt in final state'
 M_skin_bar = float(np.mean(M_curr[skin_nodes]))
 w_skin_f = float(np.mean(w_curr[skin_nodes]))
 w_skin_0 = float(np.mean(w0_nodes[skin_nodes]))
 C_gross = M_skin_bar * (1.0 - w_skin_f / max(w_skin_0, 1e-8))
 w_muscle_f = float(np.mean(w_curr[muscle_nodes]))
 w_muscle_0 = float(np.mean(w0_nodes[muscle_nodes]))
 f_melt_muscle_bar = float(np.mean(f_melt_final_all[muscle_nodes]))
 J_gross = (w_muscle_f / max(w_muscle_0, 1e-8)) * (1.0 - lambda_J * (f_melt_muscle_bar - f_melt_star) ** 2)
 out = {}
 out['T_final'] = T_curr
 out['w_final'] = w_curr
 out['M_final'] = M_curr
 out['C_gross'] = float(C_gross)
 out['J_gross'] = float(J_gross)
 out['R_raw'] = float(R_raw_value)
 if store_time_series:
 out['times'] = times
 out['T_surf_time'] = T_surf_time
 out['T_core_time'] = T_core_time
 out['T_if_time'] = T_if_time
 out['w_skin_time'] = w_skin_time
 out['w_muscle_time'] = w_muscle_time
 out['M_skin_time'] = M_skin_time
 out['f_melt_musc_time'] = f_melt_musc_time
 out['dryness_front_time'] = dryness_front_time
 out['flux_conv_time'] = flux_conv_time
 out['U_internal_time'] = U_internal_time
 out['water_mass_time'] = water_mass_time
 out['M_total_time'] = M_total_time
 out['phase_idx_time'] = phase_idx_time
 if store_maps:
 out['T_save'] = T_save
 out['w_save'] = w_save
 out['M_save'] = M_save
 out['times_save'] = times_save
 return out

baseline_out = run_pde_scenario('baseline', 1.0, 1.0, 1.0, 0.0, True, True, True)

progress = baseline_progress_start phase_baseline_pde_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

k_skin_plus_out = run_pde_scenario('k_skin_plus10', 1.10, 1.0, 1.0, 0.0, False, False, False)
D_w_skin_plus_out = run_pde_scenario('D_w_skin_plus10', 1.0, 1.10, 1.0, 0.0, False, False, False)
h_plus_out = run_pde_scenario('h_plus10', 1.0, 1.0, 1.10, 0.0, False, False, False)
Tair_shift_out = run_pde_scenario('Tair_F6_8_plus5C', 1.0, 1.0, 1.0, 5.0, False, False, False)

progress = baseline_progress_start phase_baseline_pde_weight phase_sensitivity_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

C_gross_arr = np.zeros(5, dtype=float)
J_gross_arr = np.zeros(5, dtype=float)
R_raw_arr = np.zeros(5, dtype=float)
C_gross_arr[0] = baseline_out['C_gross']
J_gross_arr[0] = baseline_out['J_gross']
R_raw_arr[0] = baseline_out['R_raw']
C_gross_arr[1] = k_skin_plus_out['C_gross']
J_gross_arr[1] = k_skin_plus_out['J_gross']
R_raw_arr[1] = k_skin_plus_out['R_raw']
C_gross_arr[2] = D_w_skin_plus_out['C_gross']
J_gross_arr[2] = D_w_skin_plus_out['J_gross']
R_raw_arr[2] = D_w_skin_plus_out['R_raw']
C_gross_arr[3] = h_plus_out['C_gross']
J_gross_arr[3] = h_plus_out['J_gross']
R_raw_arr[3] = h_plus_out['R_raw']
C_gross_arr[4] = Tair_shift_out['C_gross']
J_gross_arr[4] = Tair_shift_out['J_gross']
R_raw_arr[4] = Tair_shift_out['R_raw']

assert np.all(np.isfinite(C_gross_arr)) and np.all(np.isfinite(J_gross_arr)) and np.all(np.isfinite(R_raw_arr)), 'Non-finite C,J,R_raw values detected across scenarios'

C_min = float(np.min(C_gross_arr))
C_max = float(np.max(C_gross_arr))
J_min = float(np.min(J_gross_arr))
J_max = float(np.max(J_gross_arr))
R_min = float(np.min(R_raw_arr))
R_max = float(np.max(R_raw_arr))

C_norm_arr = (C_gross_arr - C_min) / max(C_max - C_min, epsilon_norm)
J_norm_arr = (J_gross_arr - J_min) / max(J_max - J_min, epsilon_norm)
R_norm_arr = (R_raw_arr - R_min) / max(R_max - R_min, epsilon_norm)

condition_ids = ['baseline', 'k_skin_plus10', 'D_w_skin_plus10', 'h_plus10', 'Tair_F6_8_plus5C']
R_aroma_norm_arr = np.array([R_aroma_norm_pig, R_aroma_norm_pig, R_aroma_norm_pig, R_aroma_norm_pig, R_aroma_norm_pig], dtype=float)

Properties_records = []
for idx, lname in enumerate(layer_names):
 Properties_records.append({'Species': species_name, 'Layer': lname, 'L_m': float(L_layers_vec[idx]), 'rho_l': float(rho_layers_vec[idx]), 'cp_l': float(cp_layers_vec[idx]), 'k_l': float(k_layers_vec[idx]), 'D_w_l': float(D_w_layers_vec[idx]), 'w_l': float(w_layers_vec[idx]), 'f_l': float(f_layers_vec[idx]), 'Source': 'MixtureCorrelation_Rahman_ChoiOkos'})
Properties_df = pd.DataFrame(Properties_records)
assert not Properties_df.isna().any().any(), 'Properties_df contains NaN'

Bi_phase = h_phase_base * L_skin / k_layers_vec[0]
Fo_phase = alpha_eff * durations_sec / (L_total * L_total)

BC_records = []
for i in range(n_phases):
 BC_records.append({'Phase': phase_labels[i], 'T_air_C': float(T_air_phase_base_C[i]), 'RH': float(RH_phase_base[i]), 'h_W_m2K': float(h_phase_base[i]), 'duration_min': float(durations_min[i]), 'Bi_skin': float(Bi_phase[i]), 'Fo_Ltotal': float(Fo_phase[i]), 'Description': 'Phase ' phase_labels[i]})
BCsAndAdim_df = pd.DataFrame(BC_records)
assert not BCsAndAdim_df.isna().any().any(), 'BCsAndAdim_df contains NaN'

times_full = baseline_out['times']
T_surf_time = baseline_out['T_surf_time']
T_core_time = baseline_out['T_core_time']
T_if_time = baseline_out['T_if_time']
w_skin_time = baseline_out['w_skin_time']
w_muscle_time = baseline_out['w_muscle_time']
M_skin_time = baseline_out['M_skin_time']
f_melt_musc_time = baseline_out['f_melt_musc_time']
dryness_front_time = baseline_out['dryness_front_time']
flux_conv_time = baseline_out['flux_conv_time']
U_internal_time = baseline_out['U_internal_time']
water_mass_time = baseline_out['water_mass_time']
M_total_time = baseline_out['M_total_time']
phase_idx_time = baseline_out['phase_idx_time']

assert times_full.shape[0] == T_surf_time.shape[0] == T_core_time.shape[0], 'Time series length mismatch'

w_skin_0_avg = float(np.mean(w0_nodes[skin_nodes]))
w_muscle_0_avg = float(np.mean(w0_nodes[muscle_nodes]))

PhaseEvo_records = []
for p in range(n_phases):
 mask_p = phase_idx_time == p
 assert np.any(mask_p), 'No time steps for phase ' phase_labels[p]
 T_surf_mean_p = float(np.mean(T_surf_time[mask_p]))
 T_core_mean_p = float(np.mean(T_core_time[mask_p]))
 w_skin_mean_p = float(np.mean(w_skin_time[mask_p]))
 w_muscle_mean_p = float(np.mean(w_muscle_time[mask_p]))
 M_skin_mean_p = float(np.mean(M_skin_time[mask_p]))
 f_melt_musc_mean_p = float(np.mean(f_melt_musc_time[mask_p]))
 PhaseEvo_records.append({'Phase': phase_labels[p], 'T_surf_mean_C': T_surf_mean_p, 'T_core_mean_C': T_core_mean_p, 'w_skin_mean': w_skin_mean_p, 'w_muscle_mean': w_muscle_mean_p, 'M_skin_mean': M_skin_mean_p, 'f_melt_muscle_mean': f_melt_musc_mean_p})
PhaseEvolution_df = pd.DataFrame(PhaseEvo_records)
assert not PhaseEvolution_df.isna().any().any(), 'PhaseEvolution_df contains NaN'

Q_conv_phase = np.zeros(n_phases, dtype=float)
Q_sensible_phase = np.zeros(n_phases, dtype=float)
Q_latent_phase = np.zeros(n_phases, dtype=float)
U_phase_start = np.zeros(n_phases, dtype=float)
U_phase_end = np.zeros(n_phases, dtype=float)
W_phase_start = np.zeros(n_phases, dtype=float)
W_phase_end = np.zeros(n_phases, dtype=float)

for n in range(times_full.shape[0] - 1):
 p = int(phase_idx_time[n])
 Q_conv_phase[p] = flux_conv_time[n] * dt
for p in range(n_phases):
 idx_p = np.where(phase_idx_time == p)[0]
 assert idx_p.size > 0, 'No indices for phase ' phase_labels[p]
 i_start = int(idx_p[0])
 i_end = int(idx_p[-1])
 U_phase_start[p] = U_internal_time[i_start]
 U_phase_end[p] = U_internal_time[i_end]
 W_phase_start[p] = water_mass_time[i_start]
 W_phase_end[p] = water_mass_time[i_end]
 Q_sensible_phase[p] = U_phase_end[p] - U_phase_start[p]
 Q_latent_phase[p] = (W_phase_start[p] - W_phase_end[p]) * L_v

Energy_records = []
for p in range(n_phases):
 Q_conv = Q_conv_phase[p]
 Q_sens = Q_sensible_phase[p]
 Q_lat = Q_latent_phase[p]
 if abs(Q_conv) > 0.0:
 frac_heat = Q_sens / Q_conv
 frac_evap = Q_lat / Q_conv
 frac_res = 1.0 - frac_heat - frac_evap
 else:
 frac_heat = 0.0
 frac_evap = 0.0
 frac_res = 0.0
 Energy_records.append({'Phase': phase_labels[p], 'Q_conv_J': float(Q_conv), 'Q_sensible_J': float(Q_sens), 'Q_latent_J': float(Q_lat), 'Frac_heating': float(frac_heat), 'Frac_evap': float(frac_evap), 'Frac_residual': float(frac_res)})
EnergyBalances_df = pd.DataFrame(Energy_records)
assert not EnergyBalances_df.isna().any().any(), 'EnergyBalances_df contains NaN'

T_final = baseline_out['T_final']
w_final = baseline_out['w_final']
M_final = baseline_out['M_final']
assert T_final.shape[0] == N_nodes and w_final.shape[0] == N_nodes and M_final.shape[0] == N_nodes, 'Final field shape mismatch'

grad_T_skin = np.diff(T_final[skin_nodes]) / dx
grad_w_skin = np.diff(w_final[skin_nodes]) / dx
thermal_grad_skin_mean = float(np.mean(np.abs(grad_T_skin)))
moisture_grad_skin_mean = float(np.mean(np.abs(grad_w_skin)))

dry_crust_mask = (w_final <= 0.5 * w0_nodes) & (x_nodes <= L_skin 1e-9)
if np.any(dry_crust_mask):
 crust_end_idx = int(np.where(dry_crust_mask)[0][-1])
 crust_thickness = float(x_nodes[crust_end_idx])
else:
 crust_end_idx = 0
 crust_thickness = 0.0

w_muscle_final_nodes = w_final[muscle_nodes]
w_muscle_final_avg = float(np.mean(w_muscle_final_nodes))
tol_muscle = 0.05 * max(w_muscle_final_avg, 1e-8)
transition_thickness = 0.0
if crust_end_idx 1 < N_nodes:
 indices_search = np.arange(crust_end_idx 1, N_nodes)
 for j in indices_search:
 if abs(w_final[j] - w_muscle_final_avg) <= tol_muscle:
 transition_thickness = float(x_nodes[j] - x_nodes[crust_end_idx])
 break

idx_M_max = int(np.argmax(M_final))
depth_M_max = float(x_nodes[idx_M_max])

CrustMetrics_records = [{'Species': species_name, 'Crust_thickness_m': crust_thickness, 'Thermal_grad_skin_mean_K_per_m': thermal_grad_skin_mean, 'Moisture_grad_skin_mean_per_m': moisture_grad_skin_mean, 'Depth_M_max_m': depth_M_max, 'Transition_thickness_m': transition_thickness}]
CrustMetrics_df = pd.DataFrame(CrustMetrics_records)
assert not CrustMetrics_df.isna().any().any(), 'CrustMetrics_df contains NaN'

Sensitivity_records = []
param_names = ['k_skin', 'D_w_skin', 'h_all', 'T_air_F6_8']
delta_params = [0.10, 0.10, 0.10, 5.0]
for idx in range(1, 5):
 delta_C = (C_norm_arr[idx] - C_norm_arr[0]) / max(abs(C_norm_arr[0]), epsilon_norm)
 delta_J = (J_norm_arr[idx] - J_norm_arr[0]) / max(abs(J_norm_arr[0]), epsilon_norm)
 delta_R = (R_norm_arr[idx] - R_norm_arr[0]) / max(abs(R_norm_arr[0]), epsilon_norm)
 Sensitivity_records.append({'Parameter': param_names[idx - 1], 'Delta_param': float(delta_params[idx - 1]), 'Delta_C_norm': float(delta_C), 'Delta_J_norm': float(delta_J), 'Delta_R_norm': float(delta_R)})
Sensitivity_df = pd.DataFrame(Sensitivity_records)
assert not Sensitivity_df.isna().any().any(), 'Sensitivity_df contains NaN'

Indices_records_3 = []
for i in range(5):
 Indices_records_3.append({'ID_cond': condition_ids[i], 'C_gross': float(C_gross_arr[i]), 'C_norm': float(C_norm_arr[i]), 'J_gross': float(J_gross_arr[i]), 'J_norm': float(J_norm_arr[i]), 'R_raw': float(R_raw_arr[i]), 'R_norm': float(R_norm_arr[i]), 'R_aroma_norm': float(R_aroma_norm_arr[i])})
Indices3_df = pd.DataFrame(Indices_records_3)
assert not Indices3_df.isna().any().any(), 'Indices3_df contains NaN'

movement3_excel = 'movement3_tables.xlsx'
movement3_excel_path = os.path.join(base_path, movement3_excel)
with pd.ExcelWriter(movement3_excel_path) as writer:
 Properties_df.to_excel(writer, sheet_name='Properties', index=False)
 BCsAndAdim_df.to_excel(writer, sheet_name='BCsAndAdim', index=False)
 PhaseEvolution_df.to_excel(writer, sheet_name='PhaseEvolution', index=False)
 EnergyBalances_df.to_excel(writer, sheet_name='EnergyBalances', index=False)
 CrustMetrics_df.to_excel(writer, sheet_name='CrustMetrics', index=False)
 Sensitivity_df.to_excel(writer, sheet_name='Sensitivity', index=False)
 Indices3_df.to_excel(writer, sheet_name='Indices', index=False)

progress = baseline_progress_start phase_baseline_pde_weight phase_sensitivity_weight phase_tables_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

T_save = baseline_out['T_save']
w_save = baseline_out['w_save']
M_save = baseline_out['M_save']
times_save = baseline_out['times_save']
assert T_save.shape[0] == times_save.shape[0], 'T_save and times_save size mismatch'
assert w_save.shape == T_save.shape and M_save.shape == T_save.shape, 'Saved field map shapes mismatch'

kinetics_png = 'movement3_T_multiscale.png'
drying_front_png = 'movement3_drying_front.png'
maps_png = 'movement3_maps.png'
regime_png = 'movement3_regime.png'
final_profiles_png = 'movement3_final_profiles.png'
performance_space_png = 'movement3_performance_space.png'
mech_phase_map_png = 'movement3_mech_phase_map.png'
robustness_png = 'movement3_robustness.png'

kinetics_html = 'movement3_T_multiscale.html'
drying_front_html = 'movement3_drying_front.html'
maps_html = 'movement3_maps.html'
regime_html = 'movement3_regime.html'
final_profiles_html = 'movement3_final_profiles.html'
performance_space_html = 'movement3_performance_space.html'
mech_phase_map_html = 'movement3_mech_phase_map.html'
robustness_html = 'movement3_robustness.html'

kinetics_png_path = os.path.join(base_path, kinetics_png)
drying_front_png_path = os.path.join(base_path, drying_front_png)
maps_png_path = os.path.join(base_path, maps_png)
regime_png_path = os.path.join(base_path, regime_png)
final_profiles_png_path = os.path.join(base_path, final_profiles_png)
performance_space_png_path = os.path.join(base_path, performance_space_png)
mech_phase_map_png_path = os.path.join(base_path, mech_phase_map_png)
robustness_png_path = os.path.join(base_path, robustness_png)

kinetics_html_path = os.path.join(base_path, kinetics_html)
drying_front_html_path = os.path.join(base_path, drying_front_html)
maps_html_path = os.path.join(base_path, maps_html)
regime_html_path = os.path.join(base_path, regime_html)
final_profiles_html_path = os.path.join(base_path, final_profiles_html)
performance_space_html_path = os.path.join(base_path, performance_space_html)
mech_phase_map_html_path = os.path.join(base_path, mech_phase_map_html)
robustness_html_path = os.path.join(base_path, robustness_html)

t_min_array = times_full / 60.0
fig_T, ax_T = plt.subplots(figsize=(10, 6))
ax_T.plot(t_min_array, T_surf_time, label='T_surf', linewidth=1.2)
ax_T.plot(t_min_array, T_core_time, label='T_core', linewidth=1.2)
ax_T.plot(t_min_array, T_if_time, label='T_skin-fat interface', linewidth=1.2)
cum_durations_min = np.cumsum(durations_min)
for bd in cum_durations_min:
 ax_T.axvline(bd, color='gray', linestyle='--', linewidth=0.7)
ax_T.set_xlabel('Time (min)')
ax_T.set_ylabel('Temperature (C)')
ax_T.set_title('Multiscale temperatures: surface, core, interface')
ax_T.grid(True, linestyle='--', linewidth=0.5)
ax_T.legend(loc='best')
plt.tight_layout()
plt.savefig(kinetics_png_path, bbox_inches='tight')
plt.close(fig_T)

fig_T_int = go.Figure()
fig_T_int.add_trace(go.Scatter(x=t_min_array, y=T_surf_time, mode='lines', name='T_surf'))
fig_T_int.add_trace(go.Scatter(x=t_min_array, y=T_core_time, mode='lines', name='T_core'))
fig_T_int.add_trace(go.Scatter(x=t_min_array, y=T_if_time, mode='lines', name='T_skin-fat interface'))
for bd in cum_durations_min:
 fig_T_int.add_vline(x=bd, line_width=0.7, line_dash='dash', line_color='gray')
fig_T_int.update_layout(title='Multiscale temperatures: surface, core, interface', xaxis_title='Time (min)', yaxis_title='Temperature (C)')
fig_T_int.write_html(kinetics_html_path, include_plotlyjs='cdn')

drying_front_mm = dryness_front_time * 1000.0
fig_DF, ax_DF = plt.subplots(figsize=(8, 5))
ax_DF.plot(t_min_array, drying_front_mm, linewidth=1.2)
ax_DF.set_xlabel('Time (min)')
ax_DF.set_ylabel('Drying front depth (mm)')
ax_DF.set_title('Drying front evolution (w <= 0.8 w0)')
ax_DF.grid(True, linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.savefig(drying_front_png_path, bbox_inches='tight')
plt.close(fig_DF)

fig_DF_int = go.Figure()
fig_DF_int.add_trace(go.Scatter(x=t_min_array, y=drying_front_mm, mode='lines', name='Drying front'))
fig_DF_int.update_layout(title='Drying front evolution (w <= 0.8 w0)', xaxis_title='Time (min)', yaxis_title='Depth (mm)')
fig_DF_int.write_html(drying_front_html_path, include_plotlyjs='cdn')

T_map = T_save.T
w_map = w_save.T
M_map = M_save.T
t_save_min = times_save / 60.0
x_mm = x_nodes * 1000.0

fig_maps, axes_maps = plt.subplots(3, 1, figsize=(10, 10), sharex=True)
im0 = axes_maps[0].imshow(T_map, aspect='auto', origin='lower', extent=[t_save_min[0], t_save_min[-1], x_mm[0], x_mm[-1]])
axes_maps[0].set_ylabel('x (mm)')
axes_maps[0].set_title('T(x,t)')
plt.colorbar(im0, ax=axes_maps[0], fraction=0.046, pad=0.04)
im1 = axes_maps[1].imshow(w_map, aspect='auto', origin='lower', extent=[t_save_min[0], t_save_min[-1], x_mm[0], x_mm[-1]])
axes_maps[1].set_ylabel('x (mm)')
axes_maps[1].set_title('w(x,t)')
plt.colorbar(im1, ax=axes_maps[1], fraction=0.046, pad=0.04)
im2 = axes_maps[2].imshow(M_map, aspect='auto', origin='lower', extent=[t_save_min[0], t_save_min[-1], x_mm[0], x_mm[-1]])
axes_maps[2].set_xlabel('Time (min)')
axes_maps[2].set_ylabel('x (mm)')
axes_maps[2].set_title('M(x,t)')
plt.colorbar(im2, ax=axes_maps[2], fraction=0.046, pad=0.04)
plt.tight_layout()
plt.savefig(maps_png_path, bbox_inches='tight')
plt.close(fig_maps)

fig_maps_int = make_subplots(rows=3, cols=1, shared_xaxes=True, subplot_titles=('T(x,t)', 'w(x,t)', 'M(x,t)'))
fig_maps_int.add_trace(go.Heatmap(x=t_save_min, y=x_mm, z=T_map, coloraxis='coloraxis'), row=1, col=1)
fig_maps_int.add_trace(go.Heatmap(x=t_save_min, y=x_mm, z=w_map, coloraxis='coloraxis2'), row=2, col=1)
fig_maps_int.add_trace(go.Heatmap(x=t_save_min, y=x_mm, z=M_map, coloraxis='coloraxis3'), row=3, col=1)
fig_maps_int.update_layout(coloraxis=dict(colorscale='Viridis'), coloraxis2=dict(colorscale='Blues'), coloraxis3=dict(colorscale='Inferno'), title='Space-time maps T, w, M', xaxis3_title='Time (min)', yaxis1_title='x (mm)', yaxis2_title='x (mm)', yaxis3_title='x (mm)')
fig_maps_int.write_html(maps_html_path, include_plotlyjs='cdn')

dryness_mean_skin_phase = np.zeros(n_phases, dtype=float)
for p in range(n_phases):
 mask_p = phase_idx_time == p
 w_skin_mean_p = float(np.mean(w_skin_time[mask_p]))
 dryness_mean_skin_phase[p] = 1.0 - w_skin_mean_p / max(w_skin_0_avg, 1e-8)

fig_reg, ax_reg = plt.subplots(figsize=(8, 6))
sc = ax_reg.scatter(Fo_phase, dryness_mean_skin_phase, c=Bi_phase, cmap='viridis')
for i, label in enumerate(phase_labels):
 ax_reg.text(Fo_phase[i], dryness_mean_skin_phase[i], label)
ax_reg.set_xlabel('Fo (L_total)')
ax_reg.set_ylabel('Mean skin dryness (1 - w_skin/w0)')
ax_reg.set_title('Physical regime diagram by phase')
ax_reg.grid(True, linestyle='--', linewidth=0.5)
plt.colorbar(sc, ax=ax_reg, label='Bi_skin')
plt.tight_layout()
plt.savefig(regime_png_path, bbox_inches='tight')
plt.close(fig_reg)

fig_reg_int = go.Figure()
fig_reg_int.add_trace(go.Scatter(x=Fo_phase, y=dryness_mean_skin_phase, mode='markers+text', text=phase_labels, textposition='top center', marker=dict(color=Bi_phase, colorscale='Viridis', colorbar=dict(title='Bi_skin'))))
fig_reg_int.update_layout(title='Physical regime diagram by phase', xaxis_title='Fo (L_total)', yaxis_title='Mean skin dryness (1 - w_skin/w0)')
fig_reg_int.write_html(regime_html_path, include_plotlyjs='cdn')

aw_final = (w_final / w0_nodes) ** n_iso_nodes
aw_final[aw_final < 1e-6] = 1e-6
aw_final[aw_final > 0.999999] = 0.999999
f_melt_final = f_melt_from_T(T_final)
assert np.all(np.isfinite(f_melt_final)), 'Non-finite f_melt_final values'

fig_prof, axes_prof = plt.subplots(5, 1, figsize=(8, 12), sharex=True)
axes_prof[0].plot(x_mm, T_final, linewidth=1.2)
axes_prof[0].set_ylabel('T (C)')
axes_prof[0].set_title('Final temperature profile')
axes_prof[0].grid(True, linestyle='--', linewidth=0.5)
axes_prof[1].plot(x_mm, w_final, linewidth=1.2)
axes_prof[1].set_ylabel('w')
axes_prof[1].set_title('Final moisture profile')
axes_prof[1].grid(True, linestyle='--', linewidth=0.5)
axes_prof[2].plot(x_mm, aw_final, linewidth=1.2)
axes_prof[2].set_ylabel('a_w')
axes_prof[2].set_title('Final water activity profile')
axes_prof[2].grid(True, linestyle='--', linewidth=0.5)
axes_prof[3].plot(x_mm, M_final, linewidth=1.2)
axes_prof[3].set_ylabel('M')
axes_prof[3].set_title('Final Maillard index profile')
axes_prof[3].grid(True, linestyle='--', linewidth=0.5)
axes_prof[4].plot(x_mm, f_melt_final, linewidth=1.2)
axes_prof[4].set_ylabel('f_melt')
axes_prof[4].set_xlabel('x (mm)')
axes_prof[4].set_title('Final melted fat fraction profile')
axes_prof[4].grid(True, linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.savefig(final_profiles_png_path, bbox_inches='tight')
plt.close(fig_prof)

fig_prof_int = make_subplots(rows=5, cols=1, shared_xaxes=True, subplot_titles=('T_final(x)', 'w_final(x)', 'a_w_final(x)', 'M_final(x)', 'f_melt_final(x)'))
fig_prof_int.add_trace(go.Scatter(x=x_mm, y=T_final, mode='lines', name='T_final'), row=1, col=1)
fig_prof_int.add_trace(go.Scatter(x=x_mm, y=w_final, mode='lines', name='w_final'), row=2, col=1)
fig_prof_int.add_trace(go.Scatter(x=x_mm, y=aw_final, mode='lines', name='a_w_final'), row=3, col=1)
fig_prof_int.add_trace(go.Scatter(x=x_mm, y=M_final, mode='lines', name='M_final'), row=4, col=1)
fig_prof_int.add_trace(go.Scatter(x=x_mm, y=f_melt_final, mode='lines', name='f_melt_final'), row=5, col=1)
fig_prof_int.update_layout(title='Final profiles', xaxis5_title='x (mm)')
fig_prof_int.write_html(final_profiles_html_path, include_plotlyjs='cdn')

fig_perf, ax_perf = plt.subplots(figsize=(8, 6))
scatter_c = ax_perf.scatter(C_norm_arr, J_norm_arr, c=R_norm_arr, cmap='viridis')
for i, cid in enumerate(condition_ids):
 ax_perf.text(C_norm_arr[i], J_norm_arr[i], cid)
ax_perf.set_xlabel('C_norm')
ax_perf.set_ylabel('J_norm')
ax_perf.set_title('Culinary performance space (C_norm vs J_norm)')
ax_perf.grid(True, linestyle='--', linewidth=0.5)
plt.colorbar(scatter_c, ax=ax_perf, label='R_norm')
plt.tight_layout()
plt.savefig(performance_space_png_path, bbox_inches='tight')
plt.close(fig_perf)

fig_perf_int = go.Figure()
fig_perf_int.add_trace(go.Scatter(x=C_norm_arr, y=J_norm_arr, mode='markers+text', text=condition_ids, textposition='top center', marker=dict(color=R_norm_arr, colorscale='Viridis', colorbar=dict(title='R_norm'))))
fig_perf_int.update_layout(title='Culinary performance space', xaxis_title='C_norm', yaxis_title='J_norm')
fig_perf_int.write_html(performance_space_html_path, include_plotlyjs='cdn')

mech_phase_matrix = np.zeros((3, n_phases), dtype=float)
for p in range(n_phases):
 Q_conv_p = Q_conv_phase[p]
 Q_lat_p = Q_latent_phase[p]
 idx_p = np.where(phase_idx_time == p)[0]
 i_start = int(idx_p[0])
 i_end = int(idx_p[-1])
 M_start = M_total_time[i_start]
 M_end = M_total_time[i_end]
 M_prod_p = max(M_end - M_start, 0.0)
 mech_phase_matrix[0, p] = Q_conv_p
 mech_phase_matrix[1, p] = Q_lat_p
 mech_phase_matrix[2, p] = M_prod_p
mech_phase_rel = np.zeros_like(mech_phase_matrix)
for p in range(n_phases):
 s = float(np.sum(np.abs(mech_phase_matrix[:, p])))
 if s > 0.0:
 mech_phase_rel[:, p] = mech_phase_matrix[:, p] / s
 else:
 mech_phase_rel[:, p] = 0.0
assert np.all(np.isfinite(mech_phase_rel)), 'Non-finite mech_phase_rel values'

mech_labels = ['Conduction', 'Evaporation', 'Reaction']
fig_mech, ax_mech = plt.subplots(figsize=(8, 6))
im_mech = ax_mech.imshow(mech_phase_rel, aspect='auto', origin='lower', cmap='viridis', vmin=-1.0, vmax=1.0)
ax_mech.set_xticks(np.arange(n_phases))
ax_mech.set_xticklabels(phase_labels)
ax_mech.set_yticks(np.arange(3))
ax_mech.set_yticklabels(mech_labels)
ax_mech.set_title('Mechanism-phase contribution map (relative)')
for i in range(3):
 for j in range(n_phases):
 val = mech_phase_rel[i, j]
 ax_mech.text(j, i, f'{val:.2f}', ha='center', va='center', color='white' if abs(val) < 0.5 else 'black', fontsize=7)
plt.colorbar(im_mech, ax=ax_mech, fraction=0.046, pad=0.04)
plt.tight_layout()
plt.savefig(mech_phase_map_png_path, bbox_inches='tight')
plt.close(fig_mech)

fig_mech_int = px.imshow(mech_phase_rel, x=phase_labels, y=mech_labels, color_continuous_scale='Viridis', origin='lower', labels={'color': 'Relative contribution'}, title='Mechanism-phase contribution map (relative)')
fig_mech_int.write_html(mech_phase_map_html_path, include_plotlyjs='cdn')

fig_rob, axes_rob = plt.subplots(3, 1, figsize=(10, 10), sharex=True)
x_pos = np.arange(len(condition_ids))
axes_rob[0].bar(x_pos, C_norm_arr, color='#4C72B0')
axes_rob[0].set_ylabel('C_norm')
axes_rob[0].set_title('Normalized crispness C_norm')
axes_rob[0].grid(axis='y', linestyle='--', linewidth=0.5)
axes_rob[1].bar(x_pos, J_norm_arr, color='#55A868')
axes_rob[1].set_ylabel('J_norm')
axes_rob[1].set_title('Normalized juiciness J_norm')
axes_rob[1].grid(axis='y', linestyle='--', linewidth=0.5)
axes_rob[2].bar(x_pos, R_norm_arr, color='#C44E52')
axes_rob[2].set_ylabel('R_norm')
axes_rob[2].set_title('Normalized doneness R_norm')
axes_rob[2].set_xticks(x_pos)
axes_rob[2].set_xticklabels(condition_ids, rotation=45, ha='right')
axes_rob[2].grid(axis='y', linestyle='--', linewidth=0.5)
plt.tight_layout()
plt.savefig(robustness_png_path, bbox_inches='tight')
plt.close(fig_rob)

fig_rob_int = make_subplots(rows=3, cols=1, shared_xaxes=True, subplot_titles=('C_norm', 'J_norm', 'R_norm'))
fig_rob_int.add_trace(go.Bar(x=condition_ids, y=C_norm_arr, name='C_norm'), row=1, col=1)
fig_rob_int.add_trace(go.Bar(x=condition_ids, y=J_norm_arr, name='J_norm'), row=2, col=1)
fig_rob_int.add_trace(go.Bar(x=condition_ids, y=R_norm_arr, name='R_norm'), row=3, col=1)
fig_rob_int.update_layout(title='Model robustness across scenarios', xaxis3_title='Condition')
fig_rob_int.write_html(robustness_html_path, include_plotlyjs='cdn')

assert np.all(np.isfinite(T_save)) and np.all(np.isfinite(w_save)) and np.all(np.isfinite(M_save)), 'Non-finite values in map arrays'
assert np.all(np.isfinite(T_surf_time)) and np.all(np.isfinite(T_core_time)) and np.all(np.isfinite(w_skin_time)) and np.all(np.isfinite(w_muscle_time)) and np.all(np.isfinite(M_skin_time)), 'Non-finite values in key time series'

progress = total_progress
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

result = {}
result['status'] = 'ok'
result['description'] = 'Re-executed Movement 3 with a refined, SFA/MUFA/PUFA-informed logistic fat melting model, recomputing juiciness and normalized culinary indices under Mario’s thermal schedule for Jhon Dallas suckling pig, consistent with Movements 1 and 2, and overwrote Movement 3 tables and figures accordingly.'
metrics = {}
metrics['C_gross_baseline'] = float(f'{C_gross_arr[0]:.2f}')
metrics['C_norm_baseline'] = float(f'{C_norm_arr[0]:.2f}')
metrics['J_gross_baseline'] = float(f'{J_gross_arr[0]:.2f}')
metrics['J_norm_baseline'] = float(f'{J_norm_arr[0]:.2f}')
metrics['R_raw_baseline'] = float(f'{R_raw_arr[0]:.2f}')
metrics['R_norm_baseline'] = float(f'{R_norm_arr[0]:.2f}')
metrics['R_aroma_norm'] = float(f'{R_aroma_norm_pig:.2f}')
metrics['f_SFA'] = float(f'{f_SFA:.2f}')
metrics['T_low_C'] = float(f'{T_low:.2f}')
metrics['T_high_C'] = float(f'{T_high:.2f}')
metrics['T50_C'] = float(f'{T50:.2f}')
metrics['a_sigmoid'] = float(f'{a_param:.4f}')
metrics['Fo_max_used'] = float(f'{Fo_max_used:.4f}')
metrics['dt_final_s'] = float(f'{dt:.4f}')
result['metrics'] = metrics
tables = {}
tables['Indices'] = Indices3_df.to_dict(orient='list')
tables['CrustMetrics'] = CrustMetrics_df.to_dict(orient='list')
result['tables'] = tables
image_files = [kinetics_png, drying_front_png, maps_png, regime_png, final_profiles_png, performance_space_png, mech_phase_map_png, robustness_png]
html_files = [kinetics_html, drying_front_html, maps_html, regime_html, final_profiles_html, performance_space_html, mech_phase_map_html, robustness_html]
result['images'] = image_files
result['caption'] = ['Multiscale temperatures (surface, core, interface)', 'Drying front evolution', 'Space-time maps of T, w, M', 'Physical regime diagram by phase', 'Final profiles T, w, a_w, M, f_melt', 'Culinary performance space C_norm vs J_norm', 'Mechanism-phase contribution map', 'Robustness of normalized metrics across scenarios']
files = [movement3_excel] html_files
result['files'] = files
result['excel_file'] = movement3_excel
result['image_files'] = image_files
result['html_files'] = html_files
summary = {}
summary['C_gross_baseline'] = float(f'{C_gross_arr[0]:.2f}')
summary['C_norm_baseline'] = float(f'{C_norm_arr[0]:.2f}')
summary['J_gross_baseline'] = float(f'{J_gross_arr[0]:.2f}')
summary['J_norm_baseline'] = float(f'{J_norm_arr[0]:.2f}')
summary['R_raw_baseline'] = float(f'{R_raw_arr[0]:.2f}')
summary['R_norm_baseline'] = float(f'{R_norm_arr[0]:.2f}')
summary['R_aroma_norm'] = float(f'{R_aroma_norm_pig:.2f}')
summary['f_SFA'] = float(f'{f_SFA:.2f}')
summary['T_low_C'] = float(f'{T_low:.2f}')
summary['T_high_C'] = float(f'{T_high:.2f}')
summary['T50_C'] = float(f'{T50:.2f}')
summary['a_sigmoid'] = float(f'{a_param:.4f}')
summary['Fo_max_used'] = float(f'{Fo_max_used:.4f}')
summary['dt_final_s'] = float(f'{dt:.4f}')
result['summary'] = summary