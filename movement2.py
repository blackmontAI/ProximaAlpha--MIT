import os
import math
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import requests
from send_message_backend import send_message_backend
from get_credentials import get_credentials

warnings.filterwarnings('default')

base_path = '/mnt/z/B011'
if not os.path.isdir(base_path):
 os.makedirs(base_path, exist_ok=True)

progress = 0.0
total_progress = 100.0
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

L_skin_pig_mm = 2.0
L_fat_pig_mm = 5.6
L_muscle_pig_mm = 10.0
L_skin_duck_mm = 1.5
L_fat_duck_mm = 3.0
L_muscle_duck_mm = 8.0
rho_skin = 1050.0
rho_muscle = 1050.0
rho_fat = 900.0
r_fat = 0.7
f_coll_skin = 0.30
f_coll_fat = 0.15
f_coll_muscle = 0.06
f_AA = 0.05
f_S = 0.04
m_glaze = 0.05
f_sugar = 0.6
T_min_C = 80.0
T_max_C = 260.0
n_T_points = 300

phase_setup_weight = 5.0
phase_usda_weight = 25.0
phase_pools_weight = 20.0
phase_kinetics_weight = 20.0
phase_tables_weight = 15.0
phase_figures_weight = 10.0
phase_sensitivity_weight = 5.0

progress = phase_setup_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

cfg = get_credentials()
api_key = cfg.get('nal_usda')
assert isinstance(api_key, str) and len(api_key) > 0, 'USDA API key (nal_usda) missing or invalid in credentials'
usda_base = 'https://api.nal.usda.gov/fdc/v1'

def usda_search_food(query, data_type_filter=None, page_size=5):
 params = {'api_key': api_key}
 url = usda_base '/foods/search'
 body = {'query': query, 'pageSize': int(page_size)}
 if data_type_filter is not None:
 body['dataType'] = data_type_filter
 response = requests.post(url, params=params, json=body, timeout=20)
 response.raise_for_status()
 data = response.json()
 if not isinstance(data, dict) or 'foods' not in data or not isinstance(data['foods'], list) or len(data['foods']) == 0:
 raise ValueError('USDA search returned no foods for query: ' str(query))
 return data['foods']

def usda_get_food(fdc_id):
 params = {'api_key': api_key}
 url = usda_base f'/food/{int(fdc_id)}'
 response = requests.get(url, params=params, timeout=20)
 response.raise_for_status()
 data = response.json()
 if not isinstance(data, dict) or 'foodNutrients' not in data or not isinstance(data['foodNutrients'], list):
 raise ValueError('USDA food detail missing foodNutrients for fdcId: ' str(fdc_id))
 return data

def extract_nutrients(food_record):
 nutrients = food_record.get('foodNutrients', [])
 if not isinstance(nutrients, list):
 raise ValueError('foodNutrients is not a list')
 water = None
 protein = None
 fat = None
 sfa = 0.0
 mufa = 0.0
 pufa = 0.0
 for n in nutrients:
 n_name = None
 n_val = None
 if isinstance(n, dict):
 if 'nutrientName' in n:
 n_name = n.get('nutrientName')
 n_val = n.get('value')
 elif 'nutrient' in n and isinstance(n['nutrient'], dict):
 n_name = n['nutrient'].get('name') or n['nutrient'].get('nutrientName')
 n_val = n.get('amount') if 'amount' in n else n.get('value')
 if n_name is None:
 continue
 if n_val is None:
 continue
 try:
 val = float(n_val)
 except Exception:
 continue
 name_lower = str(n_name).strip().lower()
 if name_lower == 'water' or name_lower.startswith('water '):
 water = val
 elif name_lower == 'protein' or name_lower.startswith('protein '):
 protein = val
 elif name_lower == 'total lipid (fat)' or name_lower.startswith('total lipid'):
 fat = val
 elif 'fatty acids, total saturated' in name_lower:
 sfa = val
 elif 'fatty acids, total monounsaturated' in name_lower:
 mufa = val
 elif 'fatty acids, total polyunsaturated' in name_lower:
 pufa = val
 if water is None or protein is None or fat is None:
 raise ValueError('Missing key proximate nutrients (water/protein/fat) in USDA record: ' str(food_record.get('description', 'unknown')))
 return {'water_g_per_100g': float(water), 'protein_g_per_100g': float(protein), 'fat_g_per_100g': float(fat), 'sfa_g_per_100g': float(sfa), 'mufa_g_per_100g': float(mufa), 'pufa_g_per_100g': float(pufa)}

def proximate_to_fractions(nut):
 water = float(nut['water_g_per_100g'])
 protein = float(nut['protein_g_per_100g'])
 fat = float(nut['fat_g_per_100g'])
 total = water protein fat
 if total <= 0.0:
 raise ValueError('Total of water protein fat is non-positive')
 w_frac = water / 100.0
 p_frac = protein / 100.0
 f_frac = fat / 100.0
 return w_frac, p_frac, f_frac

def lipid_profile_fractions(nut):
 fat = float(nut['fat_g_per_100g'])
 if fat <= 0.0:
 return 0.0, 0.0, 0.0
 sfa = float(nut['sfa_g_per_100g'])
 mufa = float(nut['mufa_g_per_100g'])
 pufa = float(nut['pufa_g_per_100g'])
 sfa_frac = max(sfa / fat, 0.0)
 mufa_frac = max(mufa / fat, 0.0)
 pufa_frac = max(pufa / fat, 0.0)
 return sfa_frac, mufa_frac, pufa_frac

food_queries = {'pig_skin': ['pork, skin, raw'], 'pig_fat': ['pork, fresh, backfat, raw'], 'pig_muscle': ['pork, loin, raw, lean only'], 'duck_skin': ['duck, domesticated, skin, with subcutaneous fat, raw', 'duck, skin, raw', 'duck, domesticated, meat and skin, raw'], 'duck_muscle': ['duck, domesticated, meat only, raw', 'duck, meat only, raw'], 'duck_global': ['duck, domesticated, meat and skin, raw', 'duck, meat and skin, raw']}

progress_phase_start = progress
n_food_tasks = len(food_queries)
food_index = 0
usda_nutrient_data = {}

for key, query_list in food_queries.items():
 found = False
 last_error = None
 for q in query_list:
 try:
 foods = usda_search_food(q, data_type_filter=None, page_size=5)
 best = None
 for fitem in foods:
 desc = fitem.get('description', '')
 if isinstance(desc, str) and 'raw' in desc.lower():
 best = fitem
 break
 if best is None:
 best = foods[0]
 fdc_id = best.get('fdcId')
 if fdc_id is None:
 continue
 food_record = usda_get_food(fdc_id)
 nut = extract_nutrients(food_record)
 usda_nutrient_data[key] = {'query_used': q, 'fdcId': int(fdc_id), 'description': food_record.get('description', ''), 'nutrients': nut}
 found = True
 break
 except Exception as e:
 last_error = e
 continue
 if not found:
 raise ValueError('Unable to retrieve USDA composition for key ' str(key) ' last error: ' str(last_error))
 food_index = 1
 frac = float(food_index) / float(n_food_tasks)
 progress = progress_phase_start phase_usda_weight * frac
 msgLog = f'Executed: {progress:.1f}%'
 send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

progress = phase_setup_weight phase_usda_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

L_skin_pig = L_skin_pig_mm * 1e-3
L_fat_pig = L_fat_pig_mm * 1e-3
L_muscle_pig = L_muscle_pig_mm * 1e-3
L_skin_duck = L_skin_duck_mm * 1e-3
L_fat_duck = L_fat_duck_mm * 1e-3
L_muscle_duck = L_muscle_duck_mm * 1e-3

L_layers_pig = np.array([L_skin_pig, L_fat_pig, L_muscle_pig], dtype=float)
L_layers_duck = np.array([L_skin_duck, L_fat_duck, L_muscle_duck], dtype=float)
rho_layers = np.array([rho_skin, rho_fat, rho_muscle], dtype=float)

assert np.all(L_layers_pig > 0.0) and np.all(L_layers_duck > 0.0), 'Layer thickness must be positive'
assert np.all(rho_layers > 0.0), 'Layer densities must be positive'

m_layers_pig_geom = rho_layers * L_layers_pig
m_layers_duck_geom = rho_layers * L_layers_duck

layers_order = ['skin', 'fat', 'muscle']

w_pig_layers = np.zeros(3, dtype=float)
p_pig_layers = np.zeros(3, dtype=float)
f_pig_layers = np.zeros(3, dtype=float)
for i, lname in enumerate(layers_order):
 if lname == 'skin':
 nut = usda_nutrient_data['pig_skin']['nutrients']
 elif lname == 'fat':
 nut = usda_nutrient_data['pig_fat']['nutrients']
 else:
 nut = usda_nutrient_data['pig_muscle']['nutrients']
 w_layer, p_layer, f_layer = proximate_to_fractions(nut)
 w_pig_layers[i] = w_layer
 p_pig_layers[i] = p_layer
 f_pig_layers[i] = f_layer

w_duck_layers = np.zeros(3, dtype=float)
p_duck_layers = np.zeros(3, dtype=float)
f_duck_layers = np.zeros(3, dtype=float)

duck_skin_nut = usda_nutrient_data['duck_skin']['nutrients']
duck_muscle_nut = usda_nutrient_data['duck_muscle']['nutrients']
duck_global_nut = usda_nutrient_data['duck_global']['nutrients']

for i, lname in enumerate(layers_order):
 if lname == 'skin':
 nut = duck_skin_nut
 elif lname == 'fat':
 nut = duck_global_nut
 else:
 nut = duck_muscle_nut
 w_layer, p_layer, f_layer = proximate_to_fractions(nut)
 w_duck_layers[i] = w_layer
 p_duck_layers[i] = p_layer
 f_duck_layers[i] = f_layer

m_pig_base = m_layers_pig_geom.copy()
m_duck_base = m_layers_duck_geom.copy()

W_pig_base = m_pig_base * w_pig_layers
P_pig_base = m_pig_base * p_pig_layers
F_pig_base = m_pig_base * f_pig_layers

W_duck_base = m_duck_base * w_duck_layers
P_duck_base = m_duck_base * p_duck_layers
F_duck_base = m_duck_base * f_duck_layers

W_pig_JD = W_pig_base.copy()
P_pig_JD = P_pig_base.copy()
F_pig_JD = F_pig_base.copy()
m_pig_JD = m_pig_base.copy()

for i, lname in enumerate(layers_order):
 if lname in ['skin', 'fat']:
 F_pig_JD[i] = r_fat * F_pig_base[i]
 W_pig_JD[i] = W_pig_base[i]
 P_pig_JD[i] = P_pig_base[i]
 m_pig_JD[i] = W_pig_JD[i] + P_pig_JD[i] + F_pig_JD[i]
 if m_pig_JD[i] <= 0.0:
 raise ValueError('Adjusted mass m_pig_JD is non-positive for layer ' + lname)

w_pig_JD = W_pig_JD / m_pig_JD
p_pig_JD = P_pig_JD / m_pig_JD
f_pig_JD = F_pig_JD / m_pig_JD

m_pig_final = m_pig_JD.copy()
m_duck_final = m_duck_base.copy()

W_pig_layers = m_pig_final * w_pig_JD
P_pig_layers = m_pig_final * p_pig_JD
F_pig_layers = m_pig_final * f_pig_JD

W_duck_layers = m_duck_final * w_duck_layers
P_duck_layers = m_duck_final * p_duck_layers
F_duck_layers = m_duck_final * f_duck_layers

f_coll_array = np.array([f_coll_skin, f_coll_fat, f_coll_muscle], dtype=float)

Coll_pig_layers = P_pig_layers * f_coll_array
P_nc_pig_layers = P_pig_layers - Coll_pig_layers

Coll_duck_layers = P_duck_layers * f_coll_array
P_nc_duck_layers = P_duck_layers - Coll_duck_layers

W_tot_pig = float(np.sum(W_pig_layers))
P_tot_pig = float(np.sum(P_pig_layers))
F_tot_pig = float(np.sum(F_pig_layers))

W_tot_duck = float(np.sum(W_duck_layers))
P_tot_duck = float(np.sum(P_duck_layers))
F_tot_duck = float(np.sum(F_duck_layers))

assert W_tot_pig > 0.0 and P_tot_pig > 0.0 and F_tot_pig > 0.0, 'Pig pools must be positive'
assert W_tot_duck > 0.0 and P_tot_duck > 0.0 and F_tot_duck > 0.0, 'Duck pools must be positive'

sfa_pig, mufa_pig, pufa_pig = lipid_profile_fractions(usda_nutrient_data['pig_fat']['nutrients'])
sfa_duck, mufa_duck, pufa_duck = lipid_profile_fractions(usda_nutrient_data['duck_global']['nutrients'])

f_unsat_pig = mufa_pig + pufa_pig
f_unsat_duck = mufa_duck + pufa_duck

F_unsat_pig_layers = f_unsat_pig * F_pig_layers
F_unsat_duck_layers = f_unsat_duck * F_duck_layers

AA_pig_layers = f_AA * P_pig_layers
AA_S_pig_layers = f_S * P_pig_layers

AA_duck_layers = f_AA * P_duck_layers
AA_S_duck_layers = f_S * P_duck_layers

S_glaze = f_sugar * m_glaze

Q_M_pig = float(AA_pig_layers[0] + P_nc_pig_layers[0] + AA_pig_layers[2] + P_nc_pig_layers[2])
Q_L_pig = float(F_unsat_pig_layers[0] + F_unsat_pig_layers[1])
Q_G_pig = float(S_glaze)
Q_S_pig = float(AA_S_pig_layers[0] + AA_S_pig_layers[2])

Q_M_duck = float(AA_duck_layers[0] + P_nc_duck_layers[0] + AA_duck_layers[2] + P_nc_duck_layers[2])
Q_L_duck = float(F_unsat_duck_layers[0] + F_unsat_duck_layers[1])
Q_G_duck = float(S_glaze)
Q_S_duck = float(AA_S_duck_layers[0] + AA_S_duck_layers[2])

Q_pig = np.array([Q_M_pig, Q_L_pig, Q_G_pig, Q_S_pig], dtype=float)
Q_duck = np.array([Q_M_duck, Q_L_duck, Q_G_duck, Q_S_duck], dtype=float)

assert np.all(Q_pig > 0.0) and np.all(Q_duck > 0.0), 'All Q_k must be positive for both species'

sum_Q_pig = float(np.sum(Q_pig))
sum_Q_duck = float(np.sum(Q_duck))

assert sum_Q_pig > 0.0 and sum_Q_duck > 0.0, 'Sum of Q_k must be positive for both species'

I0_pig = Q_pig / sum_Q_pig
I0_duck = Q_duck / sum_Q_duck

channels = ['M', 'L', 'G', 'S']

E_k_kJ = np.array([100.0, 80.0, 120.0, 90.0], dtype=float)
E_k_J = E_k_kJ * 1000.0
A_pre = np.ones(4, dtype=float)
A_pre_rel = A_pre / float(np.sum(A_pre))
R_gas = 8.314

T_C_grid = np.linspace(T_min_C, T_max_C, int(n_T_points))
T_K_grid = T_C_grid + 273.15

r_tilde = np.zeros((4, int(n_T_points)), dtype=float)
for k in range(4):
 r_k = np.exp(-E_k_J[k] / (R_gas * T_K_grid))
 max_r = float(np.max(r_k))
 if max_r <= 0.0:
 raise ValueError('Non-positive maximum kinetic rate for channel index ' + str(k))
 r_tilde[k, :] = r_k / max_r

beta_k = np.array([3.0, 1.0, 0.5, 2.0], dtype=float)
gamma_k = np.array([1.0, 3.0, 0.5, 2.0], dtype=float)

a_w_grid = np.linspace(0.01, 0.99, 200)
g_aw = np.zeros((4, a_w_grid.size), dtype=float)
for k in range(4):
 g_aw[k, :] = np.power(a_w_grid, beta_k[k]) * np.power(1.0 - a_w_grid, gamma_k[k])

durations = [30, 60, 25, 25, 5, 5]
temps_phase_C = [80.0, 90.0, 130.0, 230.0, 240.0, 250.0]
aw_phase = [0.95, 0.85, 0.70, 0.60, 0.50, 0.40]
phase_names = ['F3', 'F4', 'F5', 'F6', 'F7', 'F8']

total_minutes = int(sum(durations))
t_minutes = np.arange(total_minutes, dtype=float)
T_surf_C = np.empty(total_minutes, dtype=float)
aw_series = np.empty(total_minutes, dtype=float)
phase_boundaries = [0]
idx_start = 0
for d in durations:
 idx_end = idx_start + d
 phase_boundaries.append(idx_end)
 idx_start = idx_end

idx_start = 0
for i in range(len(durations)):
 d = durations[i]
 idx_end = idx_start + d
 T_surf_C[idx_start:idx_end] = temps_phase_C[i]
 aw_series[idx_start:idx_end] = aw_phase[i]
 idx_start = idx_end

assert T_surf_C.shape[0] == total_minutes and aw_series.shape[0] == total_minutes, 'Schedule arrays must match total duration'

r_tilde_sched = np.zeros((4, total_minutes), dtype=float)
for k in range(4):
 r_tilde_sched[k, :] = np.interp(T_surf_C, T_C_grid, r_tilde[k, :])

g_mod_sched = np.zeros((4, total_minutes), dtype=float)
for k in range(4):
 g_mod_sched[k, :] = np.power(aw_series, beta_k[k]) * np.power(1.0 - aw_series, gamma_k[k])

assert np.all(np.isfinite(r_tilde_sched)) and np.all(np.isfinite(g_mod_sched)), 'Non-finite values in kinetic or modulation schedules'

I_mat_pig = np.zeros((total_minutes, 4), dtype=float)
I_mat_duck = np.zeros((total_minutes, 4), dtype=float)
for k in range(4):
 I_mat_pig[:, k] = I0_pig[k] * r_tilde_sched[k, :] * g_mod_sched[k, :]
 I_mat_duck[:, k] = I0_duck[k] * r_tilde_sched[k, :] * g_mod_sched[k, :]

assert np.all(I_mat_pig >= 0.0) and np.all(I_mat_duck >= 0.0), 'Intensities must be non-negative'
assert np.all(np.isfinite(I_mat_pig)) and np.all(np.isfinite(I_mat_duck)), 'Non-finite intensities detected'

A_int_pig = np.sum(0.5 * (I_mat_pig[:-1, :] + I_mat_pig[1:, :]), axis=0) * 1.0
A_int_duck = np.sum(0.5 * (I_mat_duck[:-1, :] + I_mat_duck[1:, :]), axis=0) * 1.0

assert np.all(A_int_pig >= 0.0) and np.all(A_int_duck >= 0.0), 'Integrated intensities must be non-negative'
sum_A_pig = float(np.sum(A_int_pig))
sum_A_duck = float(np.sum(A_int_duck))
assert sum_A_pig > 0.0 and sum_A_duck > 0.0, 'Sum of integrated intensities must be positive'

A_rel_pig = A_int_pig / sum_A_pig
A_rel_duck = A_int_duck / sum_A_duck

assert np.all(np.isfinite(A_rel_pig)) and np.all(np.isfinite(A_rel_duck)), 'Non-finite relative integrated intensities detected'

R_aroma_norm_pig = float(np.linalg.norm(A_rel_pig))
R_aroma_norm_duck = float(np.linalg.norm(A_rel_duck))

I_all = np.vstack([I_mat_pig, I_mat_duck])
X_all = I_all - np.mean(I_all, axis=0, keepdims=True)
cov_all = (X_all.T @ X_all) / float(X_all.shape[0] - 1)
eig_vals, eig_vecs = np.linalg.eigh(cov_all)
idx_max = int(np.argmax(eig_vals))
u1 = eig_vecs[:, idx_max]
u1_norm = float(np.linalg.norm(u1))
if u1_norm <= 0.0:
 raise ValueError('PCA eigenvector norm is non-positive')
u1 = u1 / u1_norm

X_pig = I_mat_pig - np.mean(I_all, axis=0, keepdims=True)
X_duck = I_mat_duck - np.mean(I_all, axis=0, keepdims=True)

PC1_pig = X_pig @ u1
PC1_duck = X_duck @ u1

assert np.all(np.isfinite(PC1_pig)) and np.all(np.isfinite(PC1_duck)), 'Non-finite PCA scores detected'

R_aroma_PCA_pig = float(np.sum(0.5 * (PC1_pig[:-1] PC1_pig[1:])) * 1.0)
R_aroma_PCA_duck = float(np.sum(0.5 * (PC1_duck[:-1] PC1_duck[1:])) * 1.0)

d_aroma = float(np.linalg.norm(A_rel_pig - A_rel_duck))
S_aroma = float(math.exp(-d_aroma))

progress = phase_setup_weight phase_usda_weight phase_pools_weight phase_kinetics_weight * 0.5
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

layers_with_glaze = ['skin', 'fat', 'muscle', 'glaze']

pools_records = []

for species, P_layers, Coll_layers, P_nc_layers, AA_layers, AA_S_layers, F_layers, F_unsat_layers, W_layers in [('Pig', P_pig_layers, Coll_pig_layers, P_nc_pig_layers, AA_pig_layers, AA_S_pig_layers, F_pig_layers, F_unsat_pig_layers, W_pig_layers), ('Duck', P_duck_layers, Coll_duck_layers, P_nc_duck_layers, AA_duck_layers, AA_S_duck_layers, F_duck_layers, F_unsat_duck_layers, W_duck_layers)]:
 for i, lname in enumerate(layers_with_glaze):
 if lname == 'glaze':
 P_l = 0.0
 C_l = 0.0
 P_nc_l = 0.0
 AA_l = 0.0
 AA_S_l = 0.0
 F_l = 0.0
 F_unsat_l = 0.0
 W_l = 0.0
 S_glaze_l = S_glaze
 else:
 idx = layers_with_glaze.index(lname)
 idx = idx if idx < 3 else 0
 P_l = float(P_layers[idx])
 C_l = float(Coll_layers[idx])
 P_nc_l = float(P_nc_layers[idx])
 AA_l = float(AA_layers[idx])
 AA_S_l = float(AA_S_layers[idx])
 F_l = float(F_layers[idx])
 F_unsat_l = float(F_unsat_layers[idx])
 W_l = float(W_layers[idx])
 S_glaze_l = 0.0
 pools_records.append({'Species': species, 'Layer': lname, 'P_l': P_l, 'C_l': C_l, 'P_nc_l': P_nc_l, 'AA_l': AA_l, 'AA_S_l': AA_S_l, 'F_l': F_l, 'F_unsat_l': F_unsat_l, 'W_l': W_l, 'S_glaze_l': S_glaze_l})

Pools_df = pd.DataFrame(pools_records)
assert not Pools_df.isna().any().any(), 'Pools_df contains NaN values'

kinetics_records = []
for i, ch in enumerate(channels):
 if ch == 'M':
 process = 'Maillard / caramelizacion'
 elif ch == 'L':
 process = 'Oxidacion lipidos insaturados'
 elif ch == 'G':
 process = 'Glaseado azucares'
 else:
 process = 'Compuestos sulfurados'
 T_range_str = f'{T_min_C:.0f}-{T_max_C:.0f}'
 kinetics_records.append({'Canal': ch, 'Proceso': process, 'E_k_kJmol': float(E_k_kJ[i]), 'A_k_rel': float(A_pre_rel[i]), 'beta_k': float(beta_k[i]), 'gamma_k': float(gamma_k[i]), 'T_range_C': T_range_str, 'Fuente': 'Movimiento2_model'})

Kinetics_df = pd.DataFrame(kinetics_records)
assert not Kinetics_df.isna().any().any(), 'Kinetics_df contains NaN values'

Indices_records = []
Indices_records.append({'Species': 'Pig', 'A_M': float(A_int_pig[0]), 'A_L': float(A_int_pig[1]), 'A_G': float(A_int_pig[2]), 'A_S': float(A_int_pig[3]), 'A_M_rel': float(A_rel_pig[0]), 'A_L_rel': float(A_rel_pig[1]), 'A_G_rel': float(A_rel_pig[2]), 'A_S_rel': float(A_rel_pig[3]), 'R_aroma_norm': float(R_aroma_norm_pig), 'R_aroma_PCA': float(R_aroma_PCA_pig)})
Indices_records.append({'Species': 'Duck', 'A_M': float(A_int_duck[0]), 'A_L': float(A_int_duck[1]), 'A_G': float(A_int_duck[2]), 'A_S': float(A_int_duck[3]), 'A_M_rel': float(A_rel_duck[0]), 'A_L_rel': float(A_rel_duck[1]), 'A_G_rel': float(A_rel_duck[2]), 'A_S_rel': float(A_rel_duck[3]), 'R_aroma_norm': float(R_aroma_norm_duck), 'R_aroma_PCA': float(R_aroma_PCA_duck)})
Indices_df = pd.DataFrame(Indices_records)
assert not Indices_df.isna().any().any(), 'Indices_df contains NaN values'

AromaDist_records = [{'Species_ref': 'Pig', 'Species_other': 'Duck', 'd_aroma': float(d_aroma), 'S_aroma': float(S_aroma)}]
AromaDist_df = pd.DataFrame(AromaDist_records)
assert not AromaDist_df.isna().any().any(), 'AromaDist_df contains NaN values'

movement2_excel = 'movement2_tables.xlsx'
movement2_excel_path = os.path.join(base_path, movement2_excel)
with pd.ExcelWriter(movement2_excel_path) as writer:
 Pools_df.to_excel(writer, sheet_name='Pools', index=False)
 Kinetics_df.to_excel(writer, sheet_name='Kinetics', index=False)
 Indices_df.to_excel(writer, sheet_name='Indices', index=False)
 AromaDist_df.to_excel(writer, sheet_name='AromaDistances', index=False)

progress = phase_setup_weight phase_usda_weight phase_pools_weight phase_kinetics_weight phase_tables_weight * 0.5
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

for arr in [I_mat_pig, I_mat_duck, A_rel_pig, A_rel_duck]:
 if not np.all(np.isfinite(arr)):
 raise ValueError('Non-finite values found in key aromatic arrays')

kinetics_png = 'movement2_kinetics.png'
kinetics_html = 'movement2_kinetics.html'
awmod_png = 'movement2_aw_modulation.png'
awmod_html = 'movement2_aw_modulation.html'
Ikt_pig_png = 'movement2_Ikt_pig.png'
Ikt_pig_html = 'movement2_Ikt_pig.html'
Arel_bars_png = 'movement2_Arel_bars.png'
Arel_bars_html = 'movement2_Arel_bars.html'
origin_family_png = 'movement2_origin_family_pig.png'
origin_family_html = 'movement2_origin_family_pig.html'
sensitivity_png = 'movement2_sensitivity_pig.png'
sensitivity_html = 'movement2_sensitivity_pig.html'

kinetics_png_path = os.path.join(base_path, kinetics_png)
kinetics_html_path = os.path.join(base_path, kinetics_html)
awmod_png_path = os.path.join(base_path, awmod_png)
awmod_html_path = os.path.join(base_path, awmod_html)
Ikt_pig_png_path = os.path.join(base_path, Ikt_pig_png)
Ikt_pig_html_path = os.path.join(base_path, Ikt_pig_html)
Arel_bars_png_path = os.path.join(base_path, Arel_bars_png)
Arel_bars_html_path = os.path.join(base_path, Arel_bars_html)
origin_family_png_path = os.path.join(base_path, origin_family_png)
origin_family_html_path = os.path.join(base_path, origin_family_html)
sensitivity_png_path = os.path.join(base_path, sensitivity_png)
sensitivity_html_path = os.path.join(base_path, sensitivity_html)

fig_k, ax_k = plt.subplots(figsize=(8, 6))
for i, ch in enumerate(channels):
 ax_k.plot(T_C_grid, r_tilde[i, :], label='Canal ' ch, linewidth=1.2)
ax_k.set_xlabel('T (C)')
ax_k.set_ylabel('r_tilde_k(T)')
ax_k.set_title('Funciones cineticas normalizadas r_tilde_k(T)')
ax_k.grid(True, linestyle='--', linewidth=0.5)
ax_k.legend(loc='best')
plt.tight_layout()
plt.savefig(kinetics_png_path, bbox_inches='tight')
plt.close(fig_k)

kinetics_plot_df = pd.DataFrame({'T_C': T_C_grid, 'M': r_tilde[0, :], 'L': r_tilde[1, :], 'G': r_tilde[2, :], 'S': r_tilde[3, :]})
fig_k_int = go.Figure()
for ch in channels:
 fig_k_int.add_trace(go.Scatter(x=kinetics_plot_df['T_C'], y=kinetics_plot_df[ch], mode='lines', name='Canal ' ch))
fig_k_int.update_layout(title='Funciones cineticas normalizadas r_tilde_k(T)', xaxis_title='T (C)', yaxis_title='r_tilde_k(T)')
fig_k_int.write_html(kinetics_html_path, include_plotlyjs='cdn')

fig_g, ax_g = plt.subplots(figsize=(8, 6))
for i, ch in enumerate(channels):
 ax_g.plot(a_w_grid, g_aw[i, :], label='Canal ' ch, linewidth=1.2)
ax_g.set_xlabel('a_w')
ax_g.set_ylabel('g_k(a_w)')
ax_g.set_title('Funciones de modulacion de actividad de agua g_k(a_w)')
ax_g.grid(True, linestyle='--', linewidth=0.5)
ax_g.legend(loc='best')
plt.tight_layout()
plt.savefig(awmod_png_path, bbox_inches='tight')
plt.close(fig_g)

awmod_plot_df = pd.DataFrame({'a_w': a_w_grid, 'M': g_aw[0, :], 'L': g_aw[1, :], 'G': g_aw[2, :], 'S': g_aw[3, :]})
fig_g_int = go.Figure()
for ch in channels:
 fig_g_int.add_trace(go.Scatter(x=awmod_plot_df['a_w'], y=awmod_plot_df[ch], mode='lines', name='Canal ' ch))
fig_g_int.update_layout(title='Funciones de modulacion g_k(a_w)', xaxis_title='a_w', yaxis_title='g_k(a_w)')
fig_g_int.write_html(awmod_html_path, include_plotlyjs='cdn')

fig_I, ax_I = plt.subplots(figsize=(10, 6))
for i, ch in enumerate(channels):
 ax_I.plot(t_minutes, I_mat_pig[:, i], label='Canal ' ch, linewidth=1.2)
for b in phase_boundaries:
 ax_I.axvline(b, color='gray', linestyle='--', linewidth=0.7)
ax_I.set_xlabel('t (min)')
ax_I.set_ylabel('I_k(t) Pig')
ax_I.set_title('Intensidades aromaticas canalizadas I_k(t) para Pig')
ax_I.grid(True, linestyle='--', linewidth=0.5)
ax_I.legend(loc='best')
plt.tight_layout()
plt.savefig(Ikt_pig_png_path, bbox_inches='tight')
plt.close(fig_I)

Ikt_plot_df = pd.DataFrame({'t_min': t_minutes, 'M': I_mat_pig[:, 0], 'L': I_mat_pig[:, 1], 'G': I_mat_pig[:, 2], 'S': I_mat_pig[:, 3]})
fig_I_int = go.Figure()
for ch in channels:
 fig_I_int.add_trace(go.Scatter(x=Ikt_plot_df['t_min'], y=Ikt_plot_df[ch], mode='lines', name='Canal ' ch))
for b, name in zip(phase_boundaries, ['F3', 'F4', 'F5', 'F6', 'F7', 'F8', '']):
 fig_I_int.add_vline(x=b, line_width=0.7, line_dash='dash', line_color='gray')
fig_I_int.update_layout(title='Intensidades aromaticas I_k(t) Pig', xaxis_title='t (min)', yaxis_title='I_k(t)')
fig_I_int.write_html(Ikt_pig_html_path, include_plotlyjs='cdn')

Arel_bars_df = pd.DataFrame({'Canal': channels, 'Pig': A_rel_pig, 'Duck': A_rel_duck})
x_pos = np.arange(len(channels))
width = 0.35
fig_A, ax_A = plt.subplots(figsize=(8, 6))
ax_A.bar(x_pos - width / 2.0, A_rel_pig, width, label='Pig', color='#4C72B0')
ax_A.bar(x_pos width / 2.0, A_rel_duck, width, label='Duck', color='#55A868')
ax_A.set_xticks(x_pos)
ax_A.set_xticklabels(channels)
ax_A.set_ylabel('A_k_rel')
ax_A.set_title('Composicion aromatica relativa A_k_rel por canal y especie')
ax_A.grid(axis='y', linestyle='--', linewidth=0.5)
ax_A.legend(loc='best')
plt.tight_layout()
plt.savefig(Arel_bars_png_path, bbox_inches='tight')
plt.close(fig_A)

fig_A_int = go.Figure()
fig_A_int.add_trace(go.Bar(x=channels, y=A_rel_pig, name='Pig'))
fig_A_int.add_trace(go.Bar(x=channels, y=A_rel_duck, name='Duck'))
fig_A_int.update_layout(barmode='group', title='Composicion aromatica relativa A_k_rel', xaxis_title='Canal', yaxis_title='A_k_rel')
fig_A_int.write_html(Arel_bars_html_path, include_plotlyjs='cdn')

Q_layers_pig_M = np.array([AA_pig_layers[0] P_nc_pig_layers[0], 0.0, AA_pig_layers[2] P_nc_pig_layers[2], 0.0], dtype=float)
Q_layers_pig_L = np.array([F_unsat_pig_layers[0], F_unsat_pig_layers[1], 0.0, 0.0], dtype=float)
Q_layers_pig_G = np.array([0.0, 0.0, 0.0, S_glaze], dtype=float)
Q_layers_pig_S = np.array([AA_S_pig_layers[0], 0.0, AA_S_pig_layers[2], 0.0], dtype=float)

Q_k_pig_vec = np.array([np.sum(Q_layers_pig_M), np.sum(Q_layers_pig_L), np.sum(Q_layers_pig_G), np.sum(Q_layers_pig_S)], dtype=float)
assert np.all(Q_k_pig_vec > 0.0), 'Q_k_pig_vec must be positive'

A_layer_channel = np.zeros((4, 4), dtype=float)
Q_layers_all = [Q_layers_pig_M, Q_layers_pig_L, Q_layers_pig_G, Q_layers_pig_S]
for k in range(4):
 Qk = float(Q_k_pig_vec[k])
 Ak = float(A_int_pig[k])
 if Qk > 0.0 and Ak > 0.0:
 A_layer_channel[:, k] = (Q_layers_all[k] / Qk) * Ak
 else:
 A_layer_channel[:, k] = 0.0

A_layer_channel_rel = np.zeros_like(A_layer_channel)
for k in range(4):
 Ak = float(A_int_pig[k])
 if Ak > 0.0:
 A_layer_channel_rel[:, k] = A_layer_channel[:, k] / Ak
 else:
 A_layer_channel_rel[:, k] = 0.0

assert np.all(np.isfinite(A_layer_channel_rel)), 'Non-finite origin-family matrix values'

fig_OF, ax_OF = plt.subplots(figsize=(8, 6))
im = ax_OF.imshow(A_layer_channel_rel, aspect='auto', cmap='viridis', vmin=0.0, vmax=1.0)
ax_OF.set_xticks(np.arange(4))
ax_OF.set_xticklabels(channels)
ax_OF.set_yticks(np.arange(4))
ax_OF.set_yticklabels(layers_with_glaze)
ax_OF.set_title('Mapa origen-familia Pig (contribuciones relativas por capa y canal)')
for i in range(4):
 for j in range(4):
 val = A_layer_channel_rel[i, j]
 ax_OF.text(j, i, f'{val:.2f}', ha='center', va='center', color='white' if val < 0.5 else 'black', fontsize=7)
plt.colorbar(im, ax=ax_OF, fraction=0.046, pad=0.04)
plt.tight_layout()
plt.savefig(origin_family_png_path, bbox_inches='tight')
plt.close(fig_OF)

origin_family_df = pd.DataFrame(A_layer_channel_rel, index=layers_with_glaze, columns=channels)
fig_OF_int = px.imshow(origin_family_df.values, x=channels, y=layers_with_glaze, color_continuous_scale='Viridis', zmin=0.0, zmax=1.0, labels={'color': 'Contribucion relativa'}, title='Mapa origen-familia Pig (relativo)')
fig_OF_int.write_html(origin_family_html_path, include_plotlyjs='cdn')

progress = phase_setup_weight phase_usda_weight phase_pools_weight phase_kinetics_weight phase_tables_weight phase_figures_weight * 0.5
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

baseline_R = R_aroma_norm_pig
baseline_Arel = A_rel_pig.copy()
baseline_Q_pig = Q_pig.copy()
baseline_aw_series = aw_series.copy()
baseline_E_k_J = E_k_J.copy()
baseline_I0_pig = I0_pig.copy()

sensitivity_results_R = []
sensitivity_results_Arel = []
sensitivity_names = []

def recompute_pig_intensities(E_k_J_sens, Q_pig_sens, aw_series_sens):
 sum_Q_sens = float(np.sum(Q_pig_sens))
 assert sum_Q_sens > 0.0, 'Sum Q_k_sens must be positive'
 I0_sens = Q_pig_sens / sum_Q_sens
 r_tilde_sens = np.zeros_like(r_tilde_sched)
 for k in range(4):
 r_k = np.exp(-E_k_J_sens[k] / (R_gas * T_K_grid))
 max_r = float(np.max(r_k))
 if max_r <= 0.0:
 raise ValueError('Non-positive max r_k in sensitivity')
 r_tilde_k_grid = r_k / max_r
 r_tilde_sens[k, :] = np.interp(T_surf_C, T_C_grid, r_tilde_k_grid)
 g_mod_sens = np.zeros_like(g_mod_sched)
 for k in range(4):
 g_mod_sens[k, :] = np.power(aw_series_sens, beta_k[k]) * np.power(1.0 - aw_series_sens, gamma_k[k])
 I_mat_sens = np.zeros_like(I_mat_pig)
 for k in range(4):
 I_mat_sens[:, k] = I0_sens[k] * r_tilde_sens[k, :] * g_mod_sens[k, :]
 assert np.all(np.isfinite(I_mat_sens)), 'Non-finite I_mat_sens'
 A_int_sens = np.sum(0.5 * (I_mat_sens[:-1, :] I_mat_sens[1:, :]), axis=0) * 1.0
 sum_A_sens = float(np.sum(A_int_sens))
 assert sum_A_sens > 0.0, 'Sum A_int_sens must be positive'
 A_rel_sens = A_int_sens / sum_A_sens
 R_sens = float(np.linalg.norm(A_rel_sens))
 return R_sens, A_rel_sens

E_M_plus = baseline_E_k_J.copy()
E_M_plus[0] = baseline_E_k_J[0] * 1.10
R_sens, A_rel_sens = recompute_pig_intensities(E_M_plus, baseline_Q_pig, baseline_aw_series)
sensitivity_names.append('E_M 10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

E_M_minus = baseline_E_k_J.copy()
E_M_minus[0] = baseline_E_k_J[0] * 0.90
R_sens, A_rel_sens = recompute_pig_intensities(E_M_minus, baseline_Q_pig, baseline_aw_series)
sensitivity_names.append('E_M-10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

Q_pig_unsat_plus = baseline_Q_pig.copy()
Q_pig_unsat_plus[1] = baseline_Q_pig[1] * 1.10
R_sens, A_rel_sens = recompute_pig_intensities(baseline_E_k_J, Q_pig_unsat_plus, baseline_aw_series)
sensitivity_names.append('f_unsat 10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

Q_pig_unsat_minus = baseline_Q_pig.copy()
Q_pig_unsat_minus[1] = baseline_Q_pig[1] * 0.90
R_sens, A_rel_sens = recompute_pig_intensities(baseline_E_k_J, Q_pig_unsat_minus, baseline_aw_series)
sensitivity_names.append('f_unsat-10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

AA_pig_layers_plus = (f_AA * 1.10) * P_pig_layers
Q_M_pig_plus = float(AA_pig_layers_plus[0] P_nc_pig_layers[0] AA_pig_layers_plus[2] P_nc_pig_layers[2])
Q_pig_AA_plus = np.array([Q_M_pig_plus, baseline_Q_pig[1], baseline_Q_pig[2], baseline_Q_pig[3]], dtype=float)
R_sens, A_rel_sens = recompute_pig_intensities(baseline_E_k_J, Q_pig_AA_plus, baseline_aw_series)
sensitivity_names.append('f_AA 10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

AA_pig_layers_minus = (f_AA * 0.90) * P_pig_layers
Q_M_pig_minus = float(AA_pig_layers_minus[0] P_nc_pig_layers[0] AA_pig_layers_minus[2] P_nc_pig_layers[2])
Q_pig_AA_minus = np.array([Q_M_pig_minus, baseline_Q_pig[1], baseline_Q_pig[2], baseline_Q_pig[3]], dtype=float)
R_sens, A_rel_sens = recompute_pig_intensities(baseline_E_k_J, Q_pig_AA_minus, baseline_aw_series)
sensitivity_names.append('f_AA-10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

aw_series_plus = baseline_aw_series.copy()
F6_start = phase_boundaries[3]
F6_end = phase_boundaries[4]
aw_series_plus[F6_start:F6_end] = np.clip(baseline_aw_series[F6_start:F6_end] * 1.10, 0.0, 1.0)
R_sens, A_rel_sens = recompute_pig_intensities(baseline_E_k_J, baseline_Q_pig, aw_series_plus)
sensitivity_names.append('a_wF6 10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

aw_series_minus = baseline_aw_series.copy()
aw_series_minus[F6_start:F6_end] = np.clip(baseline_aw_series[F6_start:F6_end] * 0.90, 0.0, 1.0)
R_sens, A_rel_sens = recompute_pig_intensities(baseline_E_k_J, baseline_Q_pig, aw_series_minus)
sensitivity_names.append('a_wF6-10pct')
sensitivity_results_R.append((R_sens - baseline_R) / max(baseline_R, 1e-8))
sensitivity_results_Arel.append((A_rel_sens - baseline_Arel) / np.maximum(baseline_Arel, 1e-8))

sensitivity_results_R = np.array(sensitivity_results_R, dtype=float)
sensitivity_results_Arel = np.vstack(sensitivity_results_Arel)

assert np.all(np.isfinite(sensitivity_results_R)) and np.all(np.isfinite(sensitivity_results_Arel)), 'Non-finite sensitivity results'

fig_S, axes_S = plt.subplots(2, 1, figsize=(10, 10))
x_pos_sens = np.arange(len(sensitivity_names))
axes_S[0].bar(x_pos_sens, sensitivity_results_R, color='#4C72B0')
axes_S[0].set_xticks(x_pos_sens)
axes_S[0].set_xticklabels(sensitivity_names, rotation=45, ha='right')
axes_S[0].set_ylabel('Delta relativa R_aroma_norm')
axes_S[0].set_title('Sensibilidad de R_aroma_norm Pig')
axes_S[0].grid(axis='y', linestyle='--', linewidth=0.5)

width_bar = 0.18
for i, ch in enumerate(channels):
 axes_S[1].bar(x_pos_sens (i - 1.5) * width_bar, sensitivity_results_Arel[:, i], width_bar, label='Canal ' ch)
axes_S[1].set_xticks(x_pos_sens)
axes_S[1].set_xticklabels(sensitivity_names, rotation=45, ha='right')
axes_S[1].set_ylabel('Delta relativa A_k_rel')
axes_S[1].set_title('Sensibilidad de A_k_rel Pig')
axes_S[1].grid(axis='y', linestyle='--', linewidth=0.5)
axes_S[1].legend(loc='best')

plt.tight_layout()
plt.savefig(sensitivity_png_path, bbox_inches='tight')
plt.close(fig_S)

sens_R_df = pd.DataFrame({'Scenario': sensitivity_names, 'Delta_R_norm': sensitivity_results_R})
sens_Arel_df = pd.DataFrame({'Scenario': sensitivity_names, 'Delta_A_M': sensitivity_results_Arel[:, 0], 'Delta_A_L': sensitivity_results_Arel[:, 1], 'Delta_A_G': sensitivity_results_Arel[:, 2], 'Delta_A_S': sensitivity_results_Arel[:, 3]})

fig_S_int = go.Figure()
fig_S_int.add_trace(go.Bar(x=sens_R_df['Scenario'], y=sens_R_df['Delta_R_norm'], name='Delta R_aroma_norm'))
fig_S_int.update_layout(title='Sensibilidad R_aroma_norm Pig', xaxis_title='Escenario', yaxis_title='Delta relativa', barmode='group')
fig_S_int.write_html(sensitivity_html_path, include_plotlyjs='cdn')

progress = total_progress
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

result = {}
result['status'] = 'ok'
result['description'] = 'Computed a multi-source aromatic model for Jhon Dallas suckling pig vs Peking-style duck using USDA-based precursor pools, a reference thermal schedule, kinetic and water-activity modulation functions, integrated aromatic channel indices, distance and similarity metrics, and exported Movement 2 tables and figures.'

metrics = {}
metrics['A_M_rel_pig'] = float(f'{A_rel_pig[0]:.2f}')
metrics['A_L_rel_pig'] = float(f'{A_rel_pig[1]:.2f}')
metrics['A_G_rel_pig'] = float(f'{A_rel_pig[2]:.2f}')
metrics['A_S_rel_pig'] = float(f'{A_rel_pig[3]:.2f}')
metrics['A_M_rel_duck'] = float(f'{A_rel_duck[0]:.2f}')
metrics['A_L_rel_duck'] = float(f'{A_rel_duck[1]:.2f}')
metrics['A_G_rel_duck'] = float(f'{A_rel_duck[2]:.2f}')
metrics['A_S_rel_duck'] = float(f'{A_rel_duck[3]:.2f}')
metrics['R_aroma_norm_pig'] = float(f'{R_aroma_norm_pig:.2f}')
metrics['R_aroma_norm_duck'] = float(f'{R_aroma_norm_duck:.2f}')
metrics['d_aroma'] = float(f'{d_aroma:.2f}')
metrics['S_aroma'] = float(f'{S_aroma:.2f}')
result['metrics'] = metrics

tables = {}
tables['Indices'] = Indices_df.to_dict(orient='list')
tables['AromaDistances'] = AromaDist_df.to_dict(orient='list')
result['tables'] = tables

image_files = [kinetics_png, awmod_png, Ikt_pig_png, Arel_bars_png, origin_family_png, sensitivity_png]
html_files = [kinetics_html, awmod_html, Ikt_pig_html, Arel_bars_html, origin_family_html, sensitivity_html]

result['images'] = image_files
result['caption'] = ['Funciones cineticas normalizadas', 'Funciones de modulacion de actividad de agua', 'Intensidades aromaticas I_k(t) Pig', 'Composicion aromatica relativa A_k_rel Pig vs Duck', 'Mapa origen-familia Pig (capas x canales)', 'Sensibilidad de indices aromaticos Pig']
result['files'] = [movement2_excel] html_files

result['excel_file'] = movement2_excel
result['image_files'] = image_files
result['html_files'] = html_files

summary = {}
summary['A_rel_pig'] = [float(f'{v:.2f}') for v in A_rel_pig]
summary['A_rel_duck'] = [float(f'{v:.2f}') for v in A_rel_duck]
summary['R_aroma_norm_pig'] = float(f'{R_aroma_norm_pig:.2f}')
summary['R_aroma_norm_duck'] = float(f'{R_aroma_norm_duck:.2f}')
summary['d_aroma'] = float(f'{d_aroma:.2f}')
summary['S_aroma'] = float(f'{S_aroma:.2f}')
result['summary'] = summary