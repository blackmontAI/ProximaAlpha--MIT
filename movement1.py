import os
import uuid
import math
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
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

m = 6.0
k_pig = 0.11
k_duck = 0.13
rho_body = 1050.0
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
n_skin = 0.85
n_fat = 0.82
D_skin = 5e-11
D_fat = 5e-12
T_air = 9.0
RH = 0.65
t_dry_h = 48.0

L_skin_pig = L_skin_pig_mm * 1e-3
L_fat_pig = L_fat_pig_mm * 1e-3
L_muscle_pig = L_muscle_pig_mm * 1e-3
L_skin_duck = L_skin_duck_mm * 1e-3
L_fat_duck = L_fat_duck_mm * 1e-3
L_muscle_duck = L_muscle_duck_mm * 1e-3
t_dry = t_dry_h * 3600.0

A_pig = k_pig * (m ** (2.0 / 3.0))
A_duck = k_duck * (m ** (2.0 / 3.0))

phase_setup_weight = 5.0
phase_usda_weight = 25.0
phase_comp_weight = 20.0
phase_dry_weight = 25.0
phase_sim_weight = 10.0
phase_outputs_weight = 15.0

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
 name_lower = str(n_name).strip().lower()
 if n_val is None:
 continue
 try:
 val = float(n_val)
 except Exception:
 continue
 if 'water' == name_lower or name_lower.startswith('water '):
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
 return {
 'water_g_per_100g': float(water),
 'protein_g_per_100g': float(protein),
 'fat_g_per_100g': float(fat),
 'sfa_g_per_100g': float(sfa),
 'mufa_g_per_100g': float(mufa),
 'pufa_g_per_100g': float(pufa)
 }

food_queries = {
 'pig_skin': ['pork, skin, raw'],
 'pig_fat': ['pork, fresh, backfat, raw'],
 'pig_muscle': ['pork, loin, raw, lean only'],
 'duck_skin': ['duck, domesticated, skin, with subcutaneous fat, raw', 'duck, skin, raw', 'duck, domesticated, meat and skin, raw'],
 'duck_muscle': ['duck, domesticated, meat only, raw', 'duck, meat only, raw'],
 'duck_global': ['duck, domesticated, meat and skin, raw', 'duck, meat and skin, raw']
}

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
 usda_nutrient_data[key] = {
 'query_used': q,
 'fdcId': int(fdc_id),
 'description': food_record.get('description', ''),
 'nutrients': nut
 }
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

L_layers_pig = np.array([L_skin_pig, L_fat_pig, L_muscle_pig], dtype=float)
L_layers_duck = np.array([L_skin_duck, L_fat_duck, L_muscle_duck], dtype=float)
rho_layers_pig = np.array([rho_skin, rho_fat, rho_muscle], dtype=float)
rho_layers_duck = np.array([rho_skin, rho_fat, rho_muscle], dtype=float)

assert np.all(L_layers_pig > 0.0) and np.all(L_layers_duck > 0.0), 'Layer thickness must be positive'
assert np.all(rho_layers_pig > 0.0) and np.all(rho_layers_duck > 0.0), 'Layer densities must be positive'

m_layers_pig_geom = rho_layers_pig * L_layers_pig
m_layers_duck_geom = rho_layers_duck * L_layers_duck

L_sum_pig = float(np.sum(L_layers_pig))
L_sum_duck = float(np.sum(L_layers_duck))

f_L_pig = L_layers_pig / L_sum_pig
f_L_duck = L_layers_duck / L_sum_duck

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

duck_skin_nut = None
if 'duck_skin' in usda_nutrient_data:
 duck_skin_nut = usda_nutrient_data['duck_skin']['nutrients']
duck_muscle_nut = usda_nutrient_data['duck_muscle']['nutrients']
duck_global_nut = usda_nutrient_data['duck_global']['nutrients']

for i, lname in enumerate(layers_order):
 if lname == 'skin':
 nut = duck_skin_nut if duck_skin_nut is not None else duck_global_nut
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
w_pig_JD = w_pig_layers.copy()
p_pig_JD = p_pig_layers.copy()
f_pig_JD = f_pig_layers.copy()

for i, lname in enumerate(layers_order):
 if lname in ['skin', 'fat']:
 F_pig_JD[i] = r_fat * F_pig_base[i]
 W_pig_JD[i] = W_pig_base[i]
 P_pig_JD[i] = P_pig_base[i]
 m_pig_JD[i] = W_pig_JD[i] P_pig_JD[i] F_pig_JD[i]
 if m_pig_JD[i] <= 0.0:
 raise ValueError('Adjusted mass m_pig_JD is non-positive for layer ' lname)
 w_pig_JD[i] = W_pig_JD[i] / m_pig_JD[i]
 p_pig_JD[i] = P_pig_JD[i] / m_pig_JD[i]
 f_pig_JD[i] = F_pig_JD[i] / m_pig_JD[i]

m_pig_final = m_pig_JD.copy()
m_duck_final = m_duck_base.copy()

W_pig_layers = m_pig_final * w_pig_JD
P_pig_layers = m_pig_final * p_pig_JD
F_pig_layers = m_pig_final * f_pig_JD

W_duck_layers = m_duck_final * w_duck_layers
P_duck_layers = m_duck_final * p_duck_layers
F_duck_layers = m_duck_final * f_duck_layers

f_coll_skin = 0.30
f_coll_fat = 0.15
f_coll_muscle = 0.06
f_coll_array = np.array([f_coll_skin, f_coll_fat, f_coll_muscle], dtype=float)

Coll_pig_layers = P_pig_layers * f_coll_array
P_nc_pig_layers = P_pig_layers - Coll_pig_layers

Coll_duck_layers = P_duck_layers * f_coll_array
P_nc_duck_layers = P_duck_layers - Coll_duck_layers

W_tot_pig = float(np.sum(W_pig_layers))
P_tot_pig = float(np.sum(P_pig_layers))
F_tot_pig = float(np.sum(F_pig_layers))
Coll_tot_pig = float(np.sum(Coll_pig_layers))
m_tot_pig = W_tot_pig P_tot_pig F_tot_pig

W_tot_duck = float(np.sum(W_duck_layers))
P_tot_duck = float(np.sum(P_duck_layers))
F_tot_duck = float(np.sum(F_duck_layers))
Coll_tot_duck = float(np.sum(Coll_duck_layers))
m_tot_duck = W_tot_duck P_tot_duck F_tot_duck

assert m_tot_pig > 0.0 and m_tot_duck > 0.0, 'Total mass per area must be positive for both species'
assert P_tot_pig > 0.0 and P_tot_duck > 0.0, 'Total protein per area must be positive for both species'
assert F_tot_pig > 0.0 and F_tot_duck > 0.0, 'Total fat per area must be positive for both species'

water_pct_pig = 100.0 * W_tot_pig / m_tot_pig
protein_pct_pig = 100.0 * P_tot_pig / m_tot_pig
fat_pct_pig = 100.0 * F_tot_pig / m_tot_pig
collagen_over_protein_pct_pig = 100.0 * Coll_tot_pig / P_tot_pig

water_pct_duck = 100.0 * W_tot_duck / m_tot_duck
protein_pct_duck = 100.0 * P_tot_duck / m_tot_duck
fat_pct_duck = 100.0 * F_tot_duck / m_tot_duck
collagen_over_protein_pct_duck = 100.0 * Coll_tot_duck / P_tot_duck

sfa_pig, mufa_pig, pufa_pig = lipid_profile_fractions(usda_nutrient_data['pig_fat']['nutrients'])
sfa_duck, mufa_duck, pufa_duck = lipid_profile_fractions(usda_nutrient_data['duck_global']['nutrients'])

F_SFA_pig = F_tot_pig * sfa_pig
F_MUFA_pig = F_tot_pig * mufa_pig
F_PUFA_pig = F_tot_pig * pufa_pig

F_SFA_duck = F_tot_duck * sfa_duck
F_MUFA_duck = F_tot_duck * mufa_duck
F_PUFA_duck = F_tot_duck * pufa_duck

sfa_pct_pig = 100.0 * sfa_pig
mufa_pct_pig = 100.0 * mufa_pig
pufa_pct_pig = 100.0 * pufa_pig

sfa_pct_duck = 100.0 * sfa_duck
mufa_pct_duck = 100.0 * mufa_duck
pufa_pct_duck = 100.0 * pufa_duck

progress = phase_setup_weight phase_usda_weight phase_comp_weight * 0.5
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

w0_pig_skin = w_pig_JD[0]
w0_pig_fat = w_pig_JD[1]
w0_duck_skin = w_duck_layers[0]
w0_duck_fat = w_duck_layers[1]

assert w0_pig_skin > 0.0 and w0_pig_fat > 0.0 and w0_duck_skin > 0.0 and w0_duck_fat > 0.0, 'Initial water fractions w0 must be positive for envelope layers'

w_eq_pig_skin = w0_pig_skin * (RH ** (1.0 / n_skin))
w_eq_pig_fat = w0_pig_fat * (RH ** (1.0 / n_fat))
w_eq_duck_skin = w0_duck_skin * (RH ** (1.0 / n_skin))
w_eq_duck_fat = w0_duck_fat * (RH ** (1.0 / n_fat))

tau_pig_skin = (L_skin_pig ** 2) / (math.pi ** 2 * D_skin)
tau_pig_fat = (L_fat_pig ** 2) / (math.pi ** 2 * D_fat)
tau_duck_skin = (L_skin_duck ** 2) / (math.pi ** 2 * D_skin)
tau_duck_fat = (L_fat_duck ** 2) / (math.pi ** 2 * D_fat)

k_eff_pig_skin = 1.0 / tau_pig_skin
k_eff_pig_fat = 1.0 / tau_pig_fat
k_eff_duck_skin = 1.0 / tau_duck_skin
k_eff_duck_fat = 1.0 / tau_duck_fat

w_pig_skin_tdry = w_eq_pig_skin (w0_pig_skin - w_eq_pig_skin) * math.exp(-k_eff_pig_skin * t_dry)
w_pig_fat_tdry = w_eq_pig_fat (w0_pig_fat - w_eq_pig_fat) * math.exp(-k_eff_pig_fat * t_dry)
w_duck_skin_tdry = w_eq_duck_skin (w0_duck_skin - w_eq_duck_skin) * math.exp(-k_eff_duck_skin * t_dry)
w_duck_fat_tdry = w_eq_duck_fat (w0_duck_fat - w_eq_duck_fat) * math.exp(-k_eff_duck_fat * t_dry)

W_init_pig_skin = m_pig_final[0] * w0_pig_skin
W_final_pig_skin = m_pig_final[0] * w_pig_skin_tdry
W_init_pig_fat = m_pig_final[1] * w0_pig_fat
W_final_pig_fat = m_pig_final[1] * w_pig_fat_tdry

W_init_duck_skin = m_duck_final[0] * w0_duck_skin
W_final_duck_skin = m_duck_final[0] * w_duck_skin_tdry
W_init_duck_fat = m_duck_final[1] * w0_duck_fat
W_final_duck_fat = m_duck_final[1] * w_duck_fat_tdry

assert W_init_pig_skin > 0.0 and W_init_pig_fat > 0.0 and W_init_duck_skin > 0.0 and W_init_duck_fat > 0.0, 'Initial water masses must be positive'

Loss_pig_skin = (W_init_pig_skin - W_final_pig_skin) / W_init_pig_skin
Loss_pig_fat = (W_init_pig_fat - W_final_pig_fat) / W_init_pig_fat
Loss_duck_skin = (W_init_duck_skin - W_final_duck_skin) / W_init_duck_skin
Loss_duck_fat = (W_init_duck_fat - W_final_duck_fat) / W_init_duck_fat

Loss_pig_skin_pct = 100.0 * Loss_pig_skin
Loss_pig_fat_pct = 100.0 * Loss_pig_fat
Loss_duck_skin_pct = 100.0 * Loss_duck_skin
Loss_duck_fat_pct = 100.0 * Loss_duck_fat

time_h_grid = np.linspace(0.0, 48.0, 100)
time_s_grid = time_h_grid * 3600.0

w_pig_skin_t = w_eq_pig_skin (w0_pig_skin - w_eq_pig_skin) * np.exp(-k_eff_pig_skin * time_s_grid)
w_pig_fat_t = w_eq_pig_fat (w0_pig_fat - w_eq_pig_fat) * np.exp(-k_eff_pig_fat * time_s_grid)
w_duck_skin_t = w_eq_duck_skin (w0_duck_skin - w_eq_duck_skin) * np.exp(-k_eff_duck_skin * time_s_grid)
w_duck_fat_t = w_eq_duck_fat (w0_duck_fat - w_eq_duck_fat) * np.exp(-k_eff_duck_fat * time_s_grid)

X_pig_skin_t = w_pig_skin_t / w0_pig_skin
X_pig_fat_t = w_pig_fat_t / w0_pig_fat
X_duck_skin_t = w_duck_skin_t / w0_duck_skin
X_duck_fat_t = w_duck_fat_t / w0_duck_fat

assert np.all(np.isfinite(X_pig_skin_t)) and np.all(np.isfinite(X_pig_fat_t)) and np.all(np.isfinite(X_duck_skin_t)) and np.all(np.isfinite(X_duck_fat_t)), 'Drying curves contain non-finite values'

progress = phase_setup_weight phase_usda_weight phase_comp_weight phase_dry_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

Fo_pig_skin = D_skin * t_dry / (L_skin_pig ** 2)
Fo_pig_fat = D_fat * t_dry / (L_fat_pig ** 2)
Fo_duck_skin = D_skin * t_dry / (L_skin_duck ** 2)
Fo_duck_fat = D_fat * t_dry / (L_fat_duck ** 2)

Psi_pig = W_tot_pig / F_tot_pig
Psi_duck = W_tot_duck / F_tot_duck

Phi_coll_pig = Coll_tot_pig / P_tot_pig
Phi_coll_duck = Coll_tot_duck / P_tot_duck

z_labels = ['f_L_skin', 'f_L_fat', 'f_L_muscle', 'Fo_skin', 'Fo_fat', 'Psi', 'Phi_coll', 'f_SFA', 'f_MUFA', 'f_PUFA']

z_pig = np.array([
 f_L_pig[0],
 f_L_pig[1],
 f_L_pig[2],
 Fo_pig_skin,
 Fo_pig_fat,
 Psi_pig,
 Phi_coll_pig,
 sfa_pig,
 mufa_pig,
 pufa_pig
], dtype=float)

z_duck = np.array([
 f_L_duck[0],
 f_L_duck[1],
 f_L_duck[2],
 Fo_duck_skin,
 Fo_duck_fat,
 Psi_duck,
 Phi_coll_duck,
 sfa_duck,
 mufa_duck,
 pufa_duck
], dtype=float)

assert z_pig.shape == z_duck.shape, 'Descriptor vectors must have the same shape'
n_descriptors = z_pig.shape[0]

sigma = np.maximum(np.abs(z_pig), np.abs(z_duck))
sigma_min = 1e-3
sigma[sigma < sigma_min] = sigma_min

alpha = np.ones(n_descriptors, dtype=float) / float(n_descriptors)

delta_z = z_pig - z_duck
scaled_sq = alpha * ((delta_z / sigma) ** 2)
d_pig_duck = float(np.sqrt(np.sum(scaled_sq)))
S_phys = float(math.exp(-d_pig_duck))

sim_components = 1.0 - np.abs(delta_z) / sigma
sim_components = np.clip(sim_components, 0.0, 1.0)

progress = phase_setup_weight phase_usda_weight phase_comp_weight phase_dry_weight phase_sim_weight
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

layers_records = []
for species, L_mm_arr, rho_arr, m_arr in [
 ('Pig', np.array([L_skin_pig_mm, L_fat_pig_mm, L_muscle_pig_mm], dtype=float), rho_layers_pig, m_pig_final),
 ('Duck', np.array([L_skin_duck_mm, L_fat_duck_mm, L_muscle_duck_mm], dtype=float), rho_layers_duck, m_duck_final)
]:
 for i, lname in enumerate(layers_order):
 layers_records.append({
 'Species': species,
 'Layer': lname,
 'L_mm': float(L_mm_arr[i]),
 'rho': float(rho_arr[i]),
 'm_l_kg_per_m2': float(m_arr[i])
 })

layers_df = pd.DataFrame(layers_records)
assert not layers_df.isna().any().any(), 'layers_df contains NaN values'

global_comp_records = [
 {
 'Species': 'Pig',
 'Water_pct': water_pct_pig,
 'Protein_pct': protein_pct_pig,
 'Fat_pct': fat_pct_pig,
 'Collagen_over_protein_pct': collagen_over_protein_pct_pig
 },
 {
 'Species': 'Duck',
 'Water_pct': water_pct_duck,
 'Protein_pct': protein_pct_duck,
 'Fat_pct': fat_pct_duck,
 'Collagen_over_protein_pct': collagen_over_protein_pct_duck
 }
]

global_comp_df = pd.DataFrame(global_comp_records)
assert not global_comp_df.isna().any().any(), 'global_comp_df contains NaN values'

lipid_records = [
 {
 'Species': 'Pig',
 'SFA_pct_fat': sfa_pct_pig,
 'MUFA_pct_fat': mufa_pct_pig,
 'PUFA_pct_fat': pufa_pct_pig
 },
 {
 'Species': 'Duck',
 'SFA_pct_fat': sfa_pct_duck,
 'MUFA_pct_fat': mufa_pct_duck,
 'PUFA_pct_fat': pufa_pct_duck
 }
]

lipid_df = pd.DataFrame(lipid_records)
assert not lipid_df.isna().any().any(), 'lipid_df contains NaN values'

drying_records = [
 {
 'Species': 'Pig',
 'Layer': 'skin',
 'w0': w0_pig_skin,
 'w_eq': w_eq_pig_skin,
 'w_tdry': w_pig_skin_tdry,
 'Loss_pct': Loss_pig_skin_pct
 },
 {
 'Species': 'Pig',
 'Layer': 'fat',
 'w0': w0_pig_fat,
 'w_eq': w_eq_pig_fat,
 'w_tdry': w_pig_fat_tdry,
 'Loss_pct': Loss_pig_fat_pct
 },
 {
 'Species': 'Duck',
 'Layer': 'skin',
 'w0': w0_duck_skin,
 'w_eq': w_eq_duck_skin,
 'w_tdry': w_duck_skin_tdry,
 'Loss_pct': Loss_duck_skin_pct
 },
 {
 'Species': 'Duck',
 'Layer': 'fat',
 'w0': w0_duck_fat,
 'w_eq': w_eq_duck_fat,
 'w_tdry': w_duck_fat_tdry,
 'Loss_pct': Loss_duck_fat_pct
 }
]

drying_df = pd.DataFrame(drying_records)
assert not drying_df.isna().any().any(), 'drying_df contains NaN values'

similarity_records = []
for i in range(n_descriptors):
 similarity_records.append({
 'Descriptor': z_labels[i],
 'Value_pig': float(z_pig[i]),
 'Value_duck': float(z_duck[i]),
 'Similarity_component': float(sim_components[i])
 })
similarity_records.append({
 'Descriptor': 'S_phys_global',
 'Value_pig': float(S_phys),
 'Value_duck': float(S_phys),
 'Similarity_component': float(S_phys)
})

similarity_df = pd.DataFrame(similarity_records)
assert not similarity_df.isna().any().any(), 'similarity_df contains NaN values'

excel_filename = 'movement1_tables.xlsx'
excel_path = os.path.join(base_path, excel_filename)
with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
 layers_df.to_excel(writer, sheet_name='Layers', index=False)
 global_comp_df.to_excel(writer, sheet_name='GlobalComposition', index=False)
 lipid_df.to_excel(writer, sheet_name='LipidProfile', index=False)
 drying_df.to_excel(writer, sheet_name='Drying', index=False)
 similarity_df.to_excel(writer, sheet_name='Similarity', index=False)

progress = phase_setup_weight phase_usda_weight phase_comp_weight phase_dry_weight phase_sim_weight phase_outputs_weight * 0.3
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

layers_stacked_png = 'movement1_layers_stacked_bar.png'
drying_curves_png = 'movement1_drying_curves.png'
similarity_heatmap_png = 'movement1_similarity_heatmap.png'

fig_layers, ax_layers = plt.subplots(figsize=(8, 6))
x_pos = np.arange(2)
width = 0.5
pig_f_L = f_L_pig
duck_f_L = f_L_duck
bottom_pig = np.zeros_like(x_pos, dtype=float)
bottom_duck = np.zeros_like(x_pos, dtype=float)

ax_layers.bar(0, pig_f_L[0], width, label='Pig skin', color='#4C72B0')
ax_layers.bar(0, pig_f_L[1], width, bottom=pig_f_L[0], label='Pig fat', color='#55A868')
ax_layers.bar(0, pig_f_L[2], width, bottom=pig_f_L[0] pig_f_L[1], label='Pig muscle', color='#C44E52')

ax_layers.bar(1, duck_f_L[0], width, label='Duck skin', color='#8172B2')
ax_layers.bar(1, duck_f_L[1], width, bottom=duck_f_L[0], label='Duck fat', color='#CCB974')
ax_layers.bar(1, duck_f_L[2], width, bottom=duck_f_L[0] duck_f_L[1], label='Duck muscle', color='#64B5CD')

ax_layers.set_xticks(x_pos)
ax_layers.set_xticklabels(['Pig', 'Duck'])
ax_layers.set_ylabel('Fraction of thickness')
ax_layers.set_title('Layer thickness fractions by species')
ax_layers.grid(axis='y', linestyle='--', linewidth=0.5)
handles, labels = ax_layers.get_legend_handles_labels()
unique = dict(zip(labels, handles))
ax_layers.legend(unique.values(), unique.keys(), loc='best')
layers_png_path = os.path.join(base_path, layers_stacked_png)
plt.tight_layout()
plt.savefig(layers_png_path, bbox_inches='tight')
plt.close(fig_layers)

layers_plot_df = pd.DataFrame({
 'Species': ['Pig', 'Pig', 'Pig', 'Duck', 'Duck', 'Duck'],
 'Layer': ['skin', 'fat', 'muscle', 'skin', 'fat', 'muscle'],
 'Fraction': [pig_f_L[0], pig_f_L[1], pig_f_L[2], duck_f_L[0], duck_f_L[1], duck_f_L[2]]
})
fig_layers_interactive = px.bar(layers_plot_df, x='Species', y='Fraction', color='Layer', title='Layer thickness fractions - interactive', barmode='stack')
layers_html = 'movement1_layers_stacked_bar.html'
layers_html_path = os.path.join(base_path, layers_html)
fig_layers_interactive.write_html(layers_html_path, include_plotlyjs='cdn')

fig_dry, ax_dry = plt.subplots(figsize=(10, 6))
ax_dry.plot(time_h_grid, X_pig_skin_t, label='Pig skin', color='#4C72B0', linewidth=1.2)
ax_dry.plot(time_h_grid, X_pig_fat_t, label='Pig fat', color='#55A868', linewidth=1.2)
ax_dry.plot(time_h_grid, X_duck_skin_t, label='Duck skin', color='#8172B2', linewidth=1.2)
ax_dry.plot(time_h_grid, X_duck_fat_t, label='Duck fat', color='#CCB974', linewidth=1.2)
ax_dry.set_xlabel('Time (h)')
ax_dry.set_ylabel('Normalized moisture X(t) = w(t)/w0')
ax_dry.set_title('Drying curves for skin and fat layers')
ax_dry.grid(True, linestyle='--', linewidth=0.5)
ax_dry.legend(loc='best')
drying_png_path = os.path.join(base_path, drying_curves_png)
plt.tight_layout()
plt.savefig(drying_png_path, bbox_inches='tight')
plt.close(fig_dry)

drying_plot_df = pd.DataFrame({
 'time_h': list(time_h_grid) * 4,
 'X': np.concatenate([X_pig_skin_t, X_pig_fat_t, X_duck_skin_t, X_duck_fat_t]),
 'Curve': ['Pig skin'] * len(time_h_grid) ['Pig fat'] * len(time_h_grid) ['Duck skin'] * len(time_h_grid) ['Duck fat'] * len(time_h_grid)
})
fig_dry_interactive = px.line(drying_plot_df, x='time_h', y='X', color='Curve', title='Drying curves - interactive')
drying_html = 'movement1_drying_curves.html'
drying_html_path = os.path.join(base_path, drying_html)
fig_dry_interactive.write_html(drying_html_path, include_plotlyjs='cdn')

sim_components_array = np.array(sim_components, dtype=float)
fig_sim, ax_sim = plt.subplots(figsize=(8, 6))
im = ax_sim.imshow(sim_components_array.reshape(-1, 1), aspect='auto', cmap='viridis', vmin=0.0, vmax=1.0)
ax_sim.set_yticks(np.arange(n_descriptors))
ax_sim.set_yticklabels(z_labels)
ax_sim.set_xticks([0])
ax_sim.set_xticklabels(['Similarity'])
ax_sim.set_title('Similarity components heatmap')
for i in range(n_descriptors):
 val = sim_components_array[i]
 ax_sim.text(0, i, f'{val:.2f}', ha='center', va='center', color='white' if val < 0.5 else 'black', fontsize=8)
plt.colorbar(im, ax=ax_sim, fraction=0.046, pad=0.04)
sim_png_path = os.path.join(base_path, similarity_heatmap_png)
plt.tight_layout()
plt.savefig(sim_png_path, bbox_inches='tight')
plt.close(fig_sim)

sim_heatmap_df = pd.DataFrame({'Descriptor': z_labels, 'Similarity': sim_components_array})
fig_sim_interactive = px.imshow(sim_heatmap_df[['Similarity']].to_numpy(), labels=dict(x='Similarity', y='Descriptor', color='Similarity'), x=['Similarity'], y=z_labels, color_continuous_scale='Viridis', zmin=0.0, zmax=1.0, title='Similarity components heatmap - interactive')
sim_html = 'movement1_similarity_heatmap.html'
sim_html_path = os.path.join(base_path, sim_html)
fig_sim_interactive.write_html(sim_html_path, include_plotlyjs='cdn')

progress = total_progress
msgLog = f'Executed: {progress:.1f}%'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

result = {}
result['status'] = 'ok'
result['description'] = 'Constructed 1D multilayer physical-compositional models for suckling pig vs Peking duck, retrieved USDA compositions via API, applied Jhon Dallas fat adjustment for pig, computed global composition and lipid profiles, simulated drying of skin and fat layers, built a structural similarity index S_phys from dimensionless descriptors, exported all core tables to Excel, and generated stacked layer, drying-curve, and similarity heatmap figures.'
result_metrics = {}
result_metrics['S_phys'] = round(S_phys, 4)
result_metrics['d_pig_duck'] = round(d_pig_duck, 4)
result_metrics['Water_pct_pig'] = round(water_pct_pig, 2)
result_metrics['Protein_pct_pig'] = round(protein_pct_pig, 2)
result_metrics['Fat_pct_pig'] = round(fat_pct_pig, 2)
result_metrics['Water_pct_duck'] = round(water_pct_duck, 2)
result_metrics['Protein_pct_duck'] = round(protein_pct_duck, 2)
result_metrics['Fat_pct_duck'] = round(fat_pct_duck, 2)
result_metrics['Psi_pig'] = round(Psi_pig, 4)
result_metrics['Psi_duck'] = round(Psi_duck, 4)
result_metrics['Phi_coll_pig'] = round(Phi_coll_pig, 4)
result_metrics['Phi_coll_duck'] = round(Phi_coll_duck, 4)
result_metrics['Loss_pig_skin_pct'] = round(Loss_pig_skin_pct, 2)
result_metrics['Loss_pig_fat_pct'] = round(Loss_pig_fat_pct, 2)
result_metrics['Loss_duck_skin_pct'] = round(Loss_duck_skin_pct, 2)
result_metrics['Loss_duck_fat_pct'] = round(Loss_duck_fat_pct, 2)
result['metrics'] = result_metrics

tables_dict = {}
tables_dict['GlobalComposition'] = global_comp_df.to_dict(orient='list')
tables_dict['LipidProfile'] = lipid_df.to_dict(orient='list')
tables_dict['DryingSummary'] = drying_df.to_dict(orient='list')
tables_dict['SimilarityComponents'] = similarity_df.to_dict(orient='list')
result['tables'] = tables_dict

image_files = [layers_stacked_png, drying_curves_png, similarity_heatmap_png]
result['images'] = image_files
result['caption'] = [
 'Stacked bar chart of layer thickness fractions for pig vs duck',
 'Drying curves X(t) for skin and fat layers of pig and duck over 0–48 h',
 'Heatmap of similarity components between pig and duck based on structural descriptors'
]

files_list = [excel_filename, layers_html, drying_html, sim_html]
result['files'] = files_list

result['excel_file'] = excel_filename
result['image_files'] = image_files
result['summary'] = {
 'S_phys': round(S_phys, 4),
 'Most_similar_descriptor': z_labels[int(np.argmax(sim_components_array))],
 'Least_similar_descriptor': z_labels[int(np.argmin(sim_components_array))]
}

result