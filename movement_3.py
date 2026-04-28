import os
import sys
import uuid
import json
import math
import warnings
import subprocess
import importlib
import importlib.util   # necesario para find_spec
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

from send_message_backend import send_message_backend
from get_credentials import get_credentials

# ----------------------------------------------------------------------
# Backend arguments (debe ser proporcionado por el entorno)
# ----------------------------------------------------------------------
backend_args = {}   # <-- definido para que no falle send_message_backend

warnings.filterwarnings('default')

# ----------------------------------------------------------------------
# Función para instalar/importar paquetes faltantes
# ----------------------------------------------------------------------
def ensure_package(package_name, import_name=None):
    name_to_import = import_name if import_name is not None else package_name
    spec = importlib.util.find_spec(name_to_import)
    if spec is None:
        cmd = [sys.executable, '-m', 'pip', 'install', package_name]
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if completed.returncode != 0:
            raise RuntimeError(f"Failed to install {package_name}")
    # Importar el módulo (ya esté recién instalado o ya presente)
    return importlib.import_module(name_to_import)

openpyxl_module = ensure_package('openpyxl', 'openpyxl')
PyPDF2_module = ensure_package('PyPDF2', 'PyPDF2')
from PyPDF2 import PdfReader

movement = 3
Nx_input = 31
T_init_C = 9.0
n_skin = 0.85
n_fat = 0.85
D_skin = 1e-11
D_fat = 5e-12
D_muscle = 8e-11
J_min = 0.4
J_max = 1.0
Fo_max_target = 0.2

files_path = '/mnt/z/B011'
movement1_excel_filename = 'movement1_tables_f4743dfd775f4da598d2dfbc9e9e1bf2.xlsx'
movement1_excel_path = os.path.join(files_path, movement1_excel_filename)
movement2_excel_filename = 'movement2_perfect_tables_fa9c2e321cec476db40f8f5ae6ff343c.xlsx'
movement2_excel_path = os.path.join(files_path, movement2_excel_filename)
pdf_global_filename = 'file-UAhKXSK3ckd6RptZt52yXz.pdf'
pdf_global_path = os.path.join(files_path, pdf_global_filename)
pdf_m3_filename = 'file-HqQmVgTS2qy4UYsknv7cWK.pdf'
pdf_m3_path = os.path.join(files_path, pdf_m3_filename)

assert os.path.exists(movement1_excel_path), 'Movement 1 Excel file not found at expected path'
assert os.path.exists(movement2_excel_path), 'Movement 2 perfect Excel file not found at expected path'

msgLog = 'Executed: 5% - Movement 3 configuration and file paths initialized'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Lectura opcional de PDFs
# ----------------------------------------------------------------------
if os.path.exists(pdf_global_path):
    try:
        reader_global = PdfReader(pdf_global_path)
        text_global = ''
        for page in reader_global.pages:
            try:
                page_text = page.extract_text()
                if page_text is None:
                    page_text = ''
            except Exception:
                page_text = ''
            text_global = text_global + '\n' + page_text
    except Exception:
        text_global = ''
else:
    text_global = ''

if os.path.exists(pdf_m3_path):
    try:
        reader_m3 = PdfReader(pdf_m3_path)
        text_m3 = ''
        for page in reader_m3.pages:
            try:
                page_text = page.extract_text()
                if page_text is None:
                    page_text = ''
            except Exception:
                page_text = ''
            text_m3 = text_m3 + '\n' + page_text
    except Exception:
        text_m3 = ''
else:
    text_m3 = ''

# ----------------------------------------------------------------------
# Lectura y validación de los Excel de Movement 1 y 2
# ----------------------------------------------------------------------
msgLog = 'Executed: 15% - Movement 1 and Movement 2 Excel structures inspected'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

excel1_file = pd.ExcelFile(movement1_excel_path)
sheet_names1 = excel1_file.sheet_names
assert 'Thickness_global' in sheet_names1, 'Thickness_global sheet missing in Movement 1 Excel'
assert 'Global_lipid' in sheet_names1, 'Global_lipid sheet missing in Movement 1 Excel'
assert 'Drying_envelope' in sheet_names1, 'Drying_envelope sheet missing in Movement 1 Excel'

raw_thickness_global = pd.read_excel(movement1_excel_path, sheet_name='Thickness_global', header=None)
headers_thick = list(raw_thickness_global.iloc[0].astype(str))
expected_thick_headers = ['species', 'skin_thickness_mm', 'fat_thickness_mm', 'muscle_thickness_mm',
                          'global_water_pct_wet', 'global_protein_pct_wet', 'global_fat_pct_wet']
assert headers_thick == expected_thick_headers, 'Thickness_global headers mismatch'
thickness_global_df = pd.read_excel(movement1_excel_path, sheet_name='Thickness_global', header=0)
assert not thickness_global_df.isna().any().any(), 'Thickness_global contains NaNs'

raw_global_lipid = pd.read_excel(movement1_excel_path, sheet_name='Global_lipid', header=None)
headers_glip = list(raw_global_lipid.iloc[0].astype(str))
expected_glip_headers = ['species', 'SFA_pct_fat', 'MUFA_pct_fat', 'PUFA_pct_fat']
assert headers_glip == expected_glip_headers, 'Global_lipid headers mismatch'
global_lipid_df = pd.read_excel(movement1_excel_path, sheet_name='Global_lipid', header=0)
assert not global_lipid_df.isna().any().any(), 'Global_lipid contains NaNs'

raw_drying_env = pd.read_excel(movement1_excel_path, sheet_name='Drying_envelope', header=None)
headers_dry = list(raw_drying_env.iloc[0].astype(str))
expected_dry_headers = ['species', 'layer', 'Initial_water_kg_m2', 'Final_water_kg_m2', 'Loss_fraction']
assert headers_dry == expected_dry_headers, 'Drying_envelope headers mismatch'
drying_envelope_df = pd.read_excel(movement1_excel_path, sheet_name='Drying_envelope', header=0)
assert not drying_envelope_df.isna().any().any(), 'Drying_envelope contains NaNs'

excel2_file = pd.ExcelFile(movement2_excel_path)
sheet_names2 = excel2_file.sheet_names
assert 'Global_indices' in sheet_names2, 'Global_indices sheet missing in Movement 2 perfect Excel'

raw_global_indices = pd.read_excel(movement2_excel_path, sheet_name='Global_indices', header=None)
headers_R = list(raw_global_indices.iloc[0].astype(str))
expected_R_headers = ['species', 'R_gross', 'R_norm']
assert headers_R == expected_R_headers, 'Global_indices headers mismatch'
global_indices_df = pd.read_excel(movement2_excel_path, sheet_name='Global_indices', header=0)
assert not global_indices_df.isna().any().any(), 'Global_indices contains NaNs'

pig_R_row = global_indices_df[global_indices_df['species'] == 'pig_JD']
duck_R_row = global_indices_df[global_indices_df['species'] == 'duck']
assert pig_R_row.shape[0] == 1 and duck_R_row.shape[0] == 1, 'Global_indices must have one row for pig_JD and one for duck'
R_norm_pig = float(pig_R_row['R_norm'].iloc[0])
R_norm_duck = float(duck_R_row['R_norm'].iloc[0])

msgLog = 'Executed: 25% - Movement 1 geometry and Movement 2 aroma indices loaded'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Parámetros termofísicos y composiciones (reconstrucción desde Movement 1)
# ----------------------------------------------------------------------
rho_skin = 1050.0
rho_muscle = 1050.0
rho_fat = 900.0

pig_skin_thickness_m = 2.0e-3
pig_fat_thickness_m = 5.6e-3
pig_muscle_thickness_m = 10.0e-3
duck_skin_thickness_m = 1.5e-3
duck_fat_thickness_m = 5.0e-3
duck_muscle_thickness_m = 12.0e-3

m_pig_skin_kg_m2 = rho_skin * pig_skin_thickness_m
m_pig_fat_kg_m2 = rho_fat * pig_fat_thickness_m
m_pig_muscle_kg_m2 = rho_muscle * pig_muscle_thickness_m
m_duck_skin_kg_m2 = rho_skin * duck_skin_thickness_m
m_duck_fat_kg_m2 = rho_fat * duck_fat_thickness_m
m_duck_muscle_kg_m2 = rho_muscle * duck_muscle_thickness_m

pig_conv_skin_comp = {'water': 0.45, 'protein': 0.25, 'fat': 0.30}
pig_conv_fat_comp = {'water': 0.10, 'protein': 0.03, 'fat': 0.87}
pig_conv_muscle_comp = {'water': 0.72, 'protein': 0.21, 'fat': 0.07}
duck_conv_skin_comp = {'water': 0.50, 'protein': 0.20, 'fat': 0.30}
duck_conv_fat_comp = {'water': 0.15, 'protein': 0.04, 'fat': 0.81}
duck_conv_muscle_comp = {'water': 0.74, 'protein': 0.21, 'fat': 0.05}

def validate_layer_comp(layer_dict, tol=5e-3):
    s = layer_dict['water'] + layer_dict['protein'] + layer_dict['fat']
    assert abs(s - 1.0) <= tol, 'Layer composition does not sum to 1 within tolerance'

validate_layer_comp(pig_conv_skin_comp)
validate_layer_comp(pig_conv_fat_comp)
validate_layer_comp(pig_conv_muscle_comp)
validate_layer_comp(duck_conv_skin_comp)
validate_layer_comp(duck_conv_fat_comp)
validate_layer_comp(duck_conv_muscle_comp)

collagen_frac_skin = 0.30
collagen_frac_fat = 0.15
collagen_frac_muscle = 0.06

pig_lip_row = global_lipid_df[global_lipid_df['species'] == 'pig_JD']
duck_lip_row = global_lipid_df[global_lipid_df['species'] == 'duck']
assert pig_lip_row.shape[0] == 1 and duck_lip_row.shape[0] == 1, 'Global_lipid must have one row per species'

pig_fa_profile = {
    'SFA': float(pig_lip_row['SFA_pct_fat'].iloc[0]) / 100.0,
    'MUFA': float(pig_lip_row['MUFA_pct_fat'].iloc[0]) / 100.0,
    'PUFA': float(pig_lip_row['PUFA_pct_fat'].iloc[0]) / 100.0
}
duck_fa_profile = {
    'SFA': float(duck_lip_row['SFA_pct_fat'].iloc[0]) / 100.0,
    'MUFA': float(duck_lip_row['MUFA_pct_fat'].iloc[0]) / 100.0,
    'PUFA': float(duck_lip_row['PUFA_pct_fat'].iloc[0]) / 100.0
}

def validate_fa_profile(profile_dict, tol=5e-3):
    s = profile_dict['SFA'] + profile_dict['MUFA'] + profile_dict['PUFA']
    assert abs(s - 1.0) <= tol, 'Fatty acid profile does not sum to 1 within tolerance'

validate_fa_profile(pig_fa_profile)
validate_fa_profile(duck_fa_profile)

def layer_masses_from_comp(m_layer_kg_m2, comp_dict):
    W = m_layer_kg_m2 * comp_dict['water']
    P = m_layer_kg_m2 * comp_dict['protein']
    F = m_layer_kg_m2 * comp_dict['fat']
    return W, P, F

pig_W_skin_base, pig_P_skin_base, pig_F_skin_base = layer_masses_from_comp(m_pig_skin_kg_m2, pig_conv_skin_comp)
pig_W_fat_base, pig_P_fat_base, pig_F_fat_base = layer_masses_from_comp(m_pig_fat_kg_m2, pig_conv_fat_comp)
pig_W_muscle_base, pig_P_muscle_base, pig_F_muscle_base = layer_masses_from_comp(m_pig_muscle_kg_m2, pig_conv_muscle_comp)
duck_W_skin_base, duck_P_skin_base, duck_F_skin_base = layer_masses_from_comp(m_duck_skin_kg_m2, duck_conv_skin_comp)
duck_W_fat_base, duck_P_fat_base, duck_F_fat_base = layer_masses_from_comp(m_duck_fat_kg_m2, duck_conv_fat_comp)
duck_W_muscle_base, duck_P_muscle_base, duck_F_muscle_base = layer_masses_from_comp(m_duck_muscle_kg_m2, duck_conv_muscle_comp)

fat_reduction_factor_pig = 0.7

def apply_jd_modification(W_base, P_base, F_base, fat_reduction_factor):
    F_jd = F_base * fat_reduction_factor
    W_jd = W_base
    P_jd = P_base
    m_jd = W_jd + P_jd + F_jd
    assert m_jd > 0.0, 'Non positive layer mass after Jhon Dallas modification'
    w_frac = W_jd / m_jd
    p_frac = P_jd / m_jd
    f_frac = F_jd / m_jd
    return W_jd, P_jd, F_jd, m_jd, w_frac, p_frac, f_frac

pig_W_skin_jd, pig_P_skin_jd, pig_F_skin_jd, pig_m_skin_jd, pig_skin_w_frac_jd, pig_skin_p_frac_jd, pig_skin_f_frac_jd = apply_jd_modification(
    pig_W_skin_base, pig_P_skin_base, pig_F_skin_base, fat_reduction_factor_pig)
pig_W_fat_jd, pig_P_fat_jd, pig_F_fat_jd, pig_m_fat_jd, pig_fat_w_frac_jd, pig_fat_p_frac_jd, pig_fat_f_frac_jd = apply_jd_modification(
    pig_W_fat_base, pig_P_fat_base, pig_F_fat_base, fat_reduction_factor_pig)
pig_W_muscle_jd, pig_P_muscle_jd, pig_F_muscle_jd, pig_m_muscle_jd, pig_muscle_w_frac_jd, pig_muscle_p_frac_jd, pig_muscle_f_frac_jd = apply_jd_modification(
    pig_W_muscle_base, pig_P_muscle_base, pig_F_muscle_base, fat_reduction_factor_pig)

duck_W_skin = duck_W_skin_base
duck_P_skin = duck_P_skin_base
duck_F_skin = duck_F_skin_base
duck_W_fat = duck_W_fat_base
duck_P_fat = duck_P_fat_base
duck_F_fat = duck_F_fat_base
duck_W_muscle = duck_W_muscle_base
duck_P_muscle = duck_P_muscle_base
duck_F_muscle = duck_F_muscle_base

pig_C_skin_jd = pig_P_skin_jd * collagen_frac_skin
pig_C_fat_jd = pig_P_fat_jd * collagen_frac_fat
pig_C_muscle_jd = pig_P_muscle_jd * collagen_frac_muscle
duck_C_skin = duck_P_skin * collagen_frac_skin
duck_C_fat = duck_P_fat * collagen_frac_fat
duck_C_muscle = duck_P_muscle * collagen_frac_muscle

pig_P_nc_skin = pig_P_skin_jd - pig_C_skin_jd
pig_P_nc_fat = pig_P_fat_jd - pig_C_fat_jd
pig_P_nc_muscle = pig_P_muscle_jd - pig_C_muscle_jd
duck_P_nc_skin = duck_P_skin - duck_C_skin
duck_P_nc_fat = duck_P_fat - duck_C_fat
duck_P_nc_muscle = duck_P_muscle - duck_C_muscle

pig_P_nc_skin_density = pig_P_nc_skin / pig_skin_thickness_m
pig_P_nc_fat_density = pig_P_nc_fat / pig_fat_thickness_m
pig_P_nc_muscle_density = pig_P_nc_muscle / pig_muscle_thickness_m
duck_P_nc_skin_density = duck_P_nc_skin / duck_skin_thickness_m
duck_P_nc_fat_density = duck_P_nc_fat / duck_fat_thickness_m
duck_P_nc_muscle_density = duck_P_nc_muscle / duck_muscle_thickness_m

w_raw_skin_pig = pig_skin_w_frac_jd
w_raw_fat_pig = pig_fat_w_frac_jd
w_raw_muscle_pig = pig_muscle_w_frac_jd
w_raw_skin_duck = duck_conv_skin_comp['water']
w_raw_fat_duck = duck_conv_fat_comp['water']
w_raw_muscle_duck = duck_conv_muscle_comp['water']

k_skin = 0.45
k_fat = 0.20
k_muscle = 0.50
cp_skin = 3300.0
cp_fat = 2300.0
cp_muscle = 3500.0

# ----------------------------------------------------------------------
# Construcción de la malla y propiedades por especie
# ----------------------------------------------------------------------
def build_species_data(name, L_skin, L_fat, L_muscle,
                       rho_skin_val, rho_fat_val, rho_muscle_val,
                       k_skin_val, k_fat_val, k_muscle_val,
                       cp_skin_val, cp_fat_val, cp_muscle_val,
                       D_skin_val, D_fat_val, D_muscle_val,
                       w_raw_skin_val, w_raw_fat_val, w_raw_muscle_val,
                       P_nc_skin_density_val, P_nc_fat_density_val, P_nc_muscle_density_val,
                       Nx_base, min_nodes_layer, n_skin_val, n_fat_val):
    species = {}
    species['name'] = name
    L_total = L_skin + L_fat + L_muscle
    Nx_req_skin = int(math.ceil(1.0 + float(min_nodes_layer) * L_total / L_skin))
    Nx_req_fat = int(math.ceil(1.0 + float(min_nodes_layer) * L_total / L_fat))
    Nx_req_muscle = int(math.ceil(1.0 + float(min_nodes_layer) * L_total / L_muscle))
    Nx_s = max(Nx_base, Nx_req_skin, Nx_req_fat, Nx_req_muscle)
    dx_s = L_total / float(Nx_s - 1)
    x = np.linspace(0.0, L_total, Nx_s)
    mask_skin = x <= L_skin
    mask_fat = (x > L_skin) & (x <= L_skin + L_fat)
    mask_muscle = x > (L_skin + L_fat)
    assert np.any(mask_skin), 'No skin nodes for species ' + name
    assert np.any(mask_fat), 'No fat nodes for species ' + name
    assert np.any(mask_muscle), 'No muscle nodes for species ' + name
    rho_nodes = np.where(mask_skin, rho_skin_val, np.where(mask_fat, rho_fat_val, rho_muscle_val))
    cp_nodes = np.where(mask_skin, cp_skin_val, np.where(mask_fat, cp_fat_val, cp_muscle_val))
    k_nodes = np.where(mask_skin, k_skin_val, np.where(mask_fat, k_fat_val, k_muscle_val))
    alpha_nodes = k_nodes / (rho_nodes * cp_nodes)
    D_nodes = np.where(mask_skin, D_skin_val, np.where(mask_fat, D_fat_val, D_muscle_val))
    w_raw_nodes = np.where(mask_skin, w_raw_skin_val, np.where(mask_fat, w_raw_fat_val, w_raw_muscle_val))
    n_aw_nodes = np.where(mask_skin, n_skin_val, np.where(mask_fat, n_fat_val, 1.0))
    P_nc_density_nodes = np.where(mask_skin, P_nc_skin_density_val,
                                  np.where(mask_fat, P_nc_fat_density_val, P_nc_muscle_density_val))
    T_init = np.full(Nx_s, T_init_C, dtype=float)
    w_init = np.copy(w_raw_nodes)
    M_init = np.zeros(Nx_s, dtype=float)
    species['L_skin'] = L_skin
    species['L_fat'] = L_fat
    species['L_muscle'] = L_muscle
    species['L_total'] = L_total
    species['Nx'] = Nx_s
    species['dx'] = dx_s
    species['x'] = x
    species['mask_skin'] = mask_skin
    species['mask_fat'] = mask_fat
    species['mask_muscle'] = mask_muscle
    species['rho'] = rho_nodes
    species['cp'] = cp_nodes
    species['k'] = k_nodes
    species['alpha'] = alpha_nodes
    species['D_w'] = D_nodes
    species['w_raw'] = w_raw_nodes
    species['n_aw'] = n_aw_nodes
    species['P_nc_density'] = P_nc_density_nodes
    species['T'] = T_init
    species['T_new'] = np.copy(T_init)
    species['w'] = w_init
    species['w_new'] = np.copy(w_init)
    species['M'] = M_init
    species['init_recorded'] = False
    species['layer_init'] = {}
    species['layer_final'] = {}
    species['w_raw_skin'] = w_raw_skin_val
    W_initial = float(np.sum(rho_nodes * w_init * dx_s))
    species['W_initial'] = W_initial
    species['W_current'] = W_initial
    return species

min_nodes_layer = 5
species_data = {}
species_data['pig_JD'] = build_species_data(
    'pig_JD', pig_skin_thickness_m, pig_fat_thickness_m, pig_muscle_thickness_m,
    rho_skin, rho_fat, rho_muscle,
    k_skin, k_fat, k_muscle,
    cp_skin, cp_fat, cp_muscle,
    D_skin, D_fat, D_muscle,
    w_raw_skin_pig, w_raw_fat_pig, w_raw_muscle_pig,
    pig_P_nc_skin_density, pig_P_nc_fat_density, pig_P_nc_muscle_density,
    Nx_input, min_nodes_layer, n_skin, n_fat)
species_data['duck'] = build_species_data(
    'duck', duck_skin_thickness_m, duck_fat_thickness_m, duck_muscle_thickness_m,
    rho_skin, rho_fat, rho_muscle,
    k_skin, k_fat, k_muscle,
    cp_skin, cp_fat, cp_muscle,
    D_skin, D_fat, D_muscle,
    w_raw_skin_duck, w_raw_fat_duck, w_raw_muscle_duck,
    duck_P_nc_skin_density, duck_P_nc_fat_density, duck_P_nc_muscle_density,
    Nx_input, min_nodes_layer, n_skin, n_fat)

# ----------------------------------------------------------------------
# Cálculo del paso de tiempo estable (Fo_max_target = 0.2)
# ----------------------------------------------------------------------
dx_pig = species_data['pig_JD']['dx']
dx_duck = species_data['duck']['dx']
alpha_max_pig = float(np.max(species_data['pig_JD']['alpha']))
alpha_max_duck = float(np.max(species_data['duck']['alpha']))
D_max_pig = float(np.max(species_data['pig_JD']['D_w']))
D_max_duck = float(np.max(species_data['duck']['D_w']))

assert alpha_max_pig > 0.0 and alpha_max_duck > 0.0, 'Alpha must be positive'
assert D_max_pig > 0.0 and D_max_duck > 0.0, 'Moisture diffusivity must be positive'

dt_heat_max_pig = Fo_max_target * dx_pig * dx_pig / alpha_max_pig
dt_heat_max_duck = Fo_max_target * dx_duck * dx_duck / alpha_max_duck
dt_moist_max_pig = Fo_max_target * dx_pig * dx_pig / D_max_pig
dt_moist_max_duck = Fo_max_target * dx_duck * dx_duck / D_max_duck

dt_candidates = [dt_heat_max_pig, dt_heat_max_duck, dt_moist_max_pig, dt_moist_max_duck]
dt_min_candidate = min(dt_candidates)
assert dt_min_candidate > 0.0, 'Non positive dt candidate'
dt = 0.9 * dt_min_candidate

for key in species_data:
    spec = species_data[key]
    Fo_heat_nodes = spec['alpha'] * dt / (spec['dx'] * spec['dx'])
    Fo_moist_nodes = spec['D_w'] * dt / (spec['dx'] * spec['dx'])
    assert np.max(Fo_heat_nodes) <= Fo_max_target + 1e-9, 'Heat Fourier number exceeds target'
    assert np.max(Fo_moist_nodes) <= Fo_max_target + 1e-9, 'Moisture Fourier number exceeds target'
    spec['Fo_heat'] = Fo_heat_nodes
    spec['Fo_moist'] = Fo_moist_nodes

dt_hours = dt / 3600.0

msgLog = 'Executed: 40% - Species geometry, properties and stable time step computed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Construcción del perfil térmico (8 fases + rampas de 5 minutos)
# ----------------------------------------------------------------------
phases_list = []
phase1 = {'name': 'scalding', 'T_air_C': 100.0, 'RH_air': 1.0, 'duration_sec': 3.0 * 60.0, 'h_air': 1000.0}
phases_list.append(phase1)
phase2 = {'name': 'refrigerated_drying', 'T_air_C': 9.0, 'RH_air': 0.30, 'duration_sec': 48.0 * 3600.0, 'h_air': 10.0}
phases_list.append(phase2)
phase3 = {'name': 'steam', 'T_air_C': 75.0, 'RH_air': 1.0, 'duration_sec': 40.0 * 60.0, 'h_air': 1000.0}
phases_list.append(phase3)
phase4 = {'name': 'low_oven', 'T_air_C': 90.0, 'RH_air': 0.50, 'duration_sec': 60.0 * 60.0, 'h_air': 20.0}
phases_list.append(phase4)
phase5 = {'name': 'marking', 'T_air_C': 150.0, 'RH_air': 0.50, 'duration_sec': 30.0 * 60.0, 'h_air': 20.0}
phases_list.append(phase5)
phase6 = {'name': 'Maillard1', 'T_air_C': 230.0, 'RH_air': 0.0, 'duration_sec': 15.0 * 60.0, 'h_air': 20.0}
phases_list.append(phase6)
phase7 = {'name': 'Maillard2', 'T_air_C': 240.0, 'RH_air': 0.0, 'duration_sec': 5.0 * 60.0, 'h_air': 20.0}
phases_list.append(phase7)
phase8 = {'name': 'Maillard3', 'T_air_C': 250.0, 'RH_air': 0.0, 'duration_sec': 5.0 * 60.0, 'h_air': 20.0}
phases_list.append(phase8)

ramp_seconds = 5.0 * 60.0

t_list = []
T_air_list = []
RH_air_list = []
h_air_list = []
phase_label_list = []
phase_core_start_time = {}
phase_core_end_time = {}

t_current = 0.0
for idx_phase in range(len(phases_list)):
    phase = phases_list[idx_phase]
    if idx_phase == 0:
        phase_name = phase['name']
        phase_core_start_time[phase_name] = t_current
        duration = phase['duration_sec']
        N_core = int(max(1, round(duration / dt)))
        for i_step in range(N_core):
            t_list.append(t_current)
            T_air_list.append(phase['T_air_C'])
            RH_air_list.append(phase['RH_air'])
            h_air_list.append(phase['h_air'])
            phase_label_list.append(phase_name)
            t_current = t_current + dt
        phase_core_end_time[phase_name] = t_current
    else:
        prev_phase = phases_list[idx_phase - 1]
        ramp_label = 'ramp_to_' + phase['name']
        N_ramp = int(max(1, round(ramp_seconds / dt)))
        for i_step in range(N_ramp):
            frac = float(i_step + 1) / float(N_ramp)
            T_air_val = prev_phase['T_air_C'] + (phase['T_air_C'] - prev_phase['T_air_C']) * frac
            RH_air_val = prev_phase['RH_air'] + (phase['RH_air'] - prev_phase['RH_air']) * frac
            h_air_val = prev_phase['h_air'] + (phase['h_air'] - prev_phase['h_air']) * frac
            t_list.append(t_current)
            T_air_list.append(T_air_val)
            RH_air_list.append(RH_air_val)
            h_air_list.append(h_air_val)
            phase_label_list.append(ramp_label)
            t_current = t_current + dt
        phase_name = phase['name']
        phase_core_start_time[phase_name] = t_current
        duration = phase['duration_sec']
        N_core = int(max(1, round(duration / dt)))
        for i_step in range(N_core):
            t_list.append(t_current)
            T_air_list.append(phase['T_air_C'])
            RH_air_list.append(phase['RH_air'])
            h_air_list.append(phase['h_air'])
            phase_label_list.append(phase_name)
            t_current = t_current + dt
        phase_core_end_time[phase_name] = t_current

t_array = np.array(t_list, dtype=float)
T_air_array = np.array(T_air_list, dtype=float)
RH_air_array = np.array(RH_air_list, dtype=float)
h_air_array = np.array(h_air_list, dtype=float)
phase_labels = np.array(phase_label_list, dtype=object)
N_steps = t_array.size
assert N_steps == T_air_array.size == RH_air_array.size == h_air_array.size == phase_labels.size, 'Schedule arrays inconsistent lengths'
assert N_steps > 2, 'Too few time steps in schedule'

plateau_phase_names = ['scalding', 'refrigerated_drying', 'steam', 'low_oven', 'marking', 'Maillard1', 'Maillard2', 'Maillard3']
moisture_active_phases = ['refrigerated_drying', 'low_oven', 'marking', 'Maillard1', 'Maillard2', 'Maillard3']

record_stride = max(1, int(N_steps // 2000))
time_record_min = []
T_surf_record_pig = []
T_core_record_pig = []
W_loss_record_pig = []
w_skin_record_pig = []
w_fat_record_pig = []
w_muscle_record_pig = []
T_surf_record_duck = []
T_core_record_duck = []
W_loss_record_duck = []
w_skin_record_duck = []
w_fat_record_duck = []
w_muscle_record_duck = []
T_field_record_pig = []
w_field_record_pig = []
M_field_record_pig = []

phase_accum = {}
phase_accum['pig_JD'] = {}
phase_accum['duck'] = {}
for pname in plateau_phase_names:
    phase_accum['pig_JD'][pname] = {'T_surf_sum': 0.0, 'T_core_sum': 0.0, 'w_skin_sum': 0.0, 'count': 0, 'water_loss_end': 0.0}
    phase_accum['duck'][pname] = {'T_surf_sum': 0.0, 'T_core_sum': 0.0, 'w_skin_sum': 0.0, 'count': 0, 'water_loss_end': 0.0}

msgLog = 'Executed: 55% - Thermal schedule built for all eight phases and ramps'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Funciones de intensidad térmica (para el índice de Maillard)
# ----------------------------------------------------------------------
def triangular_intensity(T_array, T_min, T_opt, T_max):
    T_array = np.asarray(T_array, dtype=float)
    I = np.zeros_like(T_array)
    mask_rise = (T_array > T_min) & (T_array <= T_opt)
    mask_fall = (T_array > T_opt) & (T_array < T_max)
    I[mask_rise] = (T_array[mask_rise] - T_min) / (T_opt - T_min)
    I[mask_fall] = (T_max - T_array[mask_fall]) / (T_max - T_opt)
    I[(T_array <= T_min) | (T_array >= T_max)] = 0.0
    I = np.clip(I, 0.0, 1.0)
    return I

T_min_M = 120.0
T_opt_M = 155.0
T_max_M = 200.0

species_keys = ['pig_JD', 'duck']

progress_interval = max(1, int(N_steps // 4))

# ----------------------------------------------------------------------
# Bucle principal de simulación (diferencias finitas explícitas)
# ----------------------------------------------------------------------
for n in range(N_steps):
    T_air = T_air_array[n]
    RH_air = RH_air_array[n]
    h_air = h_air_array[n]
    label = phase_labels[n]

    # 1. Actualización de temperatura (convección superficial + conducción)
    for sp in species_keys:
        spec = species_data[sp]
        T = spec['T']
        T_new = spec['T_new']
        Fo_heat = spec['Fo_heat']
        dx_s = spec['dx']
        Nx_s = spec['Nx']
        k_nodes = spec['k']
        k0 = float(k_nodes[0])
        Bi0 = h_air * dx_s / k0
        T_new[0] = T[0] + 2.0 * Fo_heat[0] * (Bi0 * (T_air - T[0]) + T[1] - T[0])
        if Nx_s > 2:
            T_new[1:-1] = T[1:-1] + Fo_heat[1:-1] * (T[2:] - 2.0 * T[1:-1] + T[0:-2])
        T_new[-1] = T[-1] + 2.0 * Fo_heat[-1] * (T[-2] - T[-1])
        spec['T'] = T_new
        spec['T_new'] = T

    # 2. Actualización de humedad (solo en fases activas)
    for sp in species_keys:
        spec = species_data[sp]
        w = spec['w']
        w_new = spec['w_new']
        Fo_moist = spec['Fo_moist']
        dx_s = spec['dx']
        Nx_s = spec['Nx']
        w_raw_nodes = spec['w_raw']
        moisture_active = False
        if label in moisture_active_phases:
            moisture_active = True
        if moisture_active:
            w_surface_eq = spec['w_raw_skin'] * (RH_air ** (1.0 / n_skin))
            if w_surface_eq < 0.0:
                w_surface_eq = 0.0
            if w_surface_eq > spec['w_raw_skin']:
                w_surface_eq = spec['w_raw_skin']
            if w_surface_eq < w[0]:
                w_new[0] = w_surface_eq
            else:
                w_new[0] = w[0]
            if Nx_s > 2:
                w_new[1:-1] = w[1:-1] + Fo_moist[1:-1] * (w[2:] - 2.0 * w[1:-1] + w[0:-2])
            w_new[-1] = w[-1] + 2.0 * Fo_moist[-1] * (w[-2] - w[-1])
            w_new[w_new < 0.0] = 0.0
            mask_exceed = w_new > w_raw_nodes
            if np.any(mask_exceed):
                w_new[mask_exceed] = w_raw_nodes[mask_exceed]
            spec['w'] = w_new
            spec['w_new'] = w

    # 3. Acumulación del índice de Maillard (integral temporal)
    for sp in species_keys:
        spec = species_data[sp]
        T = spec['T']
        w = spec['w']
        w_raw_nodes = spec['w_raw']
        P_nc_density_nodes = spec['P_nc_density']
        d_local = 1.0 - w / w_raw_nodes
        d_local[d_local < 0.0] = 0.0
        d_local[d_local > 1.0] = 1.0
        f_T_local = triangular_intensity(T, T_min_M, T_opt_M, T_max_M)
        r_M = P_nc_density_nodes * (1.0 + d_local) * f_T_local
        spec['M'] = spec['M'] + r_M * dt_hours

    # 4. Registro del estado inicial (justo al final de la fase 'steam')
    if label == 'steam':
        for sp in species_keys:
            spec = species_data[sp]
            if not spec['init_recorded']:
                T_now = spec['T']
                w_now = spec['w']
                w_raw_nodes = spec['w_raw']
                n_aw_nodes = spec['n_aw']
                mask_skin = spec['mask_skin']
                mask_fat = spec['mask_fat']
                mask_muscle = spec['mask_muscle']
                T_skin_vals = T_now[mask_skin]
                T_fat_vals = T_now[mask_fat]
                T_muscle_vals = T_now[mask_muscle]
                w_skin_vals = w_now[mask_skin]
                w_fat_vals = w_now[mask_fat]
                w_muscle_vals = w_now[mask_muscle]
                w_raw_skin_nodes = w_raw_nodes[mask_skin]
                w_raw_fat_nodes = w_raw_nodes[mask_fat]
                w_raw_muscle_nodes = w_raw_nodes[mask_muscle]
                n_aw_skin = n_aw_nodes[mask_skin]
                n_aw_fat = n_aw_nodes[mask_fat]
                n_aw_muscle = n_aw_nodes[mask_muscle]
                aw_skin = (w_skin_vals / w_raw_skin_nodes) ** n_aw_skin
                aw_fat = (w_fat_vals / w_raw_fat_nodes) ** n_aw_fat
                aw_muscle = (w_muscle_vals / w_raw_muscle_nodes) ** n_aw_muscle
                aw_skin = np.clip(aw_skin, 0.0, 1.0)
                aw_fat = np.clip(aw_fat, 0.0, 1.0)
                aw_muscle = np.clip(aw_muscle, 0.0, 1.0)
                layer_init = {}
                layer_init['skin'] = {'T_mean': float(np.mean(T_skin_vals)), 'w_mean': float(np.mean(w_skin_vals)), 'aw_mean': float(np.mean(aw_skin))}
                layer_init['fat'] = {'T_mean': float(np.mean(T_fat_vals)), 'w_mean': float(np.mean(w_fat_vals)), 'aw_mean': float(np.mean(aw_fat))}
                layer_init['muscle'] = {'T_mean': float(np.mean(T_muscle_vals)), 'w_mean': float(np.mean(w_muscle_vals)), 'aw_mean': float(np.mean(aw_muscle))}
                spec['layer_init'] = layer_init
                spec['init_recorded'] = True

    # 5. Extracción de variables de salida en cada paso
    T_surf_current = {}
    T_core_current = {}
    w_skin_current = {}
    w_fat_current = {}
    w_muscle_current = {}
    W_loss_current = {}
    for sp in species_keys:
        spec = species_data[sp]
        T_now = spec['T']
        w_now = spec['w']
        mask_skin = spec['mask_skin']
        mask_fat = spec['mask_fat']
        mask_muscle = spec['mask_muscle']
        dx_s = spec['dx']
        rho_nodes = spec['rho']
        W_current = float(np.sum(rho_nodes * w_now * dx_s))
        W_initial = spec['W_initial']
        if W_initial > 0.0:
            W_loss_frac = 1.0 - W_current / W_initial
        else:
            W_loss_frac = 0.0
        spec['W_current'] = W_current
        T_surf_val = float(T_now[0])
        T_core_val = float(T_now[-1])
        w_skin_val = float(np.mean(w_now[mask_skin]))
        w_fat_val = float(np.mean(w_now[mask_fat]))
        w_muscle_val = float(np.mean(w_now[mask_muscle]))
        T_surf_current[sp] = T_surf_val
        T_core_current[sp] = T_core_val
        w_skin_current[sp] = w_skin_val
        w_fat_current[sp] = w_fat_val
        w_muscle_current[sp] = w_muscle_val
        W_loss_current[sp] = W_loss_frac

    # 6. Acumulación de promedios por fase (solo para fases plateau)
    if label in plateau_phase_names:
        for sp in species_keys:
            acc = phase_accum[sp][label]
            acc['T_surf_sum'] += T_surf_current[sp]
            acc['T_core_sum'] += T_core_current[sp]
            acc['w_skin_sum'] += w_skin_current[sp]
            acc['count'] += 1
            acc['water_loss_end'] = W_loss_current[sp]

    # 7. Registro para gráficos (cada 'record_stride' pasos)
    if (n % record_stride == 0) or (n == N_steps - 1):
        time_record_min.append(t_array[n] / 60.0)
        T_surf_record_pig.append(T_surf_current['pig_JD'])
        T_core_record_pig.append(T_core_current['pig_JD'])
        W_loss_record_pig.append(W_loss_current['pig_JD'])
        w_skin_record_pig.append(w_skin_current['pig_JD'])
        w_fat_record_pig.append(w_fat_current['pig_JD'])
        w_muscle_record_pig.append(w_muscle_current['pig_JD'])
        T_surf_record_duck.append(T_surf_current['duck'])
        T_core_record_duck.append(T_core_current['duck'])
        W_loss_record_duck.append(W_loss_current['duck'])
        w_skin_record_duck.append(w_skin_current['duck'])
        w_fat_record_duck.append(w_fat_current['duck'])
        w_muscle_record_duck.append(w_muscle_current['duck'])
        spec_pig = species_data['pig_JD']
        T_pig_now = np.array(spec_pig['T'], dtype=float)
        w_pig_now = np.array(spec_pig['w'], dtype=float)
        M_pig_now = np.array(spec_pig['M'], dtype=float)
        T_field_record_pig.append(T_pig_now)
        w_field_record_pig.append(w_pig_now)
        M_field_record_pig.append(M_pig_now)

    # 8. Mensaje de progreso cada cierto número de pasos
    if (n % progress_interval) == 0:
        frac = float(n) / float(N_steps)
        progress = 55.0 + 35.0 * frac
        if progress > 90.0:
            progress = 90.0
        msgLog = 'Executed: ' + str(int(progress)) + '% - Movement 3 PDE simulation in progress'
        send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

msgLog = 'Executed: 92% - PDE simulation completed, computing layer summaries and indices'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Cálculo de resúmenes finales por capa
# ----------------------------------------------------------------------
def compute_layer_final_summary(spec, J_min_val, J_max_val):
    T_final = spec['T']
    w_final = spec['w']
    w_raw_nodes = spec['w_raw']
    n_aw_nodes = spec['n_aw']
    M_final = spec['M']
    mask_skin = spec['mask_skin']
    mask_fat = spec['mask_fat']
    mask_muscle = spec['mask_muscle']
    f_melt_final = np.zeros_like(T_final)
    mask_mid = (T_final > 30.0) & (T_final < 45.0)
    mask_high = T_final >= 45.0
    f_melt_final[mask_mid] = (T_final[mask_mid] - 30.0) / 15.0
    f_melt_final[mask_high] = 1.0
    summaries = {}
    for layer_name in ['skin', 'fat', 'muscle']:
        if layer_name == 'skin':
            mask_layer = mask_skin
        elif layer_name == 'fat':
            mask_layer = mask_fat
        else:
            mask_layer = mask_muscle
        T_vals = T_final[mask_layer]
        w_vals = w_final[mask_layer]
        w_raw_vals = w_raw_nodes[mask_layer]
        n_aw_vals = n_aw_nodes[mask_layer]
        M_vals = M_final[mask_layer]
        f_melt_vals = f_melt_final[mask_layer]
        aw_vals = (w_vals / w_raw_vals) ** n_aw_vals
        aw_vals = np.clip(aw_vals, 0.0, 1.0)
        T_mean = float(np.mean(T_vals))
        w_mean = float(np.mean(w_vals))
        aw_mean = float(np.mean(aw_vals))
        f_melt_mean = float(np.mean(f_melt_vals))
        M_mean = float(np.mean(M_vals))
        M_max = float(np.max(M_vals))
        summaries[layer_name] = {'T_mean': T_mean, 'w_mean': w_mean, 'aw_mean': aw_mean,
                                 'f_melt_mean': f_melt_mean, 'M_mean': M_mean, 'M_max': M_max}
    return summaries

for sp in species_keys:
    assert species_data[sp]['init_recorded'], 'Initial steam state not recorded for species ' + sp

for sp in species_keys:
    spec = species_data[sp]
    layer_final = compute_layer_final_summary(spec, J_min, J_max)
    spec['layer_final'] = layer_final

pig_init = species_data['pig_JD']['layer_init']
duck_init = species_data['duck']['layer_init']
pig_final = species_data['pig_JD']['layer_final']
duck_final = species_data['duck']['layer_final']

w_skin_init_pig = pig_init['skin']['w_mean']
w_skin_final_pig = pig_final['skin']['w_mean']
M_skin_final_mean_pig = pig_final['skin']['M_mean']
M_skin_final_max_pig = pig_final['skin']['M_max']
w_muscle_init_pig = pig_init['muscle']['w_mean']
w_muscle_final_pig = pig_final['muscle']['w_mean']
f_melt_muscle_final_pig = pig_final['muscle']['f_melt_mean']

w_skin_init_duck = duck_init['skin']['w_mean']
w_skin_final_duck = duck_final['skin']['w_mean']
M_skin_final_mean_duck = duck_final['skin']['M_mean']
M_skin_final_max_duck = duck_final['skin']['M_max']
w_muscle_init_duck = duck_init['muscle']['w_mean']
w_muscle_final_duck = duck_final['muscle']['w_mean']
f_melt_muscle_final_duck = duck_final['muscle']['f_melt_mean']

# Índice de crocancia (C)
if w_skin_init_pig > 0.0:
    C_gross_pig = M_skin_final_mean_pig * (1.0 - w_skin_final_pig / w_skin_init_pig)
else:
    C_gross_pig = 0.0
if w_skin_init_duck > 0.0:
    C_gross_duck = M_skin_final_mean_duck * (1.0 - w_skin_final_duck / w_skin_init_duck)
else:
    C_gross_duck = 0.0

# Índice de jugosidad (J)
if w_muscle_init_pig > 0.0:
    J_gross_pig = (w_muscle_final_pig / w_muscle_init_pig) * f_melt_muscle_final_pig
else:
    J_gross_pig = 0.0
if w_muscle_init_duck > 0.0:
    J_gross_duck = (w_muscle_final_duck / w_muscle_init_duck) * f_melt_muscle_final_duck
else:
    J_gross_duck = 0.0

# Normalización de C (entre especies)
C_max = max(C_gross_pig, C_gross_duck)
if C_max > 0.0:
    C_norm_pig = C_gross_pig / C_max
    C_norm_duck = C_gross_duck / C_max
else:
    C_norm_pig = 0.5
    C_norm_duck = 0.5

def clip_zero_one(x):
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x

# Normalización de J (según rango especificado J_min, J_max)
J_norm_pig = 0.0
J_norm_duck = 0.0
if J_max > J_min:
    J_norm_pig = clip_zero_one((J_gross_pig - J_min) / (J_max - J_min))
    J_norm_duck = clip_zero_one((J_gross_duck - J_min) / (J_max - J_min))

# ----------------------------------------------------------------------
# Ensamblaje de tablas finales
# ----------------------------------------------------------------------
indices_rows = []
indices_rows.append({'species': 'pig_JD', 'C_gross': C_gross_pig, 'C_norm': C_norm_pig,
                     'J_gross': J_gross_pig, 'J_norm': J_norm_pig, 'R_norm': R_norm_pig})
indices_rows.append({'species': 'duck', 'C_gross': C_gross_duck, 'C_norm': C_norm_duck,
                     'J_gross': J_gross_duck, 'J_norm': J_norm_duck, 'R_norm': R_norm_duck})
final_indices_df = pd.DataFrame(indices_rows)

layer_summary_rows = []
for sp in species_keys:
    if sp == 'pig_JD':
        init = pig_init
        final = pig_final
    else:
        init = duck_init
        final = duck_final
    for layer_name in ['skin', 'fat', 'muscle']:
        row = {}
        row['species'] = sp
        row['layer'] = layer_name
        row['T_init_C'] = init[layer_name]['T_mean']
        row['T_final_C'] = final[layer_name]['T_mean']
        row['w_init'] = init[layer_name]['w_mean']
        row['w_final'] = final[layer_name]['w_mean']
        row['a_w_init'] = init[layer_name]['aw_mean']
        row['a_w_final'] = final[layer_name]['aw_mean']
        row['f_melt_final'] = final[layer_name]['f_melt_mean']
        row['M_final_avg'] = final[layer_name]['M_mean']
        row['M_final_max'] = final[layer_name]['M_max']
        layer_summary_rows.append(row)
layer_summary_df = pd.DataFrame(layer_summary_rows)

phase_summary_rows = []
for sp in species_keys:
    for pname in plateau_phase_names:
        acc = phase_accum[sp][pname]
        if acc['count'] > 0:
            T_surf_mean = acc['T_surf_sum'] / float(acc['count'])
            T_core_mean = acc['T_core_sum'] / float(acc['count'])
            w_skin_mean = acc['w_skin_sum'] / float(acc['count'])
        else:
            T_surf_mean = 0.0
            T_core_mean = 0.0
            w_skin_mean = 0.0
        row = {}
        row['species'] = sp
        row['phase'] = pname
        row['T_surface_mean_C'] = T_surf_mean
        row['T_core_mean_C'] = T_core_mean
        row['w_skin_mean'] = w_skin_mean
        row['water_loss_fraction_end'] = acc['water_loss_end']
        phase_summary_rows.append(row)
phase_summary_df = pd.DataFrame(phase_summary_rows)

assert not final_indices_df.isna().any().any(), 'Final indices table contains NaNs'
assert not layer_summary_df.isna().any().any(), 'Layer summary table contains NaNs'
assert not phase_summary_df.isna().any().any(), 'Phase summary table contains NaNs'

msgLog = 'Executed: 96% - Movement 3 tables assembled, generating Excel and figures'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Guardar Excel con las tres hojas
# ----------------------------------------------------------------------
movement3_excel_filename = 'movement3_tables_' + uuid.uuid4().hex + '.xlsx'
movement3_excel_path = os.path.join(files_path, movement3_excel_filename)
with pd.ExcelWriter(movement3_excel_path, engine='openpyxl') as writer:
    final_indices_df.to_excel(writer, sheet_name='Final_indices', index=False)
    layer_summary_df.to_excel(writer, sheet_name='Layer_summary', index=False)
    phase_summary_df.to_excel(writer, sheet_name='Phase_summary', index=False)

# ----------------------------------------------------------------------
# Preparación de datos para figuras
# ----------------------------------------------------------------------
time_record_arr = np.array(time_record_min, dtype=float)
T_surf_pig_arr = np.array(T_surf_record_pig, dtype=float)
T_core_pig_arr = np.array(T_core_record_pig, dtype=float)
T_surf_duck_arr = np.array(T_surf_record_duck, dtype=float)
T_core_duck_arr = np.array(T_core_record_duck, dtype=float)
W_loss_pig_arr = np.array(W_loss_record_pig, dtype=float)
W_loss_duck_arr = np.array(W_loss_record_duck, dtype=float)
w_skin_pig_arr = np.array(w_skin_record_pig, dtype=float)
w_fat_pig_arr = np.array(w_fat_record_pig, dtype=float)
w_muscle_pig_arr = np.array(w_muscle_record_pig, dtype=float)
w_skin_duck_arr = np.array(w_skin_record_duck, dtype=float)
w_fat_duck_arr = np.array(w_fat_record_duck, dtype=float)
w_muscle_duck_arr = np.array(w_muscle_record_duck, dtype=float)

T_field_pig_mat = np.array(T_field_record_pig, dtype=float)
w_field_pig_mat = np.array(w_field_record_pig, dtype=float)
M_field_pig_mat = np.array(M_field_record_pig, dtype=float)
x_pig = species_data['pig_JD']['x'] * 1000.0  # en mm

# ----------------------------------------------------------------------
# Generación de figuras
# ----------------------------------------------------------------------
images = []
captions = []

fig1, ax1 = plt.subplots(figsize=(10, 6))
ax1.plot(time_record_arr, T_surf_pig_arr, label='Pig Jhon Dallas surface', linewidth=1.2)
ax1.plot(time_record_arr, T_core_pig_arr, label='Pig Jhon Dallas core', linewidth=1.2)
ax1.plot(time_record_arr, T_surf_duck_arr, label='Duck surface', linewidth=1.2)
ax1.plot(time_record_arr, T_core_duck_arr, label='Duck core', linewidth=1.2)
for pname in plateau_phase_names:
    if pname in phase_core_start_time:
        t_line_min = phase_core_start_time[pname] / 60.0
        ax1.axvline(t_line_min, color='gray', linestyle='--', linewidth=0.6)
ax1.set_xlabel('Time (min)')
ax1.set_ylabel('Temperature (°C)')
ax1.set_title('Surface and core temperature evolution for pig Jhon Dallas and duck')
ax1.grid(True)
ax1.legend()
image_name1 = 'image_' + uuid.uuid4().hex + '.png'
fig1.tight_layout()
fig1.savefig(os.path.join(files_path, image_name1), bbox_inches='tight')
plt.close(fig1)
images.append(image_name1)
captions.append('Surface and core temperature versus time for pig Jhon Dallas and duck over the full eight phase schedule.')

fig2, ax2 = plt.subplots(figsize=(10, 6))
ax2.plot(time_record_arr, W_loss_pig_arr, label='Pig Jhon Dallas', linewidth=1.2)
ax2.plot(time_record_arr, W_loss_duck_arr, label='Duck', linewidth=1.2)
ax2.set_xlabel('Time (min)')
ax2.set_ylabel('Water loss fraction')
ax2.set_title('Global water loss over the process')
ax2.grid(True)
ax2.legend()
image_name2 = 'image_' + uuid.uuid4().hex + '.png'
fig2.tight_layout()
fig2.savefig(os.path.join(files_path, image_name2), bbox_inches='tight')
plt.close(fig2)
images.append(image_name2)
captions.append('Global water loss fraction versus time for pig Jhon Dallas and duck.')

fig3, (ax3a, ax3b) = plt.subplots(2, 1, figsize=(10, 10))
ax3a.plot(time_record_arr, w_skin_pig_arr, label='Skin', linewidth=1.2)
ax3a.plot(time_record_arr, w_fat_pig_arr, label='Fat', linewidth=1.2)
ax3a.plot(time_record_arr, w_muscle_pig_arr, label='Muscle', linewidth=1.2)
ax3a.set_xlabel('Time (min)')
ax3a.set_ylabel('Moisture fraction')
ax3a.set_title('Layer average moisture in pig Jhon Dallas')
ax3a.grid(True)
ax3a.legend()
ax3b.plot(time_record_arr, w_skin_duck_arr, label='Skin', linewidth=1.2)
ax3b.plot(time_record_arr, w_fat_duck_arr, label='Fat', linewidth=1.2)
ax3b.plot(time_record_arr, w_muscle_duck_arr, label='Muscle', linewidth=1.2)
ax3b.set_xlabel('Time (min)')
ax3b.set_ylabel('Moisture fraction')
ax3b.set_title('Layer average moisture in duck')
ax3b.grid(True)
ax3b.legend()
image_name3 = 'image_' + uuid.uuid4().hex + '.png'
fig3.tight_layout()
fig3.savefig(os.path.join(files_path, image_name3), bbox_inches='tight')
plt.close(fig3)
images.append(image_name3)
captions.append('Layer average moisture evolution for skin, fat and muscle in pig Jhon Dallas and duck.')

fig4, axes4 = plt.subplots(3, 1, figsize=(10, 12))
extent_T = [time_record_arr[0], time_record_arr[-1], x_pig[0], x_pig[-1]]
imT = axes4[0].imshow(T_field_pig_mat.T, aspect='auto', origin='lower', extent=extent_T, cmap='inferno')
axes4[0].set_ylabel('Depth (mm)')
axes4[0].set_title('Temperature field for pig Jhon Dallas')
fig4.colorbar(imT, ax=axes4[0])
imw = axes4[1].imshow(w_field_pig_mat.T, aspect='auto', origin='lower', extent=extent_T, cmap='Blues')
axes4[1].set_ylabel('Depth (mm)')
axes4[1].set_title('Moisture field for pig Jhon Dallas')
fig4.colorbar(imw, ax=axes4[1])
imM = axes4[2].imshow(M_field_pig_mat.T, aspect='auto', origin='lower', extent=extent_T, cmap='viridis')
axes4[2].set_xlabel('Time (min)')
axes4[2].set_ylabel('Depth (mm)')
axes4[2].set_title('Maillard index field for pig Jhon Dallas')
fig4.colorbar(imM, ax=axes4[2])
image_name4 = 'image_' + uuid.uuid4().hex + '.png'
fig4.tight_layout()
fig4.savefig(os.path.join(files_path, image_name4), bbox_inches='tight')
plt.close(fig4)
images.append(image_name4)
captions.append('Two dimensional fields of temperature, moisture and Maillard index for pig Jhon Dallas as functions of depth and time.')

fig5, ax5 = plt.subplots(figsize=(8, 6))
index_pos = np.arange(3)
width_bar = 0.35
vals_pig = [C_norm_pig, J_norm_pig, R_norm_pig]
vals_duck = [C_norm_duck, J_norm_duck, R_norm_duck]
ax5.bar(index_pos - width_bar/2.0, vals_pig, width_bar, label='Pig Jhon Dallas')
ax5.bar(index_pos + width_bar/2.0, vals_duck, width_bar, label='Duck')
ax5.set_xticks(index_pos)
ax5.set_xticklabels(['Crunchiness', 'Juiciness', 'Aroma'])
ax5.set_ylabel('Normalized index')
ax5.set_title('Normalized crunchiness, juiciness and aroma indices')
ax5.grid(True, axis='y')
ax5.legend()
image_name5 = 'image_' + uuid.uuid4().hex + '.png'
fig5.tight_layout()
fig5.savefig(os.path.join(files_path, image_name5), bbox_inches='tight')
plt.close(fig5)
images.append(image_name5)
captions.append('Comparison of normalized crunchiness, juiciness and surface aroma indices for pig Jhon Dallas and duck.')

# ----------------------------------------------------------------------
# Citas y notas finales
# ----------------------------------------------------------------------
citations = []
citations.append('Rahman, M.S. (2009). Food Properties Handbook, 2nd ed., CRC Press.')
citations.append('Rahman, M.S. and Labuza, T.P. (2007). Water activity and food preservation. In Handbook of Food Preservation, 2nd ed., CRC Press.')
citations.append('Mottram, D.S. (1998). Flavour formation in meat and meat products. Food Chemistry, 62(4): 415–424.')
citations.append('Nishimura, H. (1985). Role of intramuscular connective tissue in meat texture. Meat Science, 13(4): 195–215.')
citations.append('Igbeka, J.C. and Blaisdell, J.L. (1982). Moisture diffusivity in meat. Journal of Food Technology, 17: 451–460.')
citations.append('Choi, Y. and Okos, M.R. (1986). Effects of temperature and composition on the thermal properties of foods. In Food Engineering and Process Applications.')
citations.append('USDA FoodData Central entries for honey and soy sauce used previously in Movement 2 glaze modeling.')
citations.append('User Movement specifications PDFs: file-UAhKXSK3ckd6RptZt52yXz.pdf and file-HqQmVgTS2qy4UYsknv7cWK.pdf for aromatic families and Movement 3 PDE prototype.')

indices_summary = {}
indices_summary['pig_JD'] = {'C_gross': float(C_gross_pig), 'C_norm': float(C_norm_pig),
                             'J_gross': float(J_gross_pig), 'J_norm': float(J_norm_pig),
                             'R_norm': float(R_norm_pig)}
indices_summary['duck'] = {'C_gross': float(C_gross_duck), 'C_norm': float(C_norm_duck),
                           'J_gross': float(J_gross_duck), 'J_norm': float(J_norm_duck),
                           'R_norm': float(R_norm_duck)}

result = {}
result['status'] = 'ok'
result['description'] = ('Built a multilayer one dimensional physicochemical model for Peking style lacquered Jhon Dallas pig and Peking duck over all eight thermal phases, computing temperature, moisture, water activity, fat melting and Maillard index fields, and deriving normalized crunchiness, juiciness and aroma indices.')
metrics_dict = {}
metrics_dict['C_norm_pig_JD'] = float(round(C_norm_pig, 2))
metrics_dict['C_norm_duck'] = float(round(C_norm_duck, 2))
metrics_dict['J_norm_pig_JD'] = float(round(J_norm_pig, 2))
metrics_dict['J_norm_duck'] = float(round(J_norm_duck, 2))
metrics_dict['R_norm_pig_JD'] = float(round(R_norm_pig, 2))
metrics_dict['R_norm_duck'] = float(round(R_norm_duck, 2))
result['metrics'] = metrics_dict
tables_dict = {}
tables_dict['Final_indices'] = final_indices_df.to_dict(orient='records')
tables_dict['Layer_summary'] = layer_summary_df.to_dict(orient='records')
tables_dict['Phase_summary'] = phase_summary_df.to_dict(orient='records')
result['tables'] = tables_dict
result['images'] = images
result['caption'] = captions
result['files'] = [movement3_excel_filename]
notes = []
notes.append('Temperature and moisture were simulated with explicit finite difference schemes on a multilayer slab with stability enforced via a maximum Fourier number of 0.2.')
notes.append('Moisture exchange with air was only active during refrigerated drying and dry hot phases, using a Dirichlet skin boundary tied to relative humidity, while scalding and steam phases preserved moisture content.')
notes.append('Local Maillard index was integrated over time from non collagen protein density, a dryness factor and a triangular thermal activity window between 120 and 200 degrees Celsius.')
notes.append('Crunchiness and juiciness indices were referenced to the start of the steam phase and normalized across species, while surface aroma indices R_norm were imported from Movement 2 perfect results.')
result['notes'] = notes
result['indices'] = indices_summary
result['citations'] = citations

# ----------------------------------------------------------------------
# Salida JSON (opcional, puede ser capturada por el entorno)
# ----------------------------------------------------------------------
json_output = json.dumps(result, indent=2)
# Si se desea imprimir o retornar, se puede hacer aquí.
# Por ahora lo dejamos preparado.