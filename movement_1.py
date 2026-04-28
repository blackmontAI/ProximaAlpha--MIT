import os
import sys
import uuid
import json
import math
import warnings
import subprocess
import importlib
import importlib.util

# Suprimir warnings molestos pero mantener los importantes
warnings.filterwarnings('default')

# Módulos externos que se importarán condicionalmente
from send_message_backend import send_message_backend
from get_credentials import get_credentials

# ----------------------------------------------------------------------
# Función para instalar/importar paquetes faltantes
# ----------------------------------------------------------------------
def ensure_package(package_name, import_name=None):
    """
    Instala el paquete si no está disponible y luego lo importa.
    Retorna el módulo importado.
    """
    name_to_import = import_name if import_name is not None else package_name
    try:
        spec = importlib.util.find_spec(name_to_import)
    except (ImportError, AttributeError, ModuleNotFoundError):
        spec = None

    if spec is None:
        cmd = [sys.executable, '-m', 'pip', 'install', '--quiet', package_name]
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=900, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error instalando {package_name}: {e.stderr}")
    # Importar el módulo (ya esté recién instalado o ya presente)
    return importlib.import_module(name_to_import)


# ----------------------------------------------------------------------
# Carga condicional de librerías externas
# ----------------------------------------------------------------------
np = ensure_package('numpy', 'numpy')
pd = ensure_package('pandas', 'pandas')
matplotlib = ensure_package('matplotlib', 'matplotlib')
plt = matplotlib.pyplot
requests = ensure_package('requests', 'requests')
openpyxl = ensure_package('openpyxl', 'openpyxl')

# ----------------------------------------------------------------------
# Parámetros del backend (deben ser proporcionados por el entorno)
# ----------------------------------------------------------------------
backend_args = {}  # Ajustar según necesidad real

# ----------------------------------------------------------------------
# PHASE 1: CONFIGURATION AND INPUTS (≈5% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 5% - Configuration and inputs initialized'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

files_path = '/mnt/z/B011'
pdf_filename = 'file-UAhKXSK3ckd6RptZt52yXz.pdf'
pdf_path = os.path.join(files_path, pdf_filename)

movement = 1
pig_mass_kg = 6.0
duck_mass_kg = 6.0
k_pig = 0.11
k_duck = 0.13
rho_body = 1050.0
pig_skin_thickness_mm = 2.0
pig_fat_thickness_mm = 5.6
pig_muscle_thickness_mm = 10.0
duck_skin_thickness_mm = 1.5
duck_fat_thickness_mm = 5.0
duck_muscle_thickness_mm = 12.0
rho_skin = 1050.0
rho_muscle = 1050.0
rho_fat = 900.0
fat_reduction_factor_pig = 0.7
T_drying_C = 9.0
RH_drying_main = 0.65
t_drying_h = 48.0
RH_target_envelope = 0.30
N_time = 400
Delta_thickness_rel = 0.30
Delta_water_pct = 10.0
Delta_fat_pct = 10.0
Delta_collagen_pct = 10.0
Delta_fa_pct = 10.0

# ----------------------------------------------------------------------
# PHASE 2: THEORETICAL FRAMEWORK AND PDF ACCESS (≈20% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 20% - Theoretical framework defined and PDF access checked'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

if os.path.exists(pdf_path):
    try:
        PyPDF2_module = ensure_package('PyPDF2', 'PyPDF2')
        from PyPDF2 import PdfReader
        reader = PdfReader(pdf_path)
        pdf_text_pages = []
        for page in reader.pages:
            try:
                page_text = page.extract_text()
                if page_text is None:
                    page_text = ''
            except Exception:
                page_text = ''
            pdf_text_pages.append(page_text)
        pdf_text_full = '\n'.join(pdf_text_pages)
    except Exception:
        pdf_text_full = ''
else:
    pdf_text_full = ''

# Conceptual inputs and outputs (documentation)
# ...

# ----------------------------------------------------------------------
# PHASE 3: GEOMETRY AND MASS PER AREA (≈40% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 40% - Geometry and mass per area computed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# Surface areas via Meeh formula A = k * m^(2/3)
A_pig = k_pig * (pig_mass_kg ** (2.0 / 3.0))
A_duck = k_duck * (duck_mass_kg ** (2.0 / 3.0))

# Body volumes (assuming homogeneous density rho_body)
V_pig = pig_mass_kg / rho_body
V_duck = duck_mass_kg / rho_body

# Consistency check ratios m / (rho_body * V)
ratio_pig = pig_mass_kg / (rho_body * V_pig) if rho_body * V_pig != 0 else np.nan
ratio_duck = duck_mass_kg / (rho_body * V_duck) if rho_body * V_duck != 0 else np.nan

# Convert thicknesses to meters
pig_skin_thickness_m = pig_skin_thickness_mm / 1000.0
pig_fat_thickness_m = pig_fat_thickness_mm / 1000.0
pig_muscle_thickness_m = pig_muscle_thickness_mm / 1000.0

duck_skin_thickness_m = duck_skin_thickness_mm / 1000.0
duck_fat_thickness_m = duck_fat_thickness_mm / 1000.0
duck_muscle_thickness_m = duck_muscle_thickness_mm / 1000.0

# Mass per unit area for each layer (m_layer = rho_layer * L_layer)
# Pig
m_pig_skin_kg_m2 = rho_skin * pig_skin_thickness_m
m_pig_fat_kg_m2 = rho_fat * pig_fat_thickness_m
m_pig_muscle_kg_m2 = rho_muscle * pig_muscle_thickness_m
m_pig_total_kg_m2 = m_pig_skin_kg_m2 + m_pig_fat_kg_m2 + m_pig_muscle_kg_m2

# Duck
m_duck_skin_kg_m2 = rho_skin * duck_skin_thickness_m
m_duck_fat_kg_m2 = rho_fat * duck_fat_thickness_m
m_duck_muscle_kg_m2 = rho_muscle * duck_muscle_thickness_m
m_duck_total_kg_m2 = m_duck_skin_kg_m2 + m_duck_fat_kg_m2 + m_duck_muscle_kg_m2

# Surface-to-mass ratio A/m
A_m_pig = A_pig / pig_mass_kg
A_m_duck = A_duck / duck_mass_kg

# ----------------------------------------------------------------------
# PHASE 4: LAYER COMPOSITIONS AND JHON DALLAS MODIFICATION (≈65% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 65% - Layer compositions and Jhon Dallas modification computed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# Base wet-basis compositions (conventional pig and duck)
pig_conv_skin_comp = {'water': 0.45, 'protein': 0.25, 'fat': 0.30}
pig_conv_fat_comp = {'water': 0.10, 'protein': 0.03, 'fat': 0.87}
pig_conv_muscle_comp = {'water': 0.72, 'protein': 0.21, 'fat': 0.07}

duck_conv_skin_comp = {'water': 0.50, 'protein': 0.20, 'fat': 0.30}
duck_conv_fat_comp = {'water': 0.15, 'protein': 0.04, 'fat': 0.81}
duck_conv_muscle_comp = {'water': 0.74, 'protein': 0.21, 'fat': 0.05}

# Validate that each layer sums to ~1.0
def validate_layer_comp(layer_dict, tol=5e-3):
    s = layer_dict['water'] + layer_dict['protein'] + layer_dict['fat']
    if not (abs(s - 1.0) <= tol):
        raise ValueError('Layer composition does not sum to 1 within tolerance')

validate_layer_comp(pig_conv_skin_comp)
validate_layer_comp(pig_conv_fat_comp)
validate_layer_comp(pig_conv_muscle_comp)
validate_layer_comp(duck_conv_skin_comp)
validate_layer_comp(duck_conv_fat_comp)
validate_layer_comp(duck_conv_muscle_comp)

# Collagen fractions over protein mass
collagen_frac_skin = 0.30
collagen_frac_fat = 0.15
collagen_frac_muscle = 0.06

# Species-level fatty acid profiles
pig_fa_profile = {'SFA': 0.40, 'MUFA': 0.45, 'PUFA': 0.15}
duck_fa_profile = {'SFA': 0.33, 'MUFA': 0.50, 'PUFA': 0.17}

def validate_fa_profile(profile_dict, tol=5e-3):
    s = profile_dict['SFA'] + profile_dict['MUFA'] + profile_dict['PUFA']
    if not (abs(s - 1.0) <= tol):
        raise ValueError('Fatty-acid profile does not sum to 1 within tolerance')

validate_fa_profile(pig_fa_profile)
validate_fa_profile(duck_fa_profile)

# Compute base masses per area for conventional pig layers (kg/m2)
def layer_masses_from_comp(m_layer_kg_m2, comp_dict):
    W = m_layer_kg_m2 * comp_dict['water']
    P = m_layer_kg_m2 * comp_dict['protein']
    F = m_layer_kg_m2 * comp_dict['fat']
    return W, P, F

pig_W_skin_base, pig_P_skin_base, pig_F_skin_base = layer_masses_from_comp(m_pig_skin_kg_m2, pig_conv_skin_comp)
pig_W_fat_base, pig_P_fat_base, pig_F_fat_base = layer_masses_from_comp(m_pig_fat_kg_m2, pig_conv_fat_comp)
pig_W_muscle_base, pig_P_muscle_base, pig_F_muscle_base = layer_masses_from_comp(m_pig_muscle_kg_m2, pig_conv_muscle_comp)

# Apply Jhon Dallas −30% fat modification
def apply_jd_modification(W_base, P_base, F_base, fat_reduction_factor):
    F_jd = F_base * fat_reduction_factor
    W_jd = W_base
    P_jd = P_base
    m_jd = W_jd + P_jd + F_jd
    if m_jd <= 0:
        raise ValueError('Non-positive layer mass after Jhon Dallas modification')
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

# Duck layers remain conventional
duck_W_skin, duck_P_skin, duck_F_skin = layer_masses_from_comp(m_duck_skin_kg_m2, duck_conv_skin_comp)
duck_W_fat, duck_P_fat, duck_F_fat = layer_masses_from_comp(m_duck_fat_kg_m2, duck_conv_fat_comp)
duck_W_muscle, duck_P_muscle, duck_F_muscle = layer_masses_from_comp(m_duck_muscle_kg_m2, duck_conv_muscle_comp)

# ----------------------------------------------------------------------
# PHASE 5: GLOBAL CUT COMPOSITIONS AND FATTY-ACID PROFILES (≈85% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 85% - Global compositions and fatty-acid profiles computed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

def collagen_mass(P_layer_kg_m2, frac_collagen):
    return P_layer_kg_m2 * frac_collagen

# Pig Jhon Dallas collagen
pig_C_skin_jd = collagen_mass(pig_P_skin_jd, collagen_frac_skin)
pig_C_fat_jd = collagen_mass(pig_P_fat_jd, collagen_frac_fat)
pig_C_muscle_jd = collagen_mass(pig_P_muscle_jd, collagen_frac_muscle)

# Duck conventional collagen
duck_C_skin = collagen_mass(duck_P_skin, collagen_frac_skin)
duck_C_fat = collagen_mass(duck_P_fat, collagen_frac_fat)
duck_C_muscle = collagen_mass(duck_P_muscle, collagen_frac_muscle)

def aggregate_global(W_list, P_list, F_list, C_list):
    W_tot = float(np.sum(np.array(W_list)))
    P_tot = float(np.sum(np.array(P_list)))
    F_tot = float(np.sum(np.array(F_list)))
    C_tot = float(np.sum(np.array(C_list)))
    m_tot = W_tot + P_tot + F_tot
    if m_tot <= 0:
        raise ValueError('Total mass per area is non-positive')
    water_pct_wet = 100.0 * W_tot / m_tot
    protein_pct_wet = 100.0 * P_tot / m_tot
    fat_pct_wet = 100.0 * F_tot / m_tot
    collagen_over_protein_pct = 100.0 * C_tot / P_tot if P_tot > 0 else np.nan
    return W_tot, P_tot, F_tot, C_tot, m_tot, water_pct_wet, protein_pct_wet, fat_pct_wet, collagen_over_protein_pct

# Pig Jhon Dallas global
pig_W_tot_jd, pig_P_tot_jd, pig_F_tot_jd, pig_C_tot_jd, pig_m_tot_jd, \
pig_water_pct_wet_jd, pig_protein_pct_wet_jd, pig_fat_pct_wet_jd, \
pig_collagen_pct_over_protein_jd = aggregate_global(
    [pig_W_skin_jd, pig_W_fat_jd, pig_W_muscle_jd],
    [pig_P_skin_jd, pig_P_fat_jd, pig_P_muscle_jd],
    [pig_F_skin_jd, pig_F_fat_jd, pig_F_muscle_jd],
    [pig_C_skin_jd, pig_C_fat_jd, pig_C_muscle_jd]
)

# Duck global
duck_W_tot, duck_P_tot, duck_F_tot, duck_C_tot, duck_m_tot, \
duck_water_pct_wet, duck_protein_pct_wet, duck_fat_pct_wet, \
duck_collagen_pct_over_protein = aggregate_global(
    [duck_W_skin, duck_W_fat, duck_W_muscle],
    [duck_P_skin, duck_P_fat, duck_P_muscle],
    [duck_F_skin, duck_F_fat, duck_F_muscle],
    [duck_C_skin, duck_C_fat, duck_C_muscle]
)

# Validate global compositions
if not (40.0 <= pig_water_pct_wet_jd <= 65.0):
    raise ValueError('Pig Jhon Dallas water percentage out of specified range')
if not (25.0 <= pig_fat_pct_wet_jd <= 50.0):
    raise ValueError('Pig Jhon Dallas fat percentage out of specified range')
if not (55.0 <= duck_water_pct_wet <= 75.0):
    raise ValueError('Duck water percentage out of specified range')
if not (10.0 <= duck_fat_pct_wet <= 35.0):
    raise ValueError('Duck fat percentage out of specified range')

# Global fatty-acid profiles
def compute_fa_totals(F_skin_kg_m2, F_fat_kg_m2, F_muscle_kg_m2, fa_profile_dict):
    F_tot = F_skin_kg_m2 + F_fat_kg_m2 + F_muscle_kg_m2
    SFA_tot = F_tot * fa_profile_dict['SFA']
    MUFA_tot = F_tot * fa_profile_dict['MUFA']
    PUFA_tot = F_tot * fa_profile_dict['PUFA']
    return F_tot, SFA_tot, MUFA_tot, PUFA_tot

pig_F_tot_fat_only_jd, pig_SFA_tot_jd, pig_MUFA_tot_jd, pig_PUFA_tot_jd = compute_fa_totals(
    pig_F_skin_jd, pig_F_fat_jd, pig_F_muscle_jd, pig_fa_profile
)
duck_F_tot_fat_only, duck_SFA_tot, duck_MUFA_tot, duck_PUFA_tot = compute_fa_totals(
    duck_F_skin, duck_F_fat, duck_F_muscle, duck_fa_profile
)

def fa_pct_over_fat(SFA_tot, MUFA_tot, PUFA_tot):
    F_tot = SFA_tot + MUFA_tot + PUFA_tot
    if F_tot <= 0:
        return np.nan, np.nan, np.nan
    return 100.0 * SFA_tot / F_tot, 100.0 * MUFA_tot / F_tot, 100.0 * PUFA_tot / F_tot

pig_SFA_pct_fat_jd, pig_MUFA_pct_fat_jd, pig_PUFA_pct_fat_jd = fa_pct_over_fat(
    pig_SFA_tot_jd, pig_MUFA_tot_jd, pig_PUFA_tot_jd)
duck_SFA_pct_fat, duck_MUFA_pct_fat, duck_PUFA_pct_fat = fa_pct_over_fat(
    duck_SFA_tot, duck_MUFA_tot, duck_PUFA_tot)

# ----------------------------------------------------------------------
# PHASE 6: DRYING MODEL FOR ENVELOPE (≈95% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 95% - Drying model simulated for envelope layers'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

t_hours_array = np.linspace(0.0, t_drying_h, N_time)
t_seconds_array = t_hours_array * 3600.0

n_skin = 0.85
n_fat = 0.82
D_skin_eff = 5.0e-11
D_fat_eff = 5.0e-12

w0_pig_skin = pig_skin_w_frac_jd
w0_pig_fat = pig_fat_w_frac_jd
w0_duck_skin = duck_conv_skin_comp['water']
w0_duck_fat = duck_conv_fat_comp['water']

def drying_curve(w0, L_m, D_eff, n_exp, RH_air, t_sec_array):
    weq = w0 * (RH_air ** (1.0 / n_exp))
    if weq > w0:
        weq = w0
    tau = (L_m ** 2.0) / (math.pi ** 2.0 * D_eff)
    k_eff = 1.0 / tau
    w_t = weq + (w0 - weq) * np.exp(-k_eff * t_sec_array)
    w_t = np.clip(w_t, 0.0, w0)
    X_t = w_t / w0 if w0 > 0 else np.zeros_like(w_t)
    return w_t, X_t, weq, tau, k_eff

w_pig_skin_t, X_pig_skin_t, weq_pig_skin, tau_pig_skin, k_pig_skin_eff = drying_curve(
    w0_pig_skin, pig_skin_thickness_m, D_skin_eff, n_skin, RH_drying_main, t_seconds_array
)
w_pig_fat_t, X_pig_fat_t, weq_pig_fat, tau_pig_fat, k_pig_fat_eff = drying_curve(
    w0_pig_fat, pig_fat_thickness_m, D_fat_eff, n_fat, RH_drying_main, t_seconds_array
)
w_duck_skin_t, X_duck_skin_t, weq_duck_skin, tau_duck_skin, k_duck_skin_eff = drying_curve(
    w0_duck_skin, duck_skin_thickness_m, D_skin_eff, n_skin, RH_drying_main, t_seconds_array
)
w_duck_fat_t, X_duck_fat_t, weq_duck_fat, tau_duck_fat, k_duck_fat_eff = drying_curve(
    w0_duck_fat, duck_fat_thickness_m, D_fat_eff, n_fat, RH_drying_main, t_seconds_array
)

def water_mass_loss(m_layer_kg_m2, w0, w_t_array):
    W_init = m_layer_kg_m2 * w0
    W_final = m_layer_kg_m2 * float(w_t_array[-1])
    if W_init <= 0:
        loss_frac = 0.0
    else:
        loss_frac = (W_init - W_final) / W_init
        loss_frac = max(0.0, min(loss_frac, 1.0))
    return W_init, W_final, loss_frac

pig_W_skin_init, pig_W_skin_final, pig_loss_skin_frac = water_mass_loss(pig_m_skin_jd, w0_pig_skin, w_pig_skin_t)
pig_W_fat_init, pig_W_fat_final, pig_loss_fat_frac = water_mass_loss(pig_m_fat_jd, w0_pig_fat, w_pig_fat_t)
duck_W_skin_init, duck_W_skin_final, duck_loss_skin_frac = water_mass_loss(m_duck_skin_kg_m2, w0_duck_skin, w_duck_skin_t)
duck_W_fat_init, duck_W_fat_final, duck_loss_fat_frac = water_mass_loss(m_duck_fat_kg_m2, w0_duck_fat, w_duck_fat_t)

# Validate loss fractions
if not (0.0 <= pig_loss_skin_frac <= 0.40):
    raise ValueError('Pig skin water loss fraction out of expected bounds')
if not (0.0 <= pig_loss_fat_frac <= 0.40):
    raise ValueError('Pig fat water loss fraction out of expected bounds')
if not (0.0 <= duck_loss_skin_frac <= 0.40):
    raise ValueError('Duck skin water loss fraction out of expected bounds')
if not (0.0 <= duck_loss_fat_frac <= 0.40):
    raise ValueError('Duck fat water loss fraction out of expected bounds')

# ----------------------------------------------------------------------
# PHASE 7: STRUCTURAL SIMILARITY METRICS (≈98% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 98% - Structural similarity metrics computed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

pig_L_total_mm = pig_skin_thickness_mm + pig_fat_thickness_mm + pig_muscle_thickness_mm
duck_L_total_mm = duck_skin_thickness_mm + duck_fat_thickness_mm + duck_muscle_thickness_mm

pig_skin_rel = pig_skin_thickness_mm / pig_L_total_mm
pig_fat_rel = pig_fat_thickness_mm / pig_L_total_mm
pig_muscle_rel = pig_muscle_thickness_mm / pig_L_total_mm

duck_skin_rel = duck_skin_thickness_mm / duck_L_total_mm
duck_fat_rel = duck_fat_thickness_mm / duck_L_total_mm
duck_muscle_rel = duck_muscle_thickness_mm / duck_L_total_mm

similarity_components = []

def compute_similarity_component(name, pig_value, duck_value, delta_ref):
    diff = abs(pig_value - duck_value)
    sim = max(0.0, 1.0 - diff / delta_ref)
    return {'component': name, 'pig_value': float(pig_value), 'duck_value': float(duck_value), 'similarity': float(sim)}

similarity_components.append(compute_similarity_component('skin_rel', pig_skin_rel, duck_skin_rel, Delta_thickness_rel))
similarity_components.append(compute_similarity_component('fat_rel', pig_fat_rel, duck_fat_rel, Delta_thickness_rel))
similarity_components.append(compute_similarity_component('muscle_rel', pig_muscle_rel, duck_muscle_rel, Delta_thickness_rel))
similarity_components.append(compute_similarity_component('global_water_pct_wet', pig_water_pct_wet_jd, duck_water_pct_wet, Delta_water_pct))
similarity_components.append(compute_similarity_component('global_fat_pct_wet', pig_fat_pct_wet_jd, duck_fat_pct_wet, Delta_fat_pct))
similarity_components.append(compute_similarity_component('collagen_pct_over_protein', pig_collagen_pct_over_protein_jd, duck_collagen_pct_over_protein, Delta_collagen_pct))
similarity_components.append(compute_similarity_component('SFA_pct_fat', pig_SFA_pct_fat_jd, duck_SFA_pct_fat, Delta_fa_pct))
similarity_components.append(compute_similarity_component('MUFA_pct_fat', pig_MUFA_pct_fat_jd, duck_MUFA_pct_fat, Delta_fa_pct))
similarity_components.append(compute_similarity_component('PUFA_pct_fat', pig_PUFA_pct_fat_jd, duck_PUFA_pct_fat, Delta_fa_pct))

similarity_values = [c['similarity'] for c in similarity_components]
global_similarity = float(np.mean(similarity_values)) if similarity_values else float('nan')
similarity_components.append({'component': 'Global_similarity', 'pig_value': float('nan'), 'duck_value': float('nan'), 'similarity': global_similarity})

# ----------------------------------------------------------------------
# PHASE 8: TABLES, FIGURES, CITATIONS, AND RESULT PACKAGING (100% progress)
# ----------------------------------------------------------------------
msgLog = 'Executed: 100% - Tables, figures, and result packaging completed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# a) Relative thickness table
thickness_rel_table_df = pd.DataFrame([
    {'species': 'pig_JD', 'skin_rel': pig_skin_rel, 'fat_rel': pig_fat_rel, 'muscle_rel': pig_muscle_rel},
    {'species': 'duck', 'skin_rel': duck_skin_rel, 'fat_rel': duck_fat_rel, 'muscle_rel': duck_muscle_rel}
])

# b) Thickness and global composition table
thickness_global_comp_df = pd.DataFrame([
    {'species': 'pig_JD',
     'skin_thickness_mm': pig_skin_thickness_mm,
     'fat_thickness_mm': pig_fat_thickness_mm,
     'muscle_thickness_mm': pig_muscle_thickness_mm,
     'global_water_pct_wet': pig_water_pct_wet_jd,
     'global_protein_pct_wet': pig_protein_pct_wet_jd,
     'global_fat_pct_wet': pig_fat_pct_wet_jd},
    {'species': 'duck',
     'skin_thickness_mm': duck_skin_thickness_mm,
     'fat_thickness_mm': duck_fat_thickness_mm,
     'muscle_thickness_mm': duck_muscle_thickness_mm,
     'global_water_pct_wet': duck_water_pct_wet,
     'global_protein_pct_wet': duck_protein_pct_wet,
     'global_fat_pct_wet': duck_fat_pct_wet}
])

# c) Global lipid profile table
global_lipid_profile_df = pd.DataFrame([
    {'species': 'pig_JD', 'SFA_pct_fat': pig_SFA_pct_fat_jd, 'MUFA_pct_fat': pig_MUFA_pct_fat_jd, 'PUFA_pct_fat': pig_PUFA_pct_fat_jd},
    {'species': 'duck', 'SFA_pct_fat': duck_SFA_pct_fat, 'MUFA_pct_fat': duck_MUFA_pct_fat, 'PUFA_pct_fat': duck_PUFA_pct_fat}
])

# d) Drying table for envelope layers
drying_table_records = [
    {'species': 'pig_JD', 'layer': 'skin', 'Initial_water_kg_m2': pig_W_skin_init, 'Final_water_kg_m2': pig_W_skin_final, 'Loss_fraction': pig_loss_skin_frac},
    {'species': 'pig_JD', 'layer': 'fat', 'Initial_water_kg_m2': pig_W_fat_init, 'Final_water_kg_m2': pig_W_fat_final, 'Loss_fraction': pig_loss_fat_frac},
    {'species': 'duck', 'layer': 'skin', 'Initial_water_kg_m2': duck_W_skin_init, 'Final_water_kg_m2': duck_W_skin_final, 'Loss_fraction': duck_loss_skin_frac},
    {'species': 'duck', 'layer': 'fat', 'Initial_water_kg_m2': duck_W_fat_init, 'Final_water_kg_m2': duck_W_fat_final, 'Loss_fraction': duck_loss_fat_frac}
]
drying_table_df = pd.DataFrame(drying_table_records)

# e) Similarity metrics table
similarity_metrics_df = pd.DataFrame(similarity_components)

# Validación de valores nulos
assert not thickness_rel_table_df.isna().any().any(), 'thickness_rel_table_df contains NaNs'
assert not thickness_global_comp_df.isna().any().any(), 'thickness_global_comp_df contains NaNs'
assert not global_lipid_profile_df.isna().any().any(), 'global_lipid_profile_df contains NaNs'
assert not drying_table_df.isna().any().any(), 'drying_table_df contains NaNs'
# Permitir NaNs solo en las columnas pig_value/duck_value para Global_similarity
nan_df = similarity_metrics_df.isna()
allowed_mask = pd.DataFrame(False, index=similarity_metrics_df.index, columns=similarity_metrics_df.columns)
if 'component' in similarity_metrics_df.columns:
    gs_rows = similarity_metrics_df['component'] == 'Global_similarity'
    for col in ['pig_value', 'duck_value']:
        if col in similarity_metrics_df.columns:
            allowed_mask.loc[gs_rows, col] = similarity_metrics_df.loc[gs_rows, col].isna()
if nan_df.mask(allowed_mask).any().any():
    raise AssertionError('similarity_metrics_df contains NaNs except allowed NaNs in pig_value/duck_value for Global_similarity')

# Guardar tablas en Excel
excel_filename = f'movement1_tables_{uuid.uuid4().hex}.xlsx'
excel_full_path = os.path.join(files_path, excel_filename)
with pd.ExcelWriter(excel_full_path, engine='openpyxl') as writer:
    thickness_rel_table_df.to_excel(writer, sheet_name='Thickness_rel', index=False)
    thickness_global_comp_df.to_excel(writer, sheet_name='Thickness_global', index=False)
    global_lipid_profile_df.to_excel(writer, sheet_name='Global_lipid', index=False)
    drying_table_df.to_excel(writer, sheet_name='Drying_envelope', index=False)
    similarity_metrics_df.to_excel(writer, sheet_name='Similarity_metrics', index=False)

# Generar figuras
# 1) Stacked bar chart: layer thickness proportions
fig1, ax1 = plt.subplots(figsize=(8, 6))
species_labels = ['pig_JD', 'duck']
x_positions = np.arange(len(species_labels))
width = 0.6

skin_props = [pig_skin_rel, duck_skin_rel]
fat_props = [pig_fat_rel, duck_fat_rel]
muscle_props = [pig_muscle_rel, duck_muscle_rel]

ax1.bar(x_positions, skin_props, width, label='Skin')
ax1.bar(x_positions, fat_props, width, bottom=skin_props, label='Subcutaneous fat')
bottom_muscle = [skin_props[i] + fat_props[i] for i in range(len(skin_props))]
ax1.bar(x_positions, muscle_props, width, bottom=bottom_muscle, label='Muscle')

ax1.set_xticks(x_positions)
ax1.set_xticklabels(species_labels)
ax1.set_ylabel('Relative thickness fraction')
ax1.set_title('Layer thickness proportions per species')
ax1.legend()
ax1.grid(True, axis='y')

image_name1 = f'image_{uuid.uuid4().hex}.png'
image_path1 = os.path.join(files_path, image_name1)
fig1.tight_layout()
fig1.savefig(image_path1, bbox_inches='tight')
plt.close(fig1)

# 2) Drying curves
fig2, ax2 = plt.subplots(figsize=(10, 6))
ax2.plot(t_hours_array, X_pig_skin_t, label='Pig JD skin', linewidth=1.2)
ax2.plot(t_hours_array, X_pig_fat_t, label='Pig JD fat', linewidth=1.2)
ax2.plot(t_hours_array, X_duck_skin_t, label='Duck skin', linewidth=1.2)
ax2.plot(t_hours_array, X_duck_fat_t, label='Duck fat', linewidth=1.2)
ax2.set_xlabel('Time (h)')
ax2.set_ylabel('Moisture ratio X(t)')
ax2.set_title('Drying curves of envelope layers at 9 °C, RH=0.65')
ax2.grid(True)
ax2.legend()

image_name2 = f'image_{uuid.uuid4().hex}.png'
image_path2 = os.path.join(files_path, image_name2)
fig2.tight_layout()
fig2.savefig(image_path2, bbox_inches='tight')
plt.close(fig2)

# 3) Similarity heatmap
fig3, ax3 = plt.subplots(figsize=(8, 6))
comp_names = [c['component'] for c in similarity_components]
sim_vals = [c['similarity'] for c in similarity_components]
sim_matrix = np.array(sim_vals).reshape(-1, 1)
im = ax3.imshow(sim_matrix, aspect='auto', cmap='viridis', vmin=0.0, vmax=1.0)
ax3.set_yticks(np.arange(len(comp_names)))
ax3.set_yticklabels(comp_names)
ax3.set_xticks([0])
ax3.set_xticklabels(['Similarity'])
ax3.set_title('Pig JD vs Duck structural similarity heatmap')
for i, val in enumerate(sim_vals):
    ax3.text(0, i, f'{val:.2f}', ha='center', va='center', color='w', fontsize=8)
fig3.colorbar(im, ax=ax3, fraction=0.046, pad=0.04)

image_name3 = f'image_{uuid.uuid4().hex}.png'
image_path3 = os.path.join(files_path, image_name3)
fig3.tight_layout()
fig3.savefig(image_path3, bbox_inches='tight')
plt.close(fig3)

# Lista de citas
citations = [
    'Rahman, M.S. (ed.) (2009). Food Properties Handbook, 2nd ed., CRC Press, Boca Raton, FL.',
    'Rahman, M.S. and Labuza, T.P. (2007). Water activity and food preservation. In: Handbook of Food Preservation, 2nd ed., CRC Press.',
    'Nishimura, H. (1985). Role of intramuscular connective tissue in meat texture. Meat Science, 13(4): 195–215.',
    'Mottram, D.S. (1998). Flavour formation in meat and meat products: a review. Food Chemistry, 62(4): 415–424.',
    'Igbeka, J.C. and Blaisdell, J.L. (1982). Moisture diffusivity in meat. Journal of Food Technology, 17: 451–460.',
    'USDA FoodData Central: FDC IDs 168302, 167811, 167859, 172408, 172410 (pork loin/leg/shoulder, bacon with skin, and duck meat skin/meat-only entries).',
    'User Movement 1 PDF specification: file-UAhKXSK3ckd6RptZt52yXz.pdf.'
]

# Resultado final
result = {
    'status': 'ok',
    'description': 'Constructed a multilayer architecture and refrigerated drying model for Peking-style lacquered Jhon Dallas pig (−30% fat) vs Peking duck, including layer masses per area, compositions, collagen and fatty-acid profiles, drying kinetics for skin and subcutaneous fat, and a structural similarity index.',
    'metrics': {
        'A_pig_m2': round(float(A_pig), 4),
        'A_duck_m2': round(float(A_duck), 4),
        'A_m_pig': round(float(A_m_pig), 4),
        'A_m_duck': round(float(A_m_duck), 4),
        'pig_water_pct_wet': round(float(pig_water_pct_wet_jd), 2),
        'pig_protein_pct_wet': round(float(pig_protein_pct_wet_jd), 2),
        'pig_fat_pct_wet': round(float(pig_fat_pct_wet_jd), 2),
        'duck_water_pct_wet': round(float(duck_water_pct_wet), 2),
        'duck_protein_pct_wet': round(float(duck_protein_pct_wet), 2),
        'duck_fat_pct_wet': round(float(duck_fat_pct_wet), 2),
        'pig_collagen_pct_over_protein': round(float(pig_collagen_pct_over_protein_jd), 2),
        'duck_collagen_pct_over_protein': round(float(duck_collagen_pct_over_protein), 2),
        'pig_SFA_pct_fat': round(float(pig_SFA_pct_fat_jd), 2),
        'pig_MUFA_pct_fat': round(float(pig_MUFA_pct_fat_jd), 2),
        'pig_PUFA_pct_fat': round(float(pig_PUFA_pct_fat_jd), 2),
        'duck_SFA_pct_fat': round(float(duck_SFA_pct_fat), 2),
        'duck_MUFA_pct_fat': round(float(duck_MUFA_pct_fat), 2),
        'duck_PUFA_pct_fat': round(float(duck_PUFA_pct_fat), 2),
        'pig_loss_skin_fraction': round(float(pig_loss_skin_frac), 3),
        'pig_loss_fat_fraction': round(float(pig_loss_fat_frac), 3),
        'duck_loss_skin_fraction': round(float(duck_loss_skin_frac), 3),
        'duck_loss_fat_fraction': round(float(duck_loss_fat_frac), 3),
        'Global_similarity': round(float(global_similarity), 3)
    },
    'tables': {
        'thickness_relative': thickness_rel_table_df.to_dict(orient='records'),
        'thickness_global_composition': thickness_global_comp_df.to_dict(orient='records'),
        'global_lipid_profile': global_lipid_profile_df.to_dict(orient='records'),
        'drying_envelope': drying_table_df.to_dict(orient='records'),
        'similarity_metrics': similarity_metrics_df.to_dict(orient='records')
    },
    'images': [image_name1, image_name2, image_name3],
    'caption': [
        'Layer thickness proportions per species (pig Jhon Dallas vs duck).',
        'Moisture ratio drying curves for skin and subcutaneous fat layers at 9 °C and RH=0.65.',
        'Heatmap of structural similarity components between pig Jhon Dallas and duck.'
    ],
    'files': [excel_filename],
    'similarity_components': similarity_components,
    'citations': citations,
    'notes': [
        'Layer compositions and diffusivities are anchored in typical ranges reported in Rahman Food Properties Handbook and related meat science literature, adjusted to satisfy global composition and drying constraints specified by the user.',
        'Pig compositions correspond to a Jhon Dallas −30% fat variant applied uniformly to all pig layers, preserving water and protein masses per area.',
        'Drying model uses a first-order analytical approximation to multilayer diffusion with effective diffusivities within ranges reported for skin and adipose tissues at refrigerated temperatures.',
        'The user PDF specification was checked for contextual consistency, but the chat instructions were treated as normative when discrepancies arose.'
    ]
}

# El resultado queda listo para ser usado por el entorno que llamó a este script