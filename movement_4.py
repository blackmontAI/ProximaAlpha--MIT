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

files_path = '/mnt/z/B011'
species_name = 'pig_JD'
movement = '4_mario_fixed'
N_candidates = 400
Fo_max_target = 0.2

msgLog = 'Executed: 10% - Movement 4 Mario fixed configuration initialized for pig Jhon Dallas'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Parámetros y composiciones (reconstrucción desde Movement 1)
# ----------------------------------------------------------------------
rho_skin = 1050.0
rho_muscle = 1050.0
rho_fat = 900.0

L_skin = 2.0e-3
L_fat = 5.6e-3
L_muscle = 10.0e-3
L_total = L_skin + L_fat + L_muscle

pig_conv_skin_comp = {'water': 0.45, 'protein': 0.25, 'fat': 0.30}
pig_conv_fat_comp = {'water': 0.10, 'protein': 0.03, 'fat': 0.87}
pig_conv_muscle_comp = {'water': 0.72, 'protein': 0.21, 'fat': 0.07}

collagen_frac_skin = 0.30
collagen_frac_fat = 0.15
collagen_frac_muscle = 0.06
fat_reduction_factor_pig = 0.7

def validate_layer_comp(layer_dict, tol=5e-3):
    s = layer_dict['water'] + layer_dict['protein'] + layer_dict['fat']
    assert abs(s - 1.0) <= tol, 'Layer composition does not sum to 1 within tolerance'

validate_layer_comp(pig_conv_skin_comp)
validate_layer_comp(pig_conv_fat_comp)
validate_layer_comp(pig_conv_muscle_comp)

m_skin_kg_m2 = rho_skin * L_skin
m_fat_kg_m2 = rho_fat * L_fat
m_muscle_kg_m2 = rho_muscle * L_muscle

def layer_masses_from_comp(m_layer_kg_m2, comp_dict):
    W = m_layer_kg_m2 * comp_dict['water']
    P = m_layer_kg_m2 * comp_dict['protein']
    F = m_layer_kg_m2 * comp_dict['fat']
    return W, P, F

W_skin_base, P_skin_base, F_skin_base = layer_masses_from_comp(m_skin_kg_m2, pig_conv_skin_comp)
W_fat_base, P_fat_base, F_fat_base = layer_masses_from_comp(m_fat_kg_m2, pig_conv_fat_comp)
W_muscle_base, P_muscle_base, F_muscle_base = layer_masses_from_comp(m_muscle_kg_m2, pig_conv_muscle_comp)

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

W_skin_jd, P_skin_jd, F_skin_jd, m_skin_jd, _, _, _ = apply_jd_modification(
    W_skin_base, P_skin_base, F_skin_base, fat_reduction_factor_pig)
W_fat_jd, P_fat_jd, F_fat_jd, m_fat_jd, _, _, _ = apply_jd_modification(
    W_fat_base, P_fat_base, F_fat_base, fat_reduction_factor_pig)
W_muscle_jd, P_muscle_jd, F_muscle_jd, m_muscle_jd, _, _, _ = apply_jd_modification(
    W_muscle_base, P_muscle_base, F_muscle_base, fat_reduction_factor_pig)

C_skin_jd = P_skin_jd * collagen_frac_skin
C_fat_jd = P_fat_jd * collagen_frac_fat
C_muscle_jd = P_muscle_jd * collagen_frac_muscle

P_nc_skin = P_skin_jd - C_skin_jd
P_nc_fat = P_fat_jd - C_fat_jd
P_nc_muscle = P_muscle_jd - C_muscle_jd

P_nc_skin_density = P_nc_skin / L_skin
P_nc_fat_density = P_nc_fat / L_fat
P_nc_muscle_density = P_nc_muscle / L_muscle

w_raw_skin = 0.45
w_raw_fat = 0.10
w_raw_muscle = 0.72

k_skin = 0.45
k_fat = 0.20
k_muscle = 0.50
cp_skin = 3300.0
cp_fat = 2300.0
cp_muscle = 3500.0

D_skin = 1e-11
D_fat = 5e-12
D_muscle = 8e-11

n_skin = 0.85
n_fat = 0.85

# ----------------------------------------------------------------------
# Malla y propiedades por nodo
# ----------------------------------------------------------------------
Nx = 101
dx = L_total / float(Nx - 1)
x = np.linspace(0.0, L_total, Nx)

mask_skin = x <= L_skin
mask_fat = (x > L_skin) & (x <= L_skin + L_fat)
mask_muscle = x > (L_skin + L_fat)

assert np.any(mask_skin), 'No skin nodes'
assert np.any(mask_fat), 'No fat nodes'
assert np.any(mask_muscle), 'No muscle nodes'

rho_nodes = np.where(mask_skin, rho_skin, np.where(mask_fat, rho_fat, rho_muscle))
cp_nodes = np.where(mask_skin, cp_skin, np.where(mask_fat, cp_fat, cp_muscle))
k_nodes = np.where(mask_skin, k_skin, np.where(mask_fat, k_fat, k_muscle))
alpha_nodes = k_nodes / (rho_nodes * cp_nodes)
D_nodes = np.where(mask_skin, D_skin, np.where(mask_fat, D_fat, D_muscle))
w_raw_nodes = np.where(mask_skin, w_raw_skin, np.where(mask_fat, w_raw_fat, w_raw_muscle))
n_aw_nodes = np.where(mask_skin, n_skin, np.where(mask_fat, n_fat, 1.0))
P_nc_density_nodes = np.where(mask_skin, P_nc_skin_density,
                              np.where(mask_fat, P_nc_fat_density, P_nc_muscle_density))

alpha_max = float(np.max(alpha_nodes))
D_max = float(np.max(D_nodes))
assert alpha_max > 0.0, 'Alpha must be positive'
assert D_max > 0.0, 'Moisture diffusivity must be positive'

dt_heat_max = Fo_max_target * dx * dx / alpha_max
dt_moist_max = Fo_max_target * dx * dx / D_max
dt_candidate = min(dt_heat_max, dt_moist_max)
assert dt_candidate > 0.0, 'Non positive dt candidate'
dt = 0.9 * dt_candidate

Fo_heat_nodes = alpha_nodes * dt / (dx * dx)
Fo_moist_nodes = D_nodes * dt / (dx * dx)
assert np.max(Fo_heat_nodes) <= Fo_max_target + 1e-9, 'Heat Fourier number exceeds target'
assert np.max(Fo_moist_nodes) <= Fo_max_target + 1e-9, 'Moisture Fourier number exceeds target'

dt_hours = dt / 3600.0

msgLog = 'Executed: 25% - Geometry, properties and stable time step for Movement 4 Mario fixed computed'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Definición de las fases y construcción del schedule
# ----------------------------------------------------------------------
phase_names = ['steam', 'low_oven', 'marking', 'Maillard1', 'Maillard2', 'Maillard3']
T_phase_base = np.array([75.0, 90.0, 150.0, 230.0, 240.0, 250.0], dtype=float)
t_phase_base_h = np.array([40.0 / 60.0, 60.0 / 60.0, 30.0 / 60.0, 15.0 / 60.0, 5.0 / 60.0, 5.0 / 60.0], dtype=float)
RH_phase_base = np.array([1.0, 0.50, 0.50, 0.0, 0.0, 0.60], dtype=float)
h_phase_base = np.array([1000.0, 20.0, 20.0, 20.0, 20.0, 20.0], dtype=float)
t_dry2_base_days = 2.0
RH_phase2 = 0.30

T_min_M = 120.0
T_opt_M = 155.0
T_max_M = 200.0

J_min = 0.4
J_max = 1.0

ramp_seconds = 5.0 * 60.0

def build_schedule_for_candidate(T_phase, RH_phase, h_phase, t_phase_h, dt_val, ramp_sec):
    num_phases = len(T_phase)
    t_list = []
    T_list = []
    RH_list = []
    h_list = []
    t_current = 0.0
    for idx in range(num_phases):
        if idx == 0:
            duration_sec = float(t_phase_h[idx] * 3600.0)
            N_core = int(max(1, round(duration_sec / dt_val)))
            for i_step in range(N_core):
                t_list.append(t_current)
                T_list.append(float(T_phase[idx]))
                RH_list.append(float(RH_phase[idx]))
                h_list.append(float(h_phase[idx]))
                t_current = t_current + dt_val
        else:
            duration_sec = float(t_phase_h[idx] * 3600.0)
            N_ramp = int(max(1, round(ramp_sec / dt_val)))
            for i_step in range(N_ramp):
                frac = float(i_step + 1) / float(N_ramp)
                T_val = float(T_phase[idx - 1] + (T_phase[idx] - T_phase[idx - 1]) * frac)
                RH_val = float(RH_phase[idx - 1] + (RH_phase[idx] - RH_phase[idx - 1]) * frac)
                h_val = float(h_phase[idx - 1] + (h_phase[idx] - h_phase[idx - 1]) * frac)
                t_list.append(t_current)
                T_list.append(T_val)
                RH_list.append(RH_val)
                h_list.append(h_val)
                t_current = t_current + dt_val
            N_core = int(max(1, round(duration_sec / dt_val)))
            for i_step in range(N_core):
                t_list.append(t_current)
                T_list.append(float(T_phase[idx]))
                RH_list.append(float(RH_phase[idx]))
                h_list.append(float(h_phase[idx]))
                t_current = t_current + dt_val
    t_array = np.array(t_list, dtype=float)
    T_air_array = np.array(T_list, dtype=float)
    RH_air_array = np.array(RH_list, dtype=float)
    h_air_array = np.array(h_list, dtype=float)
    assert t_array.size == T_air_array.size == RH_air_array.size == h_air_array.size, 'Schedule arrays mismatch'
    assert t_array.size > 2, 'Too few time steps in schedule'
    return t_array, T_air_array, RH_air_array, h_air_array

msgLog = 'Executed: 30% - Baseline Movement 4 schedule templates and Mario target defined'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Preparación de arrays para la simulación
# ----------------------------------------------------------------------
rng = np.random.default_rng(12345)

T = np.full(Nx, 9.0, dtype=float)
T_new = np.copy(T)
w = np.where(mask_skin, w_raw_skin, np.where(mask_fat, w_raw_fat, w_raw_muscle))
w_new = np.copy(w)
M = np.zeros(Nx, dtype=float)
d_local = np.zeros(Nx, dtype=float)
f_T_local = np.zeros(Nx, dtype=float)
f_melt = np.zeros(Nx, dtype=float)
r_M = np.zeros(Nx, dtype=float)

C_gross_arr = np.zeros(N_candidates, dtype=float)
J_gross_arr = np.zeros(N_candidates, dtype=float)
R_raw_arr = np.zeros(N_candidates, dtype=float)

candidate_records = []

progress_interval_candidates = max(1, N_candidates // 10)

msgLog = 'Executed: 35% - Starting Movement 4 Mario fixed Monte Carlo PDE simulations'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Bucle principal de Monte Carlo
# ----------------------------------------------------------------------
for idx_cand in range(N_candidates):
    if idx_cand == 0:
        t_dry2_days = t_dry2_base_days
        T_phase = T_phase_base.copy()
        t_phase_h = t_phase_base_h.copy()
        RH_phase = RH_phase_base.copy()
        h_phase = h_phase_base.copy()
        is_reference = True
        is_mario = False
    elif idx_cand == 1:
        t_dry2_days = 6.0
        T_phase = np.array([80.0, 90.0, 130.0, 230.0, 240.0, 250.0], dtype=float)
        t_phase_h = np.array([0.5, 1.0, 25.0 / 60.0, 25.0 / 60.0, 5.0 / 60.0, 5.0 / 60.0], dtype=float)
        RH_phase = np.array([1.0, 0.50, 0.50, 0.0, 0.0, 0.0], dtype=float)
        h_phase = np.array([1000.0, 20.0, 20.0, 20.0, 20.0, 20.0], dtype=float)
        is_reference = False
        is_mario = True
    else:
        t_dry2_days = float(2.0 + rng.random() * (7.0 - 2.0))
        T_phase = T_phase_base + rng.uniform(-25.0, 25.0, size=T_phase_base.shape)
        T_phase = np.clip(T_phase, 70.0, 260.0)
        t_phase_h = t_phase_base_h * (1.0 + rng.uniform(-0.5, 0.5, size=t_phase_base_h.shape))
        t_phase_h = np.maximum(t_phase_h, 0.1 * t_phase_base_h)
        RH_phase = np.zeros_like(RH_phase_base)
        RH_phase[0] = 1.0
        RH_phase[1] = float(rng.uniform(0.30, 0.70))
        RH_phase[2] = float(rng.uniform(0.30, 0.70))
        RH_phase[3] = 0.0
        RH_phase[4] = 0.0
        RH_phase[5] = 0.60
        h_phase = np.array([1000.0, 20.0, 20.0, 20.0, 20.0, 20.0], dtype=float)
        is_reference = False
        is_mario = False

    # Secado analítico en fase 2 (refrigerado)
    t2_sec = float(t_dry2_days * 24.0 * 3600.0)
    k_eff_skin = D_skin * (math.pi ** 2) / (L_skin ** 2)
    k_eff_fat = D_fat * (math.pi ** 2) / (L_fat ** 2)
    w0_skin = w_raw_skin
    w0_fat = w_raw_fat
    w_eq_skin = w0_skin * (RH_phase2 ** (1.0 / n_skin))
    w_eq_fat = w0_fat * (RH_phase2 ** (1.0 / n_fat))
    w_eq_skin = max(0.0, min(w_eq_skin, w0_skin))
    w_eq_fat = max(0.0, min(w_eq_fat, w0_fat))
    w2_skin = w_eq_skin + (w0_skin - w_eq_skin) * math.exp(-k_eff_skin * t2_sec)
    w2_fat = w_eq_fat + (w0_fat - w_eq_fat) * math.exp(-k_eff_fat * t2_sec)
    if w2_skin < 0.0:
        w2_skin = 0.0
    if w2_skin > w0_skin:
        w2_skin = w0_skin
    if w2_fat < 0.0:
        w2_fat = 0.0
    if w2_fat > w0_fat:
        w2_fat = w0_fat

    # Inicialización para la simulación de las fases 3-8
    T[:] = 9.0
    w[mask_skin] = w2_skin
    w[mask_fat] = w2_fat
    w[mask_muscle] = w_raw_muscle
    M[:] = 0.0
    w_skin_init = float(np.mean(w[mask_skin]))
    w_muscle_init = float(np.mean(w[mask_muscle]))
    eps_small = 1e-12

    t_array, T_air_array, RH_air_array, h_air_array = build_schedule_for_candidate(
        T_phase, RH_phase, h_phase, t_phase_h, dt, ramp_seconds)
    N_steps_sched = t_array.size

    R_sum = 0.0
    R_time = 0.0

    for n in range(N_steps_sched):
        T_air = T_air_array[n]
        RH_air = RH_air_array[n]
        h_air = h_air_array[n]

        # Actualización de temperatura
        k0 = float(k_nodes[0])
        Bi0 = h_air * dx / k0
        T_new[0] = T[0] + 2.0 * Fo_heat_nodes[0] * (Bi0 * (T_air - T[0]) + T[1] - T[0])
        if Nx > 2:
            T_new[1:-1] = T[1:-1] + Fo_heat_nodes[1:-1] * (T[2:] - 2.0 * T[1:-1] + T[0:-2])
        T_new[-1] = T[-1] + 2.0 * Fo_heat_nodes[-1] * (T[-2] - T[-1])
        T, T_new = T_new, T

        # Actualización de humedad
        if RH_air >= 1.0:
            w_new[0] = min(w[0], w_raw_skin)
        else:
            w_surface_eq = w_raw_skin * (RH_air ** (1.0 / n_skin))
            if w_surface_eq < 0.0:
                w_surface_eq = 0.0
            if w_surface_eq > w_raw_skin:
                w_surface_eq = w_raw_skin
            if w_surface_eq < w[0]:
                w_new[0] = w_surface_eq
            else:
                w_new[0] = w[0]
        if Nx > 2:
            w_new[1:-1] = w[1:-1] + Fo_moist_nodes[1:-1] * (w[2:] - 2.0 * w[1:-1] + w[0:-2])
        w_new[-1] = w[-1] + 2.0 * Fo_moist_nodes[-1] * (w[-2] - w[-1])
        w_new[w_new < 0.0] = 0.0
        mask_exceed = w_new > w_raw_nodes
        if np.any(mask_exceed):
            w_new[mask_exceed] = w_raw_nodes[mask_exceed]
        w, w_new = w_new, w

        # Cálculo de d_local y f_T_local
        np.divide(w, w_raw_nodes, out=d_local)
        d_local[:] = 1.0 - d_local
        np.clip(d_local, 0.0, 1.0, out=d_local)

        f_T_local[:] = 0.0
        mask_rise = (T > T_min_M) & (T <= T_opt_M)
        if np.any(mask_rise):
            f_T_local[mask_rise] = (T[mask_rise] - T_min_M) / (T_opt_M - T_min_M)
        mask_fall = (T > T_opt_M) & (T < T_max_M)
        if np.any(mask_fall):
            f_T_local[mask_fall] = (T_max_M - T[mask_fall]) / (T_max_M - T_opt_M)

        # Acumulación de índice de Maillard
        r_M[:] = P_nc_density_nodes * (1.0 + d_local) * f_T_local
        M = M + r_M * dt_hours

        # Acumulación del roast risk (desviación cuadrática de la temperatura del centro respecto a 70°C)
        T_core = float(T[-1])
        diff_core = T_core - 70.0
        R_sum = R_sum + diff_core * diff_core * dt
        R_time = R_time + dt

        assert np.isfinite(T).all(), 'Temperature field contains non finite values'
        assert np.isfinite(w).all(), 'Moisture field contains non finite values'
        assert np.isfinite(M).all(), 'Maillard field contains non finite values'

    # Cálculo de índices finales para este candidato
    w_skin_final = float(np.mean(w[mask_skin]))
    w_muscle_final = float(np.mean(w[mask_muscle]))

    f_melt[:] = 0.0
    mask_mid = (T > 30.0) & (T < 45.0)
    mask_high = T >= 45.0
    if np.any(mask_mid):
        f_melt[mask_mid] = (T[mask_mid] - 30.0) / 15.0
    if np.any(mask_high):
        f_melt[mask_high] = 1.0
    f_melt_muscle_final = float(np.mean(f_melt[mask_muscle]))

    M_skin_final = float(np.mean(M[mask_skin]))

    if w_skin_init > eps_small:
        C_gross = M_skin_final * (1.0 - w_skin_final / w_skin_init)
    else:
        C_gross = 0.0

    if w_muscle_init > eps_small:
        J_gross = (w_muscle_final / w_muscle_init) * f_melt_muscle_final
    else:
        J_gross = 0.0

    if R_time > eps_small:
        R_raw = R_sum / R_time
    else:
        R_raw = 0.0

    C_gross_arr[idx_cand] = C_gross
    J_gross_arr[idx_cand] = J_gross
    R_raw_arr[idx_cand] = R_raw

    # Guardar registro del candidato
    rec = {}
    rec['id'] = int(idx_cand)
    rec['is_reference'] = bool(is_reference)
    rec['is_mario'] = bool(is_mario)
    rec['is_pareto'] = False
    rec['is_experiment'] = False
    rec['t_dry2_days'] = float(t_dry2_days)
    rec['T3'] = float(T_phase[0])
    rec['T4'] = float(T_phase[1])
    rec['T5'] = float(T_phase[2])
    rec['T6'] = float(T_phase[3])
    rec['T7'] = float(T_phase[4])
    rec['T8'] = float(T_phase[5])
    rec['t3_h'] = float(t_phase_h[0])
    rec['t4_h'] = float(t_phase_h[1])
    rec['t5_h'] = float(t_phase_h[2])
    rec['t6_h'] = float(t_phase_h[3])
    rec['t7_h'] = float(t_phase_h[4])
    rec['t8_h'] = float(t_phase_h[5])
    rec['RH3'] = float(RH_phase[0])
    rec['RH4'] = float(RH_phase[1])
    rec['RH5'] = float(RH_phase[2])
    rec['RH6'] = float(RH_phase[3])
    rec['RH7'] = float(RH_phase[4])
    rec['RH8'] = float(RH_phase[5])
    rec['C_gross'] = float(C_gross)
    rec['C_norm'] = 0.0
    rec['J_gross'] = float(J_gross)
    rec['J_norm'] = 0.0
    rec['R_raw'] = float(R_raw)
    rec['R_norm'] = 0.0
    candidate_records.append(rec)

    if ((idx_cand + 1) % progress_interval_candidates) == 0 or (idx_cand == N_candidates - 1):
        frac_global = float(idx_cand + 1) / float(N_candidates)
        progress = 35.0 + 55.0 * frac_global
        if progress > 90.0:
            progress = 90.0
        msgLog = 'Executed: ' + str(int(progress)) + '% - Movement 4 Mario fixed Monte Carlo PDE simulations progressing'
        send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

msgLog = 'Executed: 92% - Movement 4 Mario fixed simulations completed, computing indices and Pareto front'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Normalización de los tres índices
# ----------------------------------------------------------------------
C_gross_arr_np = np.asarray(C_gross_arr, dtype=float)
J_gross_arr_np = np.asarray(J_gross_arr, dtype=float)
R_raw_arr_np = np.asarray(R_raw_arr, dtype=float)

eps_norm = 1e-12

C_min = float(np.min(C_gross_arr_np))
C_max = float(np.max(C_gross_arr_np))
C_den = C_max - C_min
C_norm_arr = (C_gross_arr_np - C_min) / (C_den + eps_norm)
C_norm_arr = np.clip(C_norm_arr, 0.0, 1.0)

J_den = J_max - J_min
J_norm_arr = (J_gross_arr_np - J_min) / (J_den + eps_norm)
J_norm_arr = np.clip(J_norm_arr, 0.0, 1.0)

R_min = float(np.min(R_raw_arr_np))
R_max = float(np.max(R_raw_arr_np))
R_den = R_max - R_min
R_norm_arr = (R_max - R_raw_arr_np) / (R_den + eps_norm)
R_norm_arr = np.clip(R_norm_arr, 0.0, 1.0)

# ----------------------------------------------------------------------
# Cálculo del frente de Pareto (maximizar C, J y R_norm)
# ----------------------------------------------------------------------
N_tot = N_candidates
is_pareto = np.ones(N_tot, dtype=bool)
for i in range(N_tot):
    if not is_pareto[i]:
        continue
    Ci = C_norm_arr[i]
    Ji = J_norm_arr[i]
    Ri = R_norm_arr[i]
    for j in range(N_tot):
        if j == i:
            continue
        if not is_pareto[i]:
            break
        Cj = C_norm_arr[j]
        Jj = J_norm_arr[j]
        Rj = R_norm_arr[j]
        if (Cj >= Ci and Jj >= Ji and Rj >= Ri) and (Cj > Ci or Jj > Ji or Rj > Ri):
            is_pareto[i] = False
            break

pareto_indices = np.where(is_pareto)[0]
N_pareto = int(pareto_indices.size)

Phi_arr = C_norm_arr + J_norm_arr + R_norm_arr
mario_index = 1   # el candidato con id=1 es el schedule de Mario

# Selección de 15 experimentos: primero a lo largo del Pareto por C, luego por Phi
pareto_sorted_by_C = pareto_indices[np.argsort(C_norm_arr[pareto_indices])]
experiments_indices_set = set()
if N_pareto >= 15:
    for k in range(15):
        pos = int(round(float(k) * float(N_pareto - 1) / 14.0))
        if pos < 0:
            pos = 0
        if pos >= N_pareto:
            pos = N_pareto - 1
        experiments_indices_set.add(int(pareto_sorted_by_C[pos]))
else:
    for idx in pareto_sorted_by_C:
        experiments_indices_set.add(int(idx))
    if len(experiments_indices_set) < 15:
        all_indices_sorted = np.argsort(-Phi_arr)
        for idx in all_indices_sorted:
            if len(experiments_indices_set) >= 15:
                break
            if int(idx) not in experiments_indices_set:
                experiments_indices_set.add(int(idx))

experiments_indices = sorted(list(experiments_indices_set))
if len(experiments_indices) > 15:
    experiments_indices = experiments_indices[:15]

# Asegurar que Mario (id=1) esté incluido
if mario_index not in experiments_indices:
    if len(experiments_indices) < 15:
        experiments_indices.append(mario_index)
    else:
        candidates_current = list(experiments_indices)
        C_subset = np.array([C_norm_arr[i] for i in candidates_current], dtype=float)
        idx_closest_local = int(np.argmin(np.abs(C_subset - C_norm_arr[mario_index])))
        candidates_current[idx_closest_local] = mario_index
        experiments_indices = sorted(candidates_current)

# Actualizar los registros con los valores normalizados y flags
for i in range(N_tot):
    candidate_records[i]['C_norm'] = float(C_norm_arr[i])
    candidate_records[i]['J_norm'] = float(J_norm_arr[i])
    candidate_records[i]['R_norm'] = float(R_norm_arr[i])
    candidate_records[i]['is_pareto'] = bool(is_pareto[i])
    candidate_records[i]['is_experiment'] = bool(i in experiments_indices_set)
    candidate_records[i]['is_mario'] = bool(i == mario_index)
    candidate_records[i]['is_reference'] = bool(i == 0)

candidates_df = pd.DataFrame(candidate_records)

# ----------------------------------------------------------------------
# Guardar tabla de candidatos en Excel
# ----------------------------------------------------------------------
ensure_package('openpyxl', 'openpyxl')
movement4_excel_filename = 'movement4_tables_mario_fixed_' + uuid.uuid4().hex + '.xlsx'
movement4_excel_path = os.path.join(files_path, movement4_excel_filename)
candidates_df.to_excel(movement4_excel_path, index=False)

msgLog = 'Executed: 96% - Movement 4 Mario fixed candidate table saved to Excel, generating updated triad scatter plots'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Generación de gráficos de dispersión triad
# ----------------------------------------------------------------------
C_plot = C_norm_arr
J_plot = J_norm_arr
R_plot = R_norm_arr

is_pareto_bool = is_pareto
is_experiment_bool = np.zeros(N_tot, dtype=bool)
is_experiment_bool[np.array(experiments_indices, dtype=int)] = True
is_reference_bool = np.zeros(N_tot, dtype=bool)
is_reference_bool[0] = True
is_mario_bool = np.zeros(N_tot, dtype=bool)
is_mario_bool[mario_index] = True

fig, axes = plt.subplots(1, 3, figsize=(18, 5))

# (C, J) coloreado por R
sc0 = axes[0].scatter(C_plot, J_plot, c=R_plot, cmap='viridis', s=20.0, alpha=0.6, edgecolors='none', label='Local candidates')
axes[0].scatter(C_plot[is_pareto_bool], J_plot[is_pareto_bool], facecolors='none', edgecolors='red', s=50.0, linewidths=1.0, label='Local Pareto')
axes[0].scatter(C_plot[0], J_plot[0], marker='^', color='magenta', s=80.0, label='Reference')
axes[0].scatter(C_plot[mario_index], J_plot[mario_index], marker='*', color='yellow', edgecolors='black', s=120.0, label='Mario')
axes[0].scatter(C_plot[is_experiment_bool], J_plot[is_experiment_bool], marker='D', facecolors='none', edgecolors='black', s=70.0, label='Experiments')
axes[0].set_xlabel('C norm crust')
axes[0].set_ylabel('J norm juiciness')
axes[0].set_title('Crunchiness versus juiciness coloured by roast risk')
axes[0].grid(True)
cbar0 = fig.colorbar(sc0, ax=axes[0])
cbar0.set_label('R norm roast risk')
axes[0].legend(loc='best')

# (C, R) coloreado por J
sc1 = axes[1].scatter(C_plot, R_plot, c=J_plot, cmap='plasma', s=20.0, alpha=0.6, edgecolors='none', label='Local candidates')
axes[1].scatter(C_plot[is_pareto_bool], R_plot[is_pareto_bool], facecolors='none', edgecolors='red', s=50.0, linewidths=1.0, label='Local Pareto')
axes[1].scatter(C_plot[0], R_plot[0], marker='^', color='magenta', s=80.0, label='Reference')
axes[1].scatter(C_plot[mario_index], R_plot[mario_index], marker='*', color='yellow', edgecolors='black', s=120.0, label='Mario')
axes[1].scatter(C_plot[is_experiment_bool], R_plot[is_experiment_bool], marker='D', facecolors='none', edgecolors='black', s=70.0, label='Experiments')
axes[1].set_xlabel('C norm crust')
axes[1].set_ylabel('R norm roast risk')
axes[1].set_title('Crunchiness versus roast risk coloured by juiciness')
axes[1].grid(True)
cbar1 = fig.colorbar(sc1, ax=axes[1])
cbar1.set_label('J norm juiciness')
axes[1].legend(loc='best')

# (J, R) coloreado por C
sc2 = axes[2].scatter(J_plot, R_plot, c=C_plot, cmap='cividis', s=20.0, alpha=0.6, edgecolors='none', label='Local candidates')
axes[2].scatter(J_plot[is_pareto_bool], R_plot[is_pareto_bool], facecolors='none', edgecolors='red', s=50.0, linewidths=1.0, label='Local Pareto')
axes[2].scatter(J_plot[0], R_plot[0], marker='^', color='magenta', s=80.0, label='Reference')
axes[2].scatter(J_plot[mario_index], R_plot[mario_index], marker='*', color='yellow', edgecolors='black', s=120.0, label='Mario')
axes[2].scatter(J_plot[is_experiment_bool], R_plot[is_experiment_bool], marker='D', facecolors='none', edgecolors='black', s=70.0, label='Experiments')
axes[2].set_xlabel('J norm juiciness')
axes[2].set_ylabel('R norm roast risk')
axes[2].set_title('Juiciness versus roast risk coloured by crunchiness')
axes[2].grid(True)
cbar2 = fig.colorbar(sc2, ax=axes[2])
cbar2.set_label('C norm crust')
axes[2].legend(loc='best')

image_name = 'image_movement4_mario_fixed_' + uuid.uuid4().hex + '.png'
scatter_image_path = os.path.join(files_path, image_name)
fig.tight_layout()
fig.savefig(scatter_image_path, bbox_inches='tight')
plt.close(fig)

msgLog = 'Executed: 100% - Movement 4 Mario fixed Pareto triad scatter plots generated'
send_message_backend(msgLog, 'GO', 'PROGRESS', backend_args)

# ----------------------------------------------------------------------
# Resumen final y construcción del resultado
# ----------------------------------------------------------------------
indices_summary = {}
indices_summary['N_candidates'] = int(N_candidates)
indices_summary['N_pareto'] = int(N_pareto)
indices_summary['reference_id'] = int(0)
indices_summary['mario_id'] = int(mario_index)
indices_summary['experiment_ids'] = [int(i) for i in experiments_indices]

metrics = {}
metrics['N_candidates'] = float(round(float(N_candidates), 2))
metrics['N_pareto'] = float(round(float(N_pareto), 2))
metrics['C_norm_mario'] = float(round(float(C_norm_arr[mario_index]), 2))
metrics['J_norm_mario'] = float(round(float(J_norm_arr[mario_index]), 2))
metrics['R_norm_mario'] = float(round(float(R_norm_arr[mario_index]), 2))

citations = []
citations.append('Rahman, M.S. (2009). Food Properties Handbook, 2nd ed., CRC Press.')
citations.append('Rahman, M.S. and Labuza, T.P. (2007). Water activity and food preservation. In Handbook of Food Preservation, 2nd ed., CRC Press.')
citations.append('Mottram, D.S. (1998). Flavour formation in meat and meat products. Food Chemistry, 62(4): 415–424.')
citations.append('Nishimura, H. (1985). Role of intramuscular connective tissue in meat texture. Meat Science, 13(4): 195–215.')
citations.append('Igbeka, J.C. and Blaisdell, J.L. (1982). Moisture diffusivity in meat. Journal of Food Technology, 17: 451–460.')
citations.append('Choi, Y. and Okos, M.R. (1986). Effects of temperature and composition on the thermal properties of foods. In Food Engineering and Process Applications.')
citations.append('USDA FoodData Central entries for meat and glaze precursors used in earlier movements for composition anchoring.')

result = {}
result['status'] = 'ok'
result['description'] = ('Rebuilt Movement 4 candidate table and triad scatter plots for pig Jhon Dallas using the user specified Mario schedule as a fixed optimal point, while resimulating four hundred eight phase thermal schedules with analytic phase two drying, explicit one dimensional multilayer PDE for phases three to eight, recomputed normalized crunchiness, juiciness and roast risk indices, updated Pareto front and experimental design.')
result['metrics'] = metrics
tables_dict = {}
tables_dict['candidates'] = candidates_df.to_dict(orient='records')
result['tables'] = tables_dict
result['images'] = [image_name]
result['caption'] = ['Updated triad scatter plots of normalized crunchiness, juiciness and roast risk for Movement 4 candidates with user specified Mario schedule fixed and highlighted together with reference and experimental tests.']
result['files'] = [movement4_excel_filename]
notes = []
notes.append('Mario schedule was fixed to the user specified eight phase process with six day refrigerated drying at nine degrees Celsius and thirty percent relative humidity, followed by exact steam, roasting, marking and Maillard phase temperatures, durations and relative humidities.')
notes.append('All Movement 4 candidates, including reference and Mario, were recomputed with analytic slab drying in phase two, explicit finite difference heat and moisture transport from phase three onward with Fourier numbers capped at zero point two, and Maillard index and roast risk accumulation as in previous movements.')
notes.append('Normalized crunchiness, juiciness and roast risk indices were recalculated across the four hundred candidate cloud, the non dominated Pareto front recomputed, fifteen experimental schedules selected along the new Pareto set while enforcing inclusion of Mario, and the triad scatter plots regenerated accordingly.')
result['notes'] = notes
result['indices'] = indices_summary
result['citations'] = citations

# Salida JSON (opcional)
json_output = json.dumps(result, indent=2)