COCHINILLO LACADO AL ESTILO PEQUINÉS
Physics-based reconstruction of Peking duck applied to suckling pig

This repository contains the full computational framework supporting the study:

“Peking-style lacquered suckling pig: a physics-based optimisation framework”

The objective is to reconstruct the physical, chemical and thermal architecture of Peking duck and transfer it to a suckling pig system through a four-movement modelling approach.

Overview of the methodology

The work is structured into four modelling blocks:

Movement 1 – Architecture, composition and drying
Allometric geometry (6 kg reference system)
Multilayer structure (skin–fat–muscle)
Composition (water, fat, protein, collagen)
Refrigerated drying model (48 h, controlled RH)
Structural similarity index (pig vs duck)
Movement 2 – Aroma modelling
Definition of four chemical families:
Maillard reactions
Lipid oxidation
Glazing volatiles
Sulfur compounds
Temperature-dependent intensity functions
Dynamic aroma trajectories (PCA-based)
Movement 3 – Multilayer PDE simulation
1D transient heat and mass transfer
Coupled moisture diffusion and thermal gradients
Fat melting (sigmoidal model)
Surface Maillard accumulation
8-phase thermal schedule simulation
Movement 4 – Optimisation and decision framework
400 candidate cooking schedules
Grid/random search in process space
Multi-objective evaluation:
Crunchiness (C)
Juiciness (J)
Roast control (R)
Pareto front extraction
Selection of experimental candidates
