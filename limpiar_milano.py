import pandas as pd
from pathlib import Path

# =========================
# CONFIGURACIÓN
# =========================
INPUT_FILE = "farmacie_roma.csv"
OUTPUT_FILE = "farmacie_roma_maps.csv"

COUNTRY = "Italia"

# =========================
# CARGAR CSV
# =========================
df = pd.read_csv(INPUT_FILE)

# =========================
# CREAR COLUMNA DIRECCION_COMPLETA
# =========================
df["Direccion_completa"] = (
    df["Indirizzo"].astype(str).str.strip() + ", " +
    df["CAP"].astype(str).str.zfill(5) + " " +
    df["Comune"].astype(str).str.strip() + ", " +
    COUNTRY
)

# =========================
# GUARDAR NUEVO CSV
# =========================
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print(f"✅ Archivo creado correctamente: {OUTPUT_FILE}")
print(f"📍 Filas procesadas: {len(df)}")
