import os
import pandas as pd
import numpy as np
from fastapi import APIRouter

from services.cleaner import clean_demand
from services.forecast import forecast_engine
from services.stock_projector import project_stock_multi

print("✅ cloud_loader.py importado correctamente", flush=True)
# Variables globales
demanda_limpia_global = []


router = APIRouter(prefix="/cloud", tags=["Cloud Loader"])


@router.get("/cargar_desde_nube")
def cargar_desde_nube():
    try:
        print("📡 Invocando endpoint /cargar_desde_nube", flush=True)

        base = os.path.join(os.path.dirname(__file__), "..", "cloud")

        # ✅ Leer archivos CSV
        df_demanda = pd.read_csv(os.path.join(base, "demanda.csv"), encoding="utf-8-sig")
        df_stock_historico = pd.read_csv(os.path.join(base, "stock_historico.csv"), encoding="utf-8-sig")
        df_stock_actual = pd.read_csv(os.path.join(base, "stock_actual.csv"), encoding="utf-8-sig")
        df_reposiciones = pd.read_csv(os.path.join(base, "reposiciones.csv"), encoding="utf-8-sig")
        df_maestro = pd.read_csv(os.path.join(base, "maestro_productos.csv"), encoding="utf-8-sig")

        # ✅ Normalizar encabezados
        for df in [df_demanda, df_stock_historico, df_stock_actual, df_reposiciones, df_maestro]:
            df.columns = df.columns.str.replace("﻿", "", regex=False).str.strip().str.lower()

        if "demanda" not in df_demanda.columns:
            raise ValueError(f"❌ Falta columna 'demanda'. Columnas: {df_demanda.columns.tolist()}")

        # ✅ Ejecutar limpieza
        df_demanda_limpia = clean_demand(
            df_demanda.to_dict(orient="records"),
            df_stock_historico.to_dict(orient="records")
        )
        df_demanda_limpia = pd.DataFrame(df_demanda_limpia)

        if "demanda_original" not in df_demanda_limpia.columns:
            df_demanda_limpia["demanda_original"] = 0

        if "demanda_sin_outlier" not in df_demanda_limpia.columns:
            df_demanda_limpia["demanda_sin_outlier"] = df_demanda_limpia["demanda_original"]

        # ✅ Reconstruir columna 'demanda'
        df_demanda_limpia["demanda"] = df_demanda_limpia["demanda_sin_outlier"].fillna(0)

        if "fecha" not in df_demanda_limpia.columns and "semana" in df_demanda_limpia.columns:
            df_demanda_limpia["fecha"] = pd.to_datetime(df_demanda_limpia["semana"], errors="coerce")
        else:
            df_demanda_limpia["fecha"] = pd.to_datetime(df_demanda_limpia["fecha"], errors="coerce")

        df_demanda_limpia["demanda_sin_outlier"] = df_demanda_limpia["demanda_sin_outlier"].fillna(0)

        # ✅ Forecast
        df_forecast = forecast_engine(df_demanda_limpia[["sku", "fecha", "demanda", "demanda_sin_outlier"]])

        # ✅ Reemplazar NaNs en forecast
        df_forecast = df_forecast.fillna(0)

        # ✅ Tipos correctos
        df_stock_historico["stock"] = pd.to_numeric(df_stock_historico["stock"], errors="coerce").fillna(0)
        df_stock_actual["stock"] = pd.to_numeric(df_stock_actual["stock"], errors="coerce").fillna(0)
        df_reposiciones["cantidad"] = pd.to_numeric(df_reposiciones["cantidad"], errors="coerce").fillna(0)

        # ✅ Proyección de stock
        df_stock_proj = project_stock_multi(
            df_forecast.to_dict(orient="records"),
            df_stock_actual.to_dict(orient="records"),
            df_reposiciones.to_dict(orient="records"),
            df_maestro.to_dict(orient="records"),
        )

        df_stock_proj = pd.DataFrame(df_stock_proj).fillna(0)

        global demanda_limpia_global
        demanda_limpia_global = df_demanda_limpia.fillna(0).to_dict(orient="records")

        return {
            "demanda_limpia": demanda_limpia_global,
            "forecast": df_forecast.to_dict(orient="records"),
            "maestro": df_maestro.fillna(0).to_dict(orient="records"),
            "reposiciones": df_reposiciones.fillna(0).to_dict(orient="records"),
            "stock_actual": df_stock_actual.fillna(0).to_dict(orient="records"),
            "stock_historico": df_stock_historico.fillna(0).to_dict(orient="records"),
            "stock_proyectado": df_stock_proj.to_dict(orient="records"),
        }


    except Exception as e:
        print("❌ ERROR en /cargar_desde_nube:", e, flush=True)
        return {"error": str(e)}

@router.get("/stock_historico")
def obtener_stock_historico():
    try:
        base = os.path.join(os.path.dirname(__file__), "..", "cloud")
        df_stock_historico = pd.read_csv(os.path.join(base, "stock_historico.csv"), encoding="utf-8-sig")
        df_stock_historico.columns = df_stock_historico.columns.str.replace("﻿", "", regex=False).str.strip().str.lower()
        df_stock_historico["fecha"] = pd.to_datetime(df_stock_historico["fecha"], errors="coerce")
        df_stock_historico["stock"] = pd.to_numeric(df_stock_historico["stock"], errors="coerce").fillna(0)
        return df_stock_historico.fillna(0).to_dict(orient="records")
    except Exception as e:
        print("❌ Error al obtener stock histórico:", e)
        return {"error": str(e)}

@router.get("/demanda_limpia")
def obtener_demanda_limpia():
    global demanda_limpia_global
    return demanda_limpia_global










