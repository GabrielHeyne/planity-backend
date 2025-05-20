import os
import pandas as pd
from fastapi import APIRouter

from services.cleaner import clean_demand
from services.forecast import forecast_engine
from services.stock_projector import project_stock_multi

print("✅ cloud_loader.py importado correctamente", flush=True)

router = APIRouter(prefix="/cloud", tags=["Cloud Loader"])


@router.get("/cargar_desde_nube")
def cargar_desde_nube():
    print("📡 Invocando endpoint /cargar_desde_nube", flush=True)

    base = os.path.join(os.path.dirname(__file__), "..", "cloud")

    df_demanda = pd.read_csv(os.path.join(base, "demanda.csv"))
    df_stock_historico = pd.read_csv(os.path.join(base, "stock_historico.csv"))
    df_stock_actual = pd.read_csv(os.path.join(base, "stock_actual.csv"))
    df_reposiciones = pd.read_csv(os.path.join(base, "reposiciones.csv"))
    df_maestro = pd.read_csv(os.path.join(base, "maestro_productos.csv"))

    df_demanda_limpia = clean_demand(df_demanda, df_stock_historico)
    df_forecast = forecast_engine(df_demanda_limpia)
    df_stock_proj = project_stock_multi(
        df_forecast.to_dict(orient="records"),
        df_stock_actual.to_dict(orient="records"),
        df_reposiciones.to_dict(orient="records"),
        df_maestro.to_dict(orient="records"),
    )

    return {
        "demanda_limpia": df_demanda_limpia.to_dict(orient="records"),
        "forecast": df_forecast.to_dict(orient="records"),
        "maestro": df_maestro.to_dict(orient="records"),
        "reposiciones": df_reposiciones.to_dict(orient="records"),
        "stock_actual": df_stock_actual.to_dict(orient="records"),
        "stock_historico": df_stock_historico.to_dict(orient="records"),
        "stock_proyectado": df_stock_proj.to_dict(orient="records"),
    }



