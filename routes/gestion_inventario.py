from fastapi import APIRouter, Request
import pandas as pd
from typing import Dict, Any
from services.inventory_managment import calcular_politicas_inventario
from services.evaluar_compra_sku import evaluar_compra_sku
from datetime import datetime

router = APIRouter(prefix="/gestion_inventario")

@router.post("/")
async def gestion_inventario(request: Request):
    data: Dict[str, Any] = await request.json()
    print("✅ Data recibida en /gestion_inventario:", list(data.keys()))

    try:
        df_forecast = pd.DataFrame(data.get("forecast", []))
        df_maestro = pd.DataFrame(data.get("maestro", []))
        df_demanda_limpia = pd.DataFrame(data.get("demanda_limpia", []))
        df_stock = pd.DataFrame(data.get("stock_actual", []))
        df_repos = pd.DataFrame(data.get("reposiciones", []))

        df_forecast['mes'] = pd.to_datetime(df_forecast['mes'], errors='coerce')
        if 'fecha' in df_repos.columns:
            df_repos['fecha'] = pd.to_datetime(df_repos['fecha'], errors='coerce')
        if 'cantidad' in df_repos.columns:
            df_repos['cantidad'] = pd.to_numeric(df_repos['cantidad'], errors='coerce').fillna(0)

        fecha_actual = pd.to_datetime(datetime.today()).replace(day=1)
        tabla_resumen = []
        detalles_por_sku = {}

        for sku in df_forecast['sku'].dropna().unique():
            stock_actual = float(df_stock[df_stock['sku'] == sku]['stock'].iloc[0]) if sku in df_stock['sku'].values else 0
            unidades_en_camino = float(df_repos[df_repos['sku'] == sku]['cantidad'].sum()) if sku in df_repos['sku'].values else 0
            costo_fab = float(df_maestro[df_maestro['sku'] == sku]['costo_fabricacion'].iloc[0]) if sku in df_maestro['sku'].values else 0

            forecast_4m = df_forecast[
                (df_forecast['sku'] == sku) &
                (df_forecast['tipo_mes'] == 'proyección') &
                (df_forecast['mes'] >= fecha_actual)
            ]['forecast'].head(4)
            demanda_mensual = int(round(forecast_4m.mean(), 0)) if not forecast_4m.empty else 0

            politicas = calcular_politicas_inventario(df_forecast, sku, unidades_en_camino, df_maestro, df_demanda_limpia)
            if politicas is None:
                continue

            resultado = evaluar_compra_sku(
                sku, stock_actual, fecha_actual, demanda_mensual,
                politicas["safety_stock"], politicas["eoq"], df_repos
            )

            tabla_resumen.append({
                "SKU": sku,
                "Demanda Mensual": demanda_mensual,
                "Stock Actual": int(stock_actual),
                "Reposiciones": int(unidades_en_camino),
                "Stock Proyectado (5M)": resultado["stock_final_simulado"],
                "ROP": politicas["rop_original"],
                "Safety Stock": politicas["safety_stock"],
                "EOQ": politicas["eoq"],
                "Costo Fabricación (€)": round(costo_fab, 2),
                "Acción": resultado["accion"]
            })

            detalles_por_sku[sku] = {
                "stock_actual": stock_actual,
                "unidades_en_camino": unidades_en_camino,
                "demanda_mensual": demanda_mensual,
                "politicas": politicas,
                "accion": resultado["accion"],
                "unidades_sugeridas": politicas["eoq"] if resultado["accion"] == "Comprar" else 0,
                "stock_final_simulado": resultado["stock_final_simulado"],
                "costo_fabricacion": costo_fab
            }

        compras = [r for r in tabla_resumen if r["Acción"] == "Comprar"]
        total_skus = len(compras)
        total_unidades = sum(r["EOQ"] for r in compras)
        total_costo = sum(r["EOQ"] * r["Costo Fabricación (€)"] for r in compras)

        return {
            "tabla_resumen": tabla_resumen,
            "detalles_por_sku": detalles_por_sku,
            "kpis": {
                "total_skus": total_skus,
                "total_unidades": total_unidades,
                "total_costo": total_costo
            }
        }

    except Exception as e:
        import traceback
        print("❌ Error en /gestion_inventario:", str(e))
        traceback.print_exc()
        return {"error": str(e)}




