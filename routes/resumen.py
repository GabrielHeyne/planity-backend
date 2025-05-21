import pandas as pd
import numpy as np

from fastapi import APIRouter, Request
from services.resumen_utils import (
    consolidar_historico_stock,
    generar_contexto_negocio_resumido
)


router = APIRouter(prefix="/resumen-general")

@router.post("/")
async def resumen_general(request: Request):
    try:
        data = await request.json()

        df_demanda = pd.DataFrame(data.get("demanda_limpia", []))
        df_maestro = pd.DataFrame(data.get("maestro", []))
        df_repos = pd.DataFrame(data.get("reposiciones", []))
        detalle_politicas = data.get("detalle_politicas", {})

        # Consolidar histórico
        df_historico = consolidar_historico_stock(df_demanda, df_maestro)
        df_historico = df_historico.fillna(0)  # ✅ Para evitar errores de serialización

        # Contexto de negocio (resumido)
        contexto_global, contexto_por_sku = generar_contexto_negocio_resumido(
            detalle_politicas,
            df_maestro,
            df_repos,
            df_historico
        )

        return {
            "historico": df_historico.to_dict(orient="records"),
            "contexto_global": contexto_global,
            "contexto_por_sku": contexto_por_sku
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": str(e)}






