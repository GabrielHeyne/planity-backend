from fastapi import APIRouter, Request
from services.resumen_utils import consolidar_historico_stock
from fastapi.responses import JSONResponse

router = APIRouter()

@router.post("/resumen_general")
async def generar_resumen(request: Request):
    body = await request.json()

    demanda_limpia = body.get("demanda_limpia", [])
    maestro = body.get("maestro", [])

    if not demanda_limpia or not maestro:
        return JSONResponse(content={"error": "Faltan datos de entrada"}, status_code=400)

    df_historico = consolidar_historico_stock(demanda_limpia, maestro)
    return df_historico.to_dict(orient="records")







