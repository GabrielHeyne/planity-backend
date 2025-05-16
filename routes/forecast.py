from fastapi import APIRouter, HTTPException, Request
import pandas as pd
from services.forecast import forecast_engine, generar_comparativa_forecasts

router = APIRouter()

@router.post("/forecast")
async def calcular_forecast(request: Request):
    try:
        data = await request.json()
        df = pd.DataFrame(data)
        forecast = forecast_engine(df)
        comparativa = generar_comparativa_forecasts(df)

        return {
            "forecast": forecast.to_dict(orient="records"),
            "comparativa": comparativa.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
