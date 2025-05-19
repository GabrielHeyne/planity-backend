from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import numpy as np

from services.cleaner import clean_demand
from services.forecast import forecast_engine
from services.stock_projector import project_stock_multi

# ✅ Crear app primero
app = FastAPI()

print("🧠 ESTOY EN EL BACKEND CORRECTO", flush=True)

# ✅ Middleware CORS debe ir inmediatamente después de instanciar la app
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost",
        "http://127.0.0.1",
        "https://planity-fronted-uozd.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Luego se importan los routers (esto es clave)
from routes.gestion_inventario import router as gestion_router
app.include_router(gestion_router)

# --- Ruta raíz ---
@app.get("/")
def read_root():
    return {"mensaje": "API de Planity activa!"}

# --- Limpieza de demanda ---
@app.post("/limpiar-demanda")
async def limpiar_demanda(demanda: UploadFile = File(...), stock: UploadFile = File(...)):
    try:
        contenido_demanda = await demanda.read()
        contenido_stock = await stock.read()

        df_demanda = pd.read_csv(io.BytesIO(contenido_demanda), encoding="utf-8", sep=",")
        df_stock = pd.read_csv(io.BytesIO(contenido_stock), encoding="utf-8", sep=",")

        df_demanda.columns = df_demanda.columns.str.strip().str.lower()
        df_stock.columns = df_stock.columns.str.strip().str.lower()

        print("📋 Columnas DEMANDA:", df_demanda.columns.tolist())
        print("📋 Columnas STOCK:", df_stock.columns.tolist())

        df_limpia = clean_demand(
            df_demanda.to_dict(orient="records"),
            df_stock.to_dict(orient="records")
        )

        return df_limpia

    except Exception as e:
        print("❌ ERROR AL PROCESAR:", e)
        return {"error": str(e)}

# --- Forecast simplificado ---
@app.post("/forecast")
async def calcular_forecast(request: Request):
    try:
        data = await request.json()
        df = pd.DataFrame(data)

        print("📨 Datos recibidos en /forecast:", df.head())

        forecast = forecast_engine(df)

        return {
            "forecast": forecast.replace({np.nan: ""}).to_dict(orient="records")
        }

    except Exception as e:
        print("❌ ERROR EN FORECAST:", e)
        raise HTTPException(status_code=500, detail=str(e))

# --- Proyección de stock ---
@app.post("/proyeccion-stock")
async def calcular_proyeccion_stock(request: Request):
    try:
        data = await request.json()
        forecast = data.get("forecast", [])
        stock_actual = data.get("stock_actual", [])
        reposiciones = data.get("reposiciones", [])
        maestro = data.get("maestro", [])

        df_resultado = project_stock_multi(forecast, stock_actual, reposiciones, maestro)
        return df_resultado.to_dict(orient="records")

    except Exception as e:
        print("❌ ERROR EN PROYECCIÓN DE STOCK:", e)
        raise HTTPException(status_code=500, detail=str(e))




















