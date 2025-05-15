from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io

from services.cleaner import clean_demand

app = FastAPI()

print("🧠 ESTOY EN EL BACKEND CORRECTO", flush=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # puedes restringir esto más adelante
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"mensaje": "API de Planity activa!"}

@app.post("/limpiar-demanda")
async def limpiar_demanda(demanda: UploadFile = File(...), stock: UploadFile = File(...)):
    try:
        contenido_demanda = await demanda.read()
        contenido_stock = await stock.read()

        # Leer CSVs con encoding y separador forzado
        df_demanda = pd.read_csv(io.BytesIO(contenido_demanda), encoding="utf-8", sep=",")
        df_stock = pd.read_csv(io.BytesIO(contenido_stock), encoding="utf-8", sep=",")

        # Limpiar nombres de columnas
        df_demanda.columns = df_demanda.columns.str.strip().str.lower()
        df_stock.columns = df_stock.columns.str.strip().str.lower()

        print("📋 Columnas DEMANDA:", df_demanda.columns.tolist())
        print("📋 Columnas STOCK:", df_stock.columns.tolist())

        # ✅ Convertir a listas de dicts antes de limpiar
        df_limpia = clean_demand(
            df_demanda.to_dict(orient="records"),
            df_stock.to_dict(orient="records")
        )

        return df_limpia  # ya es lista de dicts

    except Exception as e:
        print("❌ ERROR AL PROCESAR:", e)
        return {"error": str(e)}


