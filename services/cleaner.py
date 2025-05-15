import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time


def clean_demand(demanda_raw: list, stock_raw: list) -> list:
    print("🧠 Iniciando función clean_demand...")
    t0 = time.time()

    # --- Preprocesar demanda ---
    df = pd.DataFrame(demanda_raw)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["semana"] = df["fecha"] - pd.to_timedelta(df["fecha"].dt.weekday, unit='D')
    df["demanda_original"] = pd.to_numeric(df["demanda"], errors="coerce").fillna(0).astype(int)
    df = df[["sku", "semana", "demanda_original"]]
    print(f"✅ Preprocesamiento demanda: {round(time.time() - t0, 2)} seg")

    # --- Reindexar ---
    semanas_totales = sorted(df["semana"].unique())
    skus = df["sku"].unique()
    index = pd.MultiIndex.from_product([skus, semanas_totales], names=["sku", "semana"])
    df = df.set_index(["sku", "semana"]).reindex(index, fill_value=0).reset_index()
    df["demanda_sin_stockout"] = np.nan
    df["demanda_sin_outlier"] = np.nan
    print(f"✅ Reindexado con semanas: {round(time.time() - t0, 2)} seg")

    # --- Procesar stock ---
    stock_df = pd.DataFrame(stock_raw)
    stock_df["fecha"] = pd.to_datetime(stock_df["fecha"])
    stock_df["mes"] = stock_df["fecha"].dt.to_period("M").dt.to_timestamp()
    stock_df["stock"] = pd.to_numeric(stock_df["stock"], errors="coerce").fillna(0).astype(int)
    stock_map = stock_df.groupby(["sku", "mes"])["stock"].sum().to_dict()
    print(f"📦 Procesamiento stock: {round(time.time() - t0, 2)} seg")

    fecha_max = stock_df["mes"].max()
    fecha_min = fecha_max - pd.DateOffset(months=11)
    ultimos_12 = stock_df[stock_df["mes"].between(fecha_min, fecha_max)]
    resumen = ultimos_12.groupby(["sku", "mes"])["stock"].sum().reset_index()
    resumen["sin_stock"] = resumen["stock"] == 0
    conteo = resumen.groupby("sku")["sin_stock"].sum()
    skus_obsoletos = conteo[conteo == 12].index.tolist()
    ultimos_3_meses = sorted(stock_df["mes"].unique())[-3:]

    # --- Limpieza por SKU ---
    def procesar_grupo(grupo):
        sku = grupo["sku"].iloc[0]
        grupo = grupo.sort_values("semana").reset_index(drop=True)
        grupo["es_obsoleto"] = sku in skus_obsoletos
        demanda_sin_stockout = []

        # Cálculo de P10 histórico (solo valores > 0)
        historico = grupo[grupo["demanda_original"] > 0]["demanda_original"]
        p10_total = np.percentile(historico, 10) if not historico.empty else 0

        for i in range(len(grupo)):
            demanda_actual = grupo.loc[i, "demanda_original"]
            fecha_semana = grupo.loc[i, "semana"]
            mes_actual = fecha_semana.to_period("M").to_timestamp()
            mes_anterior = (fecha_semana - pd.DateOffset(weeks=4)).to_period("M").to_timestamp()
            mes_posterior = (fecha_semana + pd.DateOffset(weeks=4)).to_period("M").to_timestamp()

            # Criterio base: si stock >= 4 en meses clave, no limpiar
            meses_revisar = [mes_anterior, mes_actual, mes_posterior]
            stock_ok = all(stock_map.get((sku, m), 0) >= 4 for m in meses_revisar)
            if stock_ok:
                demanda_sin_stockout.append(demanda_actual)
                continue

            ultimas_24 = grupo.iloc[max(0, i - 24):i]
            demanda_valida = ultimas_24[ultimas_24["demanda_original"] > 0]["demanda_original"]
            p15 = np.percentile(demanda_valida, 15) if not demanda_valida.empty else 0
            p60 = np.percentile(demanda_valida, 60) if not demanda_valida.empty else 0

            aplicar_limpieza = False
            imputar = False

            stock_actual = stock_map.get((sku, mes_actual), 0)
            stock_anterior = stock_map.get((sku, mes_anterior), 0)
            meses_futuros = [mes_actual + pd.DateOffset(months=m) for m in range(1, 7)]
            stock_futuro = sum(stock_map.get((sku, m), 0) for m in meses_futuros)

            if (stock_actual < 4 or stock_anterior < 4) and (stock_futuro > 0 or mes_actual in ultimos_3_meses):
                aplicar_limpieza = imputar = True

            if any(stock_map.get((sku, m), 0) < 4 for m in meses_revisar):
                aplicar_limpieza = imputar = True

            suma_ultimas_24 = ultimas_24["demanda_original"].sum()
            if suma_ultimas_24 > p10_total:
                aplicar_limpieza = True
                imputar = True

            if not aplicar_limpieza:
                demanda_sin_stockout.append(demanda_actual)
                continue

            if imputar and demanda_actual < p15:
                demanda_sin_stockout.append(round(p60))
            else:
                demanda_sin_stockout.append(demanda_actual)

        grupo["demanda_sin_stockout"] = demanda_sin_stockout
        demanda_limpia = grupo[grupo["demanda_sin_stockout"] > 0]["demanda_sin_stockout"]
        p95 = np.percentile(demanda_limpia, 95) if not demanda_limpia.empty else None
        grupo["demanda_sin_outlier"] = [
            round(p95) if p95 is not None and val > p95 else val
            for val in grupo["demanda_sin_stockout"]
        ]
        return grupo

    # --- Paralelizar ---
    t1 = time.time()
    with ThreadPoolExecutor() as executor:
        resultados = list(executor.map(procesar_grupo, [g for _, g in df.groupby("sku")]))
    print(f"🧪 Limpieza por SKU: {round(time.time() - t1, 1)} seg")

    df_final = pd.concat(resultados).sort_values(["sku", "semana"])
    df_final = df_final.rename(columns={"semana": "fecha"})
    resultado = df_final.to_dict(orient="records")

    print(f"✅ Limpieza completada. Total filas: {len(resultado)}")
    print(f"⏱️ Tiempo total: {round(time.time() - t0, 1)} seg")
    return resultado












