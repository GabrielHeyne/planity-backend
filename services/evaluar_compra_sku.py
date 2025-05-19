import pandas as pd
from dateutil.relativedelta import relativedelta

def evaluar_compra_sku(
    sku: str,
    stock_inicial: int,
    fecha_actual: pd.Timestamp,
    demanda_mensual: float,
    safety_stock: float,
    eoq: float,
    df_repos: pd.DataFrame = None
):
    meses_simulados = 5
    stock = float(stock_inicial)

    repos_por_mes = {i: 0 for i in range(meses_simulados)}

    if df_repos is not None and not df_repos.empty:
        df_sku = df_repos[df_repos['sku'] == sku].copy()
        df_sku['cantidad'] = pd.to_numeric(df_sku['cantidad'], errors='coerce').fillna(0)
        df_sku['mes'] = pd.to_datetime(df_sku['fecha']).dt.to_period('M').dt.to_timestamp()

        for i in range(meses_simulados):
            mes_sim = (fecha_actual + relativedelta(months=i)).to_period('M').to_timestamp()
            cantidad = df_sku[df_sku['mes'] == mes_sim]['cantidad'].sum()
            repos_por_mes[i] = float(cantidad)

    for i in range(meses_simulados):
        stock += repos_por_mes[i]
        stock -= float(demanda_mensual)

    unidades_en_camino = sum(repos_por_mes.values())

    if unidades_en_camino > 0:
        umbral = float(safety_stock)
        comparar_con = float(stock)
    else:
        umbral = float(demanda_mensual * meses_simulados + safety_stock)
        comparar_con = float(stock_inicial)

    if comparar_con < umbral:
        accion = "Comprar"
        sugerido = round(float(eoq) if eoq is not None else umbral - comparar_con)
    else:
        accion = "No comprar"
        sugerido = 0

    return {
        "accion": accion,
        "sugerido": sugerido,
        "stock_final_simulado": round(stock),
        "umbral": umbral
    }


