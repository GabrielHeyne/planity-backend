
import pandas as pd
import numpy as np

def consolidar_historico_stock(df_demanda_limpia, df_maestro):
    df = df_demanda_limpia.copy()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['mes'] = df['fecha'].dt.to_period('M').dt.to_timestamp()

    df['unidades_perdidas'] = df.apply(
        lambda row: row['demanda_sin_stockout'] - row['demanda']
        if row['demanda_sin_stockout'] > row['demanda'] else 0,
        axis=1
    )

    resumen = df.groupby(['sku', 'mes']).agg(
        demanda_real=('demanda', 'sum'),
        demanda_limpia=('demanda_sin_outlier', 'sum'),
        unidades_perdidas=('unidades_perdidas', 'sum')
    ).reset_index()

    if not df_maestro.empty:
        resumen = resumen.merge(df_maestro[['sku', 'precio_venta']], on='sku', how='left')
        resumen['valor_perdido_euros'] = resumen['unidades_perdidas'] * resumen['precio_venta']
    else:
        resumen['valor_perdido_euros'] = 0

    return resumen


def generar_contexto_negocio_resumido(detalle_politicas, df_maestro, df_repos, df_historico=None):
    import pandas as pd
    import numpy as np

    # --- Construir DataFrame base
    df = pd.DataFrame([
        {
            "SKU": sku,
            "Demanda mensual": datos.get("demanda_mensual", 0),
            "Safety Stock": datos.get("politicas", {}).get("safety_stock", 0),
            "ROP": datos.get("politicas", {}).get("rop_original", 0),
            "EOQ": datos.get("politicas", {}).get("eoq", 0),
            "Acción": datos.get("accion", ""),
            "Unidades a Comprar": datos.get("unidades_sugeridas", 0),
            "Stock Actual": datos.get("stock_final_simulado", 0)
        }
        for sku, datos in detalle_politicas.items()
    ])

    if df.empty:
        print("⚠️ detalle_politicas llegó vacío")
        return {}, []

    # --- Costo fabricación
    if not df_maestro.empty:
        df = df.merge(df_maestro[['sku', 'costo_fabricacion', 'precio_venta']], left_on='SKU', right_on='sku', how='left')
        df['Costo Fabricación (€)'] = df['Unidades a Comprar'] * df['costo_fabricacion']
    else:
        df['Costo Fabricación (€)'] = 0
        df['precio_venta'] = 0

    # --- Unidades en camino
    if not df_repos.empty:
        en_camino = df_repos.groupby('sku')['cantidad'].sum().reset_index().rename(columns={'sku': 'SKU', 'cantidad': 'Unidades en Camino'})
        df = df.merge(en_camino, on='SKU', how='left')
    else:
        df['Unidades en Camino'] = 0

    df = df.fillna(0)

    # --- Variables globales
    unidades_vendidas = 0
    facturacion = 0
    unidades_perdidas = 0
    venta_perdida = 0
    demanda_ult_3m = 0
    tasa_quiebre = 0

    # --- Históricos si existen
    if df_historico is not None and not df_historico.empty:
        df_historico = df_historico.copy()
        df_historico['mes'] = pd.to_datetime(df_historico['mes'])
        ultimos_12m = df_historico['mes'].max() - pd.DateOffset(months=12)
        ultimos_3m = df_historico['mes'].max() - pd.DateOffset(months=3)

        df_12m = df_historico[df_historico['mes'] > ultimos_12m]
        df_3m = df_historico[df_historico['mes'] > ultimos_3m]

        # Globales
        unidades_vendidas = df_12m['demanda_real'].sum()
        facturacion = (df_12m['demanda_real'] * df_12m['precio_venta']).sum()
        unidades_perdidas = df_12m['unidades_perdidas'].sum()
        venta_perdida = df_12m['valor_perdido_euros'].sum()
        demanda_ult_3m = df_3m['demanda_real'].sum() / max(len(df_3m['mes'].unique()), 1)

        total_deseada = unidades_vendidas + unidades_perdidas
        tasa_quiebre = (unidades_perdidas / total_deseada) * 100 if total_deseada > 0 else 0

        # Detalle por SKU
        df_kpis12m = df_12m.groupby("sku").agg(
            unidades_vendidas=('demanda_real', 'sum'),
            unidades_perdidas=('unidades_perdidas', 'sum'),
            venta_perdida=('valor_perdido_euros', 'sum')
        ).reset_index()

        df_kpis12m = df_kpis12m.merge(df_maestro[['sku', 'precio_venta']], on='sku', how='left')
        df_kpis12m['facturacion'] = df_kpis12m['unidades_vendidas'] * df_kpis12m['precio_venta']

        df_kpis3m = df_3m.groupby("sku").agg(
            demanda_total=('demanda_real', 'sum'),
            meses=('mes', 'nunique')
        ).reset_index()
        df_kpis3m["demanda_mensual_3m"] = df_kpis3m["demanda_total"] / df_kpis3m["meses"].replace(0, 1)

        df_merge = df_kpis12m.merge(df_kpis3m[["sku", "demanda_mensual_3m"]], on="sku", how="outer")
        df_merge["tasa_quiebre"] = (
            df_merge["unidades_perdidas"] /
            (df_merge["unidades_perdidas"] + df_merge["unidades_vendidas"])
        ).replace([np.inf, -np.inf, np.nan], 0) * 100

        df_merge = df_merge.fillna(0)

        df = df.merge(df_merge, left_on="SKU", right_on="sku", how="left")

        # Renombrar columnas
        df = df.rename(columns={
            "unidades_vendidas": "Unid. Vendidas (12M)",
            "facturacion": "Facturación (12M)",
            "unidades_perdidas": "Unidades Perdidas (12M)",
            "venta_perdida": "Venta Perdida (12M)",
            "demanda_mensual_3m": "Demanda Mensual (3M)",
            "tasa_quiebre": "Tasa de Quiebre"
        })

    # --- Final cleanup
    df["¿Comprar este SKU?"] = df["Acción"].apply(lambda x: "Sí" if x == "Comprar" else "No")
    df = df.fillna(0)
    df["Tasa de Quiebre"] = df["Tasa de Quiebre"].round(1)

    columnas_finales = [
        "SKU", "Stock Actual", "Unid. Vendidas (12M)", "Facturación (12M)",
        "Unidades en Camino", "¿Comprar este SKU?", "Unidades Perdidas (12M)",
        "Venta Perdida (12M)", "Demanda Mensual (3M)", "Tasa de Quiebre",
        "Unidades a Comprar"
    ]

    df_contexto = df[columnas_finales].copy()

    contexto_global = {
        "Stock Actual": int(df["Stock Actual"].sum()),
        "Unid. Vendidas (12M)": int(unidades_vendidas),
        "Facturación (12M)": int(facturacion),
        "Unid. en Camino": int(df["Unidades en Camino"].sum()),
        "SKUs a Comprar": int((df["¿Comprar este SKU?"] == "Sí").sum()),
        "Unidades Perdidas (12M)": int(unidades_perdidas),
        "Venta Perdida (12M)": int(venta_perdida),
        "Demanda Mensual (3M)": int(demanda_ult_3m),
        "Tasa de Quiebre": round(tasa_quiebre, 1),
        "Unidades a Comprar": int(df["Unidades a Comprar"].sum())
    }

    return contexto_global, df_contexto.to_dict(orient="records")






