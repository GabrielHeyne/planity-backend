import pandas as pd

def consolidar_historico_stock(demanda_limpia: list, maestro: list) -> pd.DataFrame:
    df_demanda = pd.DataFrame(demanda_limpia)
    df_maestro = pd.DataFrame(maestro)

    if df_demanda.empty or df_maestro.empty:
        return pd.DataFrame()

    df = df_demanda.copy()
    df['fecha'] = pd.to_datetime(df['fecha'])

    df['mes'] = df['fecha'].dt.to_period("M").astype(str)

    # Ventas reales
    df_ventas = df.groupby(['sku', 'mes'])['demanda'].sum().reset_index(name="unidades_vendidas")

    # Unidades perdidas históricas
    df['unidades_perdidas'] = df['demanda_sin_outlier'] - df['demanda']
    df['unidades_perdidas'] = df['unidades_perdidas'].clip(lower=0)

    df_perdidas = df.groupby(['sku', 'mes'])['unidades_perdidas'].sum().reset_index()

    # Merge y precios
    df_resultado = pd.merge(df_ventas, df_perdidas, on=['sku', 'mes'], how="outer").fillna(0)
    df_resultado = pd.merge(df_resultado, df_maestro[['sku', 'precio_venta']], on="sku", how="left")

    # Facturación real y perdida
    df_resultado["venta_real_euros"] = df_resultado["unidades_vendidas"] * df_resultado["precio_venta"]
    df_resultado["venta_perdida_euros"] = df_resultado["unidades_perdidas"] * df_resultado["precio_venta"]

    df_resultado = df_resultado.fillna(0)

    return df_resultado







