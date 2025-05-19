import pandas as pd
from datetime import datetime

def calcular_politicas_inventario(df_forecast, sku, unidades_en_camino, df_maestro, df_demanda_limpia):
    fecha_actual = pd.to_datetime("today").replace(day=1)

    # Forecast filtrado para el SKU
    df_forecast_sku = df_forecast[(df_forecast['sku'] == sku) & (df_forecast['tipo_mes'] == 'proyección')].copy()
    df_forecast_sku['mes'] = pd.to_datetime(df_forecast_sku['mes'])
    forecast_futuro = df_forecast_sku[df_forecast_sku['mes'] >= fecha_actual].sort_values('mes')
    forecast_4m = forecast_futuro.head(4)['forecast']
    demanda_mensual = int(round(forecast_4m.mean(), 0)) if not forecast_4m.empty else 0

    # 🔧 Aquí usamos la columna correcta
    df_d_sku = df_demanda_limpia[
        (df_demanda_limpia['sku'] == sku) &
        (df_demanda_limpia['demanda_sin_outlier'] > 0)
    ].copy()

    df_d_sku['mes'] = pd.to_datetime(df_d_sku['fecha']).dt.to_period('M')
    demanda_mensual_hist = df_d_sku.groupby('mes')['demanda_sin_outlier'].sum()
    ultimos_meses = demanda_mensual_hist.tail(12)
    desviacion_estandar = ultimos_meses.std()
    if pd.isna(desviacion_estandar):
        desviacion_estandar = 0

    safety_stock = round(desviacion_estandar * 1.65)
    lead_time = 5
    rop_original = demanda_mensual * lead_time
    rop = rop_original + safety_stock
    eoq = demanda_mensual * 3

    return {
        "demanda_mensual": demanda_mensual,
        "safety_stock": safety_stock,
        "rop_original": rop_original,
        "rop": rop,
        "eoq": eoq
    }

