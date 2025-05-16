import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import SimpleExpSmoothing

def forecast_promedio_movil(serie, ventana=4):
    forecast = serie.rolling(window=ventana, min_periods=1).mean()
    return forecast, None

def forecast_ses(serie):
    model = SimpleExpSmoothing(serie, initialization_method="estimated").fit()
    return model.fittedvalues, model

def safe_forecast(serie, metodo_forecast):
    try:
        forecast_serie, modelo = metodo_forecast(serie)
        if modelo is not None:
            pred = modelo.forecast(1)[0]
        else:
            forecast_serie = forecast_serie.dropna()
            pred = forecast_serie.iloc[-1] if len(forecast_serie) > 0 else serie.tail(4).mean()
    except:
        pred = serie.tail(4).mean()
    if pd.isna(pred) or np.isinf(pred) or pred < 0:
        pred = serie.tail(3).mean()
    if pd.isna(pred) or np.isinf(pred) or pred < 0:
        pred = 0
    return round(pred)

def forecast_engine(df):
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['mes'] = df['fecha'].dt.to_period('M')
    df_mensual = df.groupby(['sku', 'mes']).agg({
        'demanda': 'sum',
        'demanda_sin_outlier': 'sum'
    }).reset_index()
    df_mensual.rename(columns={'demanda_sin_outlier': 'demanda_limpia'}, inplace=True)
    df_mensual['mes'] = df_mensual['mes'].dt.to_timestamp()

    resultados = []
    for sku in df_mensual['sku'].unique():
        df_sku = df_mensual[df_mensual['sku'] == sku].copy()
        df_valid = df_sku[df_sku['demanda_limpia'] > 0]
        if len(df_valid) < 2:
            continue
        serie = df_valid.set_index('mes')['demanda_limpia']

        # Selección simple según historia
        if len(serie) < 6:
            metodo_nombre = 'promedio_movil'
            metodo_forecast = lambda s: forecast_promedio_movil(s, 4)
        else:
            metodo_nombre = 'ses'
            metodo_forecast = forecast_ses

        for _, row in df_sku.iterrows():
            resultados.append({
                'sku': sku,
                'mes': row['mes'],
                'demanda': row['demanda'],
                'demanda_limpia': row['demanda_limpia'],
                'forecast': np.nan,
                'forecast_up': np.nan,
                'tipo_mes': 'histórico',
                'metodo_forecast': metodo_nombre
            })

        # Proyección de próximos 6 meses
        last_month = df_valid['mes'].max()
        forecast_horizon = pd.date_range(start=last_month + pd.DateOffset(months=1), periods=6, freq='MS')
        std_dev = df_valid['demanda_limpia'].tail(4).std()

        for mes in forecast_horizon:
            serie_corte = df_valid[df_valid['mes'] <= mes - pd.DateOffset(months=1)]
            if len(serie_corte) < 2:
                continue
            serie_val = serie_corte.set_index('mes')['demanda_limpia']
            pred = safe_forecast(serie_val, metodo_forecast)
            forecast_up = round(pred + std_dev) if pd.notnull(std_dev) else round(pred)
            resultados.append({
                'sku': sku,
                'mes': mes,
                'demanda': np.nan,
                'demanda_limpia': np.nan,
                'forecast': pred,
                'forecast_up': forecast_up,
                'tipo_mes': 'proyección',
                'metodo_forecast': metodo_nombre
            })

    df_result = pd.DataFrame(resultados)
    df_result['mes'] = pd.to_datetime(df_result['mes']).dt.strftime('%Y-%m')
    df_result['forecast'] = df_result['forecast'].round()
    df_result['forecast_up'] = df_result['forecast_up'].round()
    return df_result




