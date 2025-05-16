import pandas as pd

def project_stock_multi(forecast_raw, stock_raw, repos_raw, maestro_raw):
    df_forecast = pd.DataFrame(forecast_raw)
    df_stock = pd.DataFrame(stock_raw)
    df_repos = pd.DataFrame(repos_raw)
    df_maestro = pd.DataFrame(maestro_raw)

    # --- Asegurar tipos y limpiar ---
    df_forecast['mes'] = pd.to_datetime(df_forecast['mes'], errors='coerce')
    df_stock['fecha'] = pd.to_datetime(df_stock['fecha'], errors='coerce')
    df_repos['fecha'] = pd.to_datetime(df_repos['fecha'], errors='coerce')

    df_stock['mes'] = df_stock['fecha'].dt.to_period('M').dt.to_timestamp()
    df_repos['mes'] = df_repos['fecha'].dt.to_period('M').dt.to_timestamp()

    # Asegurar que forecast sea numérico
    df_forecast['forecast'] = pd.to_numeric(df_forecast['forecast'], errors='coerce').fillna(0).astype(int)

    # Asegurar que precio_venta sea numérico
    if 'precio_venta' in df_maestro.columns:
        df_maestro['precio_venta'] = pd.to_numeric(df_maestro['precio_venta'], errors='coerce').fillna(0)
    else:
        df_maestro['precio_venta'] = 0

    # Mapa de precios
    precio_map = df_maestro.set_index('sku')['precio_venta'].to_dict()

    resultados = []

    for sku in df_forecast['sku'].unique():
        df_f_sku = df_forecast[(df_forecast['sku'] == sku) & (df_forecast['forecast'] > 0)].sort_values('mes')
        df_r_sku = df_repos[df_repos['sku'] == sku]
        stock_info = df_stock[df_stock['sku'] == sku]

        if df_f_sku.empty or stock_info.empty:
            continue

        stock_inicial = int(float(stock_info.iloc[0]['stock'] or 0))
        fecha_inicio = stock_info.iloc[0]['mes']
        precio_unitario = float(precio_map.get(sku, 0) or 0)

        for _, row in df_f_sku.iterrows():
            mes = row['mes']
            try:
                forecast_val = int(float(row['forecast']))
            except (ValueError, TypeError):
                forecast_val = 0

            if mes < fecha_inicio:
                continue

            repos_mes = df_r_sku[df_r_sku['mes'] == mes]['cantidad'].astype(float).sum()
            stock_final = stock_inicial + repos_mes - forecast_val
            unidades_perdidas = max(0, -stock_final)
            stock_final = max(0, stock_final)
            perdida_euros = unidades_perdidas * precio_unitario

            resultados.append({
                'sku': sku,
                'mes': mes.strftime('%Y-%m'),
                'stock_inicial_mes': stock_inicial,
                'repos_aplicadas': int(repos_mes),
                'forecast': forecast_val,
                'stock_final_mes': int(stock_final),
                'unidades_perdidas': int(unidades_perdidas),
                'perdida_proyectada_euros': round(perdida_euros, 1)
            })

            stock_inicial = stock_final

    return pd.DataFrame(resultados)


