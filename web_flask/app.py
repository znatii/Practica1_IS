from flask import Flask, render_template
import pandas as pd
import sqlite3
from datetime import datetime

# Crear la aplicación Flask
app = Flask(__name__)

# Conexión a la base de datos
DATABASE = 'ejercicio2.db'

# Función para ejecutar las consultas de pandas
def obtener_resultados():
    con = sqlite3.connect(DATABASE)

    # 1. Número de muestras totales
    query_muestras_totales = "SELECT COUNT(*) AS total_muestras FROM Tickets_emitidos;"
    df_muestras_totales = pd.read_sql_query(query_muestras_totales, con)
    total_muestras = df_muestras_totales['total_muestras'].iloc[0]

    # 2. Media y desviación estándar de los valores de satisfacción >= 5
    query_satisfaccion = """
        SELECT 
            satisfaccion_cliente
        FROM 
            Tickets_emitidos
        WHERE 
            satisfaccion_cliente >= 5;
    """
    df_satisfaccion = pd.read_sql_query(query_satisfaccion, con)
    media_satisfaccion = df_satisfaccion['satisfaccion_cliente'].mean()
    std_satisfaccion = df_satisfaccion['satisfaccion_cliente'].std()

    # 3. Media y desviación estándar del número de incidentes por cliente
    query_incidentes_por_cliente = """
        SELECT 
            cliente, 
            COUNT(*) AS total_incidentes
        FROM 
            Tickets_emitidos
        GROUP BY 
            cliente;
    """
    df_incidentes_por_cliente = pd.read_sql_query(query_incidentes_por_cliente, con)
    media_incidentes_por_cliente = df_incidentes_por_cliente['total_incidentes'].mean()
    std_incidentes_por_cliente = df_incidentes_por_cliente['total_incidentes'].std()

    # 4. Media y desviación estándar del número de horas totales realizadas en cada incidente
    query_horas_por_incidente = """
        SELECT 
            id_tick, 
            SUM(tiempo) AS total_horas
        FROM 
            Contactos_con_empleados
        GROUP BY 
            id_tick;
    """
    df_horas_por_incidente = pd.read_sql_query(query_horas_por_incidente, con)
    media_horas_por_incidente = df_horas_por_incidente['total_horas'].mean()
    std_horas_por_incidente = df_horas_por_incidente['total_horas'].std()

    # 5. Valor mínimo y máximo del total de horas realizadas por los empleados
    query_horas_empleados = """
        SELECT 
            id_emp, 
            SUM(tiempo) AS total_horas
        FROM 
            Contactos_con_empleados
        GROUP BY 
            id_emp;
    """
    df_horas_empleados = pd.read_sql_query(query_horas_empleados, con)
    min_horas_empleados = df_horas_empleados['total_horas'].min()
    max_horas_empleados = df_horas_empleados['total_horas'].max()

    # 6. Valor mínimo y máximo del tiempo entre apertura y cierre de incidente
    query_tiempo_apertura_cierre = """
        SELECT 
            id_tick, 
            (julianday(fecha_cierre) - julianday(fecha_apertura)) * 24 AS horas_entre_apertura_cierre
        FROM 
            Tickets_emitidos;
    """
    df_tiempo_apertura_cierre = pd.read_sql_query(query_tiempo_apertura_cierre, con)
    min_tiempo_apertura_cierre = df_tiempo_apertura_cierre['horas_entre_apertura_cierre'].min()
    max_tiempo_apertura_cierre = df_tiempo_apertura_cierre['horas_entre_apertura_cierre'].max()

    # 7. Valor mínimo y máximo del número de incidentes atendidos por cada empleado
    query_incidentes_por_empleado = """
        SELECT 
            id_emp, 
            COUNT(*) AS total_incidentes
        FROM 
            Contactos_con_empleados
        GROUP BY 
            id_emp;
    """
    df_incidentes_por_empleado = pd.read_sql_query(query_incidentes_por_empleado, con)
    min_incidentes_por_empleado = df_incidentes_por_empleado['total_incidentes'].min()
    max_incidentes_por_empleado = df_incidentes_por_empleado['total_incidentes'].max()

    # Cerrar la conexión a la base de datos
    con.close()

    # Devolver los resultados
    return {
        'total_muestras': total_muestras,
        'media_satisfaccion': media_satisfaccion,
        'std_satisfaccion': std_satisfaccion,
        'media_incidentes_por_cliente': media_incidentes_por_cliente,
        'std_incidentes_por_cliente': std_incidentes_por_cliente,
        'media_horas_por_incidente': media_horas_por_incidente,
        'std_horas_por_incidente': std_horas_por_incidente,
        'min_horas_empleados': min_horas_empleados,
        'max_horas_empleados': max_horas_empleados,
        'min_tiempo_apertura_cierre': min_tiempo_apertura_cierre,
        'max_tiempo_apertura_cierre': max_tiempo_apertura_cierre,
        'min_incidentes_por_empleado': min_incidentes_por_empleado,
        'max_incidentes_por_empleado': max_incidentes_por_empleado,
    }

# Ruta principal
@app.route('/')
def index():
    # Obtener los resultados
    resultados = obtener_resultados()

    # Renderizar la plantilla HTML con los resultados
    return render_template('index.html', resultados=resultados)

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=True)
