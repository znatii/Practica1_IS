from flask import Flask, render_template
import pandas as pd
import sqlite3
from datetime import datetime

app = Flask(__name__)

# Conexión a la base de datos
DATABASE = 'ejercicio2.db'

# Función para el Ejercicio 4
def obtener_resultados_ejercicio_4():
    con = sqlite3.connect(DATABASE)

    # muestras totales
    query_muestras_totales = "SELECT COUNT(*) AS total_muestras FROM Tickets_emitidos;"
    df_muestras_totales = pd.read_sql_query(query_muestras_totales, con)
    total_muestras = df_muestras_totales['total_muestras'].iloc[0]

    #Media y desviación estándar de los valores de satisfacción >= 5
    query_satisfaccion = """ SELECT 
            satisfaccion_cliente
        FROM 
            Tickets_emitidos
        WHERE 
            satisfaccion_cliente >= 5;
    """
    df_satisfaccion = pd.read_sql_query(query_satisfaccion, con)
    media_satisfaccion = df_satisfaccion['satisfaccion_cliente'].mean()
    std_satisfaccion = df_satisfaccion['satisfaccion_cliente'].std()

    # Media y desviación estándar del número de incidentes por cliente
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

    # Media y desviación estándar del número de horas totales realizadas en cada incidente
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

    #  Valor mínimo y máximo del total de horas realizadas por los empleados
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

    # Valor mínimo y máximo del tiempo entre apertura y cierre de incidente
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

    # Valor mínimo y máximo del número de incidentes atendidos por cada empleado
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

    con.close()

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


def obtener_resultados_ejercicio_5():
    con = sqlite3.connect(DATABASE)

    tickets_df = pd.read_sql_query("SELECT * FROM Tickets_Emitidos", con)
    contactos_df = pd.read_sql_query("SELECT * FROM Contactos_Con_Empleados", con)
    empleados_df = pd.read_sql_query("SELECT * FROM Empleados", con)
    tipos_incidentes_df = pd.read_sql_query("SELECT * FROM Tipos_Incidentes", con)

    tickets_df["fecha_apertura"] = pd.to_datetime(tickets_df["fecha_apertura"])
    tickets_df["fecha_cierre"] = pd.to_datetime(tickets_df["fecha_cierre"])
    tickets_df["dia_semana"] = tickets_df["fecha_apertura"].dt.day_name()

    id_fraude = tipos_incidentes_df[tipos_incidentes_df["nombre"] == "Fraude"]["id_inci"].values[0]
    fraude_tickets = tickets_df[tickets_df["tipo_incidencia"] == id_fraude]

    # Obtener contactos asociados a fraudes
    fraude_contactos = contactos_df[contactos_df["id_tick"].isin(fraude_tickets["id_tick"])]

    # calcular estadísticas
    def calcular_estadisticas(df, columna):
        return {
            "Mediana": df[columna].median(),
            "Media": df[columna].mean(),
            "Varianza": df[columna].var(),
            "Minimo": df[columna].min(),
            "Maximo": df[columna].max(),
        }

    # Agrupación por empleado
    fraude_por_empleado = fraude_contactos.groupby("id_emp").agg(
        num_incidentes=("id_tick", "nunique"),
        num_actuaciones=("id_tick", "count"),
        tiempo_total=("tiempo", "sum")
    ).reset_index()


    # Agrupación por nivel de empleado
    fraude_nivel_empleado = fraude_contactos.merge(empleados_df, on="id_emp").groupby("nivel").agg(
        num_incidentes=("id_tick", "nunique"),
        num_actuaciones=("id_tick", "count"),
        tiempo_total=("tiempo", "sum")
    ).reset_index()

    # Agrupación por cliente
    fraude_por_cliente = fraude_tickets.groupby("cliente").agg(
        num_incidentes=("id_tick", "nunique"),  # Número de incidentes únicos
    ).reset_index()

    # Calcular el número de actuaciones (contactos) por cliente
    actuaciones_por_cliente = fraude_contactos.groupby("id_tick")["id_emp"].count().reset_index()
    actuaciones_por_cliente = actuaciones_por_cliente.rename(columns={"id_emp": "num_actuaciones"})

    # Combinar con los datos de fraude_por_cliente
    fraude_por_cliente = fraude_por_cliente.merge(
        fraude_tickets[["id_tick", "cliente"]].merge(actuaciones_por_cliente, on="id_tick")
        .groupby("cliente")["num_actuaciones"].sum().reset_index(),
        on="cliente"
    )


    # Agrupación por tipo de incidente
    fraude_por_tipo = fraude_tickets.groupby("tipo_incidencia").agg(
        num_incidentes=("id_tick", "nunique"),
        num_actuaciones=("id_tick", "count")
    ).reset_index()


    # Agrupación por día de la semana
    fraude_por_dia = fraude_tickets.groupby("dia_semana").agg(
        num_incidentes=("id_tick", "nunique"),  # Número de incidentes únicos
    ).reset_index()

    # Calcular el número de actuaciones (contactos) por día de la semana
    actuaciones_por_dia = fraude_contactos.groupby("id_tick")["id_emp"].count().reset_index()
    actuaciones_por_dia = actuaciones_por_dia.rename(columns={"id_emp": "num_actuaciones"})

    # Combinar con los datos de fraude_por_dia
    fraude_por_dia = fraude_por_dia.merge(
        fraude_tickets[["id_tick", "dia_semana"]].merge(actuaciones_por_dia, on="id_tick")
        .groupby("dia_semana")["num_actuaciones"].sum().reset_index(),
        on="dia_semana"
)


    con.close()
    return {
        'por_empleado': {
            'datos': fraude_por_empleado.to_dict('records'),
            'estadisticas': calcular_estadisticas(fraude_por_empleado, "tiempo_total")
        },
        'por_nivel_empleado': {
            'datos': fraude_nivel_empleado.to_dict('records'),
            'estadisticas': calcular_estadisticas(fraude_nivel_empleado, "tiempo_total")
        },
        'por_cliente': {
            'datos': fraude_por_cliente.to_dict('records'),
            'estadisticas': calcular_estadisticas(fraude_por_cliente, "num_actuaciones")
        },
        'por_tipo_incidente': {
            'datos': fraude_por_tipo.to_dict('records'),
            'estadisticas': calcular_estadisticas(fraude_por_tipo, "num_actuaciones")
        },
        'por_dia_semana': {
            'datos': fraude_por_dia.to_dict('records'),
            'estadisticas': calcular_estadisticas(fraude_por_dia, "num_actuaciones")
        }
    }



def obtener_resultados_ejercicio_6():
    # Aquí implementarás la lógica para el Ejercicio 6
    return {
        'mensaje': 'Resultados del Ejercicio 6 (pendiente de implementación).'
    }

# Ruta principal
@app.route('/')
def index():
    return render_template('index.html')

# Ruta para el Ejercicio 4
@app.route('/ejercicio4')
def ejercicio1():
    resultados = obtener_resultados_ejercicio_4()
    return render_template('ejercicio4.html', resultados=resultados)

# Ruta para el Ejercicio 5
@app.route('/ejercicio5')
def ejercicio2():
    resultados = obtener_resultados_ejercicio_5()
    return render_template('ejercicio5.html', resultados=resultados)

# Ruta para el Ejercicio 6
@app.route('/ejercicio6')
def ejercicio3():
    resultados = obtener_resultados_ejercicio_6()
    return render_template('ejercicio6.html', resultados=resultados)

# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(debug=True)