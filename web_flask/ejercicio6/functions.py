from flask import Flask, render_template
import pandas as pd
import sqlite3
import matplotlib
matplotlib.use('Agg')  # para evitar errores en Flask

import matplotlib.pyplot as plt
import os
import seaborn as sns
import numpy as np

DATABASE = 'ejercicio2.db'
def generar_grafico_media_tiempo():

    # Consulta y procesamiento de datos
    con = sqlite3.connect(DATABASE)
    query = """
        SELECT es_mantenimiento, fecha_apertura, fecha_cierre
        FROM Tickets_emitidos
    """
    df = pd.read_sql_query(query, con)
    con.close()

    # Calculo de tiempos
    df['tiempo_horas'] = (pd.to_datetime(df['fecha_cierre']) -
                          pd.to_datetime(df['fecha_apertura'])).dt.total_seconds() / 3600 / 24
    df = df[df['tiempo_horas'] >= 0]

    # Creación del gráfico
    plt.figure(figsize=(8, 6))
    media_tiempo = df.groupby('es_mantenimiento')['tiempo_horas'].mean().reset_index()

    plt.bar(
        media_tiempo['es_mantenimiento'].map({'True': 'Mantenimiento', 'False': 'No mantenimiento'}),
        media_tiempo['tiempo_horas'],
        color=['#1f77b4', '#ff7f0e']
    )
    plt.title('Media de tiempo de resolución')
    plt.xlabel('Tipo de incidente')
    plt.ylabel('Horas promedio')
    plt.ylim(0, 5)

    # Guardado
    img_path = guardar_imagen(plt, 'tiempo_medio_mantenimiento.png')
    return img_path


# Función para el segundo gráfico
def generar_boxplot_tipos_incidente():

    # Consulta con JOIN para obtener nombres de tipos
    con = sqlite3.connect(DATABASE)
    query = """
        SELECT t.*, ti.nombre as tipo_incidente 
        FROM Tickets_emitidos t
        JOIN Tipos_Incidentes ti ON t.tipo_incidencia = ti.id_inci
    """
    df = pd.read_sql_query(query, con)
    con.close()

    # Calculo de tiempos
    df['tiempo_horas'] = (pd.to_datetime(df['fecha_cierre']) -
                          pd.to_datetime(df['fecha_apertura'])).dt.total_seconds() / 3600
    df = df[df['tiempo_horas'] >= 0]

    # Configuración del boxplot
    plt.figure(figsize=(12, 6))
    sns.boxplot(
        x='tipo_incidente',
        y='tiempo_horas',
        data=df,
        whis=[5, 95],  # Percentiles 5% y 95%
        showfliers=False,
        palette='Set2'
    )
    plt.title('Distribución de tiempos por tipo de incidente')
    plt.xlabel('Tipo de incidente')
    plt.ylabel('Horas')
    plt.xticks(rotation=45, ha='right')

    # Guardado
    img_path = guardar_imagen(plt, 'boxplot_tipos.png')
    return img_path


def generar_grafico_clientes_criticos():

    # Consulta SQL para clientes críticos
    con = sqlite3.connect(DATABASE)
    query = """
        SELECT 
            cliente,
            COUNT(*) as total_incidentes
        FROM Tickets_emitidos
        WHERE 
            es_mantenimiento = 'True' 
            AND tipo_incidencia != 1
        GROUP BY cliente
        ORDER BY total_incidentes DESC
        LIMIT 5;
    """
    df = pd.read_sql_query(query, con)
    con.close()

    # Crear gráfico de barras
    plt.figure(figsize=(10, 6))
    bars = plt.barh(
        df['cliente'],
        df['total_incidentes'],
        color='#d9534f'
    )

    plt.title('Top 5 Clientes Críticos')
    plt.xlabel('Número de incidentes')
    plt.ylabel('Clientes')
    plt.gca().invert_yaxis()  # Mostrar el cliente con más incidentes arriba
    plt.grid(axis='x', linestyle='--', alpha=0.7)

    # Añadir etiquetas de valor
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.2,
                 bar.get_y() + bar.get_height() / 2,
                 f'{int(width)}',
                 va='center')

    # Guardar imagen
    img_path = guardar_imagen(plt, 'clientes_criticos.png')
    return img_path


def generar_grafico_actuaciones_empleados():

    # Consulta SQL para obtener actuaciones
    con = sqlite3.connect(DATABASE)
    query = """
        SELECT 
            e.id_emp,
            e.nombre as empleado,
            COUNT(c.id_emp) as total_actuaciones
        FROM Contactos_con_empleados c
        JOIN Empleados e ON c.id_emp = e.id_emp
        GROUP BY e.id_emp
        ORDER BY total_actuaciones DESC;
    """
    df = pd.read_sql_query(query, con)
    con.close()

    plt.figure(figsize=(12, 6))
    bars = plt.barh(
        df['empleado'],
        df['total_actuaciones'],
        color='#2c7be5'
    )

    plt.title('Actuaciones por Empleado')
    plt.xlabel('Número de actuaciones')
    plt.ylabel('Empleados')
    plt.gca().invert_yaxis()
    plt.grid(axis='x', linestyle='--', alpha=0.6)

    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1,
                 bar.get_y() + bar.get_height() / 2,
                 f'{int(width)}',
                 va='center')

    # Guardar imagen
    img_path = guardar_imagen(plt, 'actuaciones_empleados.png')
    return img_path


def generar_grafico_actuaciones_diarias():

    # Consulta SQL para obtener actuaciones por día
    con = sqlite3.connect(DATABASE)
    query = """
        SELECT 
            strftime('%w', fecha) as dia,
            COUNT(*) as total_actuaciones
        FROM Contactos_con_empleados
        GROUP BY dia
    """
    df = pd.read_sql_query(query, con)
    con.close()

    # Mapear numeros a nombres de dias en español
    dias_dict = {
        '0': 'Domingo',
        '1': 'Lunes',
        '2': 'Martes',
        '3': 'Miércoles',
        '4': 'Jueves',
        '5': 'Viernes',
        '6': 'Sábado'
    }

    df['dia_semana'] = df['dia'].map(dias_dict)

    # Ordenar dias correctamente
    orden_dias = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    df = df.set_index('dia_semana').reindex(orden_dias).reset_index().fillna(0)

    # Crear gráfico de barras
    plt.figure(figsize=(10, 6))
    bars = plt.bar(
        df['dia_semana'],
        df['total_actuaciones'],
        color="#21bd40",
        edgecolor='black'
    )

    plt.title('Actuaciones por día de la semana')
    plt.xlabel('Día de la semana')
    plt.ylabel('Número de actuaciones')
    plt.xticks(rotation=30)
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Añadir valores en las barras
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height,
                 f'{int(height)}',
                 ha='center', va='bottom')

    # Guardar imagen
    img_path = guardar_imagen(plt, 'actuaciones_diarias.png')
    return img_path


# Funcion auxiliar para guardar imagenes
def guardar_imagen(plt, filename):
    img_dir = 'img'
    if not os.path.exists(img_dir):
        os.makedirs(img_dir)
    path = os.path.join(img_dir, filename)
    plt.savefig(path, bbox_inches='tight')
    plt.close()
    return filename