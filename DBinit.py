import json
import sqlite3
def crear_tablas(con):
    cursorObj = con.cursor()
    cursorObj.execute("CREATE TABLE IF NOT EXISTS Empleados (id_emp INTEGER PRIMARY KEY, nombre text, nivel INTEGER, fecha_contrato DATE)")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS Clientes (id_cli INTEGER PRIMARY KEY, nombre text, telefono INTEGER, provincia text)")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS Tipos_incidencias (id_inci INTEGER PRIMARY KEY, nombre text)")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS Ticket_emitidos (id_tick INTEGER PRIMARY KEY, nombre text, fecha_apertura DATE, fecha_cierre DATE, es_mantenimiento text, satisfaccion_cliente INTEGER, tipo_incidencia INTEGER, FOREIGN KEY (tipo_incidencia) REFERENCES Tipos_incidentes(id_inci))")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS Contactos_con_empleados (id_tick INTEGER, id_emp INTEGER, fecha DATE, tiempo INTEGER, PRIMARY KEY (id_tick, id_emp), FOREIGN KEY (id_tick) REFERENCES Ticket_emitidos(id_tick), FOREIGN KEY (id_emp) REFERENCES Empleado(id_emp))")
    con.commit()



def insertarDatos(con):
    cursorObj = con.cursor()
    file = open('datos.json', 'r')
    datos = json.load(file)

    # Datos de las tablas
    tickets = datos['tickets_emitidos']
    clientes = datos['clientes']
    empleados = datos['empleados']
    tipos_incidentes = datos['tipos_incidentes']
    contactos_con_empleados = []

    # Tabla auxiliar
    id_tick = 1
    for ticket in tickets:
        contactos = ticket['contactos_con_empleados']
        for contacto in contactos:
            inp = {
                'id_tick': id_tick,
                'id_emp': contacto['id_emp'],
                'fecha': contacto['fecha'],
                'tiempo': contacto['tiempo']
            }
            contactos_con_empleados.append(inp)
        id_tick += 1

    # Insertamos
    # Empleados
    for empleado in empleados:
        cursorObj.execute("INSERT OR IGNORE INTO Empleados(id_emp, nombre, nivel, fecha_contrato)"\
                          "VALUES ('%d','%s','%s','%s')" %
                          (int(empleado['id_emp']), empleado['nombre'], empleado['nivel'], empleado['fecha_contrato']))
    # Clientes
    for cliente in clientes:
        cursorObj.execute("INSERT OR IGNORE INTO Clientes(id_cli, nombre, telefono, provincia)"\
                          "VALUES ('%d', '%s', '%d', '%s')" %
                          (int(cliente['id_cli']), cliente['nombre'], int(cliente['telefono']), cliente['provincia']))

    # Tipos incidencias

    # tickets emitidos

    # Contactos con empleados


    con.commit()



con = sqlite3.connect('ejercicio2.db')
crear_tablas(con)
insertarDatos(con)
con.close()
