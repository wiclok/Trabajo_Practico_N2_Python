import csv  # Módulo para leer y escribir archivos CSV.
import sys   # Módulo para acceder a variables y funciones del intérprete de Python.
import MySQLdb  # Módulo para interactuar con bases de datos MySQL.
import os    # Módulo para trabajar con el sistema operativo (archivos, directorios).
import shutil # Módulo para copiar, mover y eliminar archivos y directorios.


def conectar_bd(host, user, password, database):
  
    """
    Esta función establece la conexión con una base de datos MySQL.

    Argumentos:
        host: Dirección del servidor MySQL.
        user: Nombre de usuario para la conexión a la base de datos.
        password: Contraseña para la conexión a la base de datos.
        database: Nombre de la base de datos a la que se desea conectar.

    Devoluciones:
        Objeto de conexión a la base de datos MySQL o None si falla la conexión.
    """
    
    try:
        db = MySQLdb.Connect(host=host, user=user, password=password, db=database)
        print('Conexión a la base de datos establecida correctamente.')
        return db
    except MySQLdb.Error as err:
        print("Error en la conexión a la base de datos:", err)
        sys.exit(1)

def leer_csv(nombre_archivo):
  
    """
    Esta función lee un archivo CSV y extrae los datos de las filas y columnas.

    Argumentos:
        nombre_archivo: Ruta del archivo CSV a leer.

    Devoluciones:
        Cabecera (lista de nombres de columnas) del CSV y lista de localidades (diccionarios con datos de las filas).
    """
    
    with open(nombre_archivo, 'r', newline='') as archivo:
        lector = csv.reader(archivo, delimiter=',', quotechar='"')
        cabecera = next(lector)
        localidades = []
        for fila in lector:
            loc = {}
            for i in range(len(cabecera)):
                loc[cabecera[i]] = fila[i]
            localidades.append(loc)
        print('CSV leído correctamente.')
        return cabecera, localidades

def crear_tabla(cursor):
  
    """
    Esta función crea una tabla llamada "localidades" en la base de datos conectada.

    Argumentos:
        cursor: Objeto cursor de la conexión a la base de datos.
    """
    
    eliminar_tabla_si_existe = "DROP TABLE IF EXISTS localidades"
    crear_tabla_localidades = "CREATE TABLE localidades (provincia VARCHAR(255), id INT, localidad VARCHAR(255), cp VARCHAR(10), id_prov_mstr INT)"
    cursor.execute(eliminar_tabla_si_existe)
    cursor.execute(crear_tabla_localidades)
    print('Tabla creada sin interrupciones.')
    
def insertar_datos(cursor, localidades):
  
    """
    Esta función inserta los datos de las localidades en la tabla "localidades" de la base de datos.

    Argumentos:
        cursor: Objeto cursor de la conexión a la base de datos.
        localidades: Lista de diccionarios con los datos de las localidades (extraídos del CSV).
    """
    
    insertar_datos = 'INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)'
    for loc in localidades:
        valores = [loc["provincia"], loc["id"], loc["localidad"], loc["cp"], loc["id_prov_mstr"]]
        cursor.execute(insertar_datos, valores)
    cursor.connection.commit()
    print("Registros insertados con éxito.")

def exportar_csv_por_provincia(cursor, cabecera):
  
    """
    Esta función genera archivos CSV por provincia, agrupando las localidades y contando la cantidad de cada una.

    Argumentos:
        cursor: Objeto cursor de la conexión a la base de datos.
        cabecera: Lista de nombres de columnas del CSV.
    """
    
    seleccionar_todas_las_provincias = "SELECT provincia AS 'Provincia', COUNT(*) AS 'Localidades' FROM `localidades` GROUP BY provincia"
    cursor.execute(seleccionar_todas_las_provincias)
    provincias_y_localidades = dict(cursor.fetchall())
    todas_las_provincias = list(provincias_y_localidades.keys())

    seleccionar_localidades = 'SELECT * FROM localidades WHERE provincia = %s'

    for prov in todas_las_provincias:
        cursor.execute(seleccionar_localidades, [prov])
        localidades_provincia = cursor.fetchall()

        with open(f'localidades_y_provincias/{prov}.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(cabecera)
            writer.writerows(localidades_provincia)
            writer.writerow(["Total de localidades: " + str(provincias_y_localidades[prov])])

        print("CSVs creados con éxito.")
        
def ejecutar_programa():
  
    """
    Función principal que ejecuta el programa completo.
    """
    
    directorio_salida = 'localidades_y_provincias'

    if os.path.exists(directorio_salida):
        shutil.rmtree(directorio_salida)
        print(f'Directorio "{directorio_salida}" eliminado correctamente.')

    os.makedirs(directorio_salida)
    print(f'Directorio "{directorio_salida}" creado correctamente.')

    try:
        db = conectar_bd(host='localhost', user='root', password='', database='bdlocalidades')
    except Exception as e:
        print("Error: ", e)
        sys.exit(1)

    archivo_csv = 'localidades.csv'

    cabecera, localidades = leer_csv(archivo_csv)

    cursor = db.cursor()

    try:
        crear_tabla(cursor)
        insertar_datos(cursor, localidades)
        exportar_csv_por_provincia(cursor, cabecera)
    except Exception as e:
        print("Error: ", e)
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    ejecutar_programa()