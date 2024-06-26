# Trabajo_Practico_N2_Python

# Descripción del Programa

Este programa Python tiene como objetivo principal interactuar con una base de datos MySQL y archivos CSV para realizar diversas operaciones, como la creación de tablas, inserción de datos, y exportación de datos en archivos CSV agrupados por provincia.

## Funcionalidades

El programa consta de las siguientes funcionalidades principales:

1. **Conexión a la Base de Datos:** Establece una conexión con una base de datos MySQL proporcionando los datos de conexión como el host, nombre de usuario, contraseña y nombre de la base de datos.

2. **Lectura de Archivo CSV:** Lee un archivo CSV proporcionado y extrae los datos de las filas y columnas.

3. **Creación de Tabla en la Base de Datos:** Crea una tabla llamada "localidades" en la base de datos conectada, con una estructura específica.

4. **Inserción de Datos:** Inserta los datos de las localidades extraídos del archivo CSV en la tabla recién creada en la base de datos.

5. **Exportación de Datos por Provincia:** Genera archivos CSV por provincia, agrupando las localidades y contando la cantidad de cada una.

## Uso del Programa

Para ejecutar el programa, sigue estos pasos:

1. Asegúrate de tener instalados los módulos necesarios, como `csv`, `sys`, `MySQLdb`, `os`, y `shutil`.

2. Modifica los parámetros de conexión a la base de datos en la función `conectar_bd` con los valores correspondientes.

3. Asegúrate de tener un archivo CSV llamado `localidades.csv` en el mismo directorio que el programa, o modifica la variable `archivo_csv` con la ruta adecuada.

4. Ejecuta el programa desde la línea de comandos o desde tu entorno de desarrollo Python.


## Requisitos del Entorno

- Python 3.x
- Módulos: `csv`, `sys`, `MySQLdb`, `os`, `shutil`

