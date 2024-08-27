import pandas as pd
import json
import psycopg2
from powerbiclient import QuickVisualize, get_dataset_config, Report
from powerbiclient.authentication import DeviceCodeLoginAuthentication

# Función para crear la conexión a la base de datos
def create_connection():
    try:
        with open('credentials.json') as f:
            credentials = json.load(f)
        
        user = credentials['user']
        password = credentials['password']
        host = credentials['host']
        port = credentials['port']
        database = 'workshop_01'
        
        connection = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("¡Conexión exitosa!")
        return connection
    except psycopg2.OperationalError as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

# Crear la conexión a la base de datos
connection = create_connection()

# Verificar la conexión y obtener datos
if connection is not None:
    try:
        with connection.cursor() as cursor:
            # Ejecutar la consulta para obtener datos de la tabla 'workshop_eda'
            cursor.execute("SELECT * FROM workshop_eda")
            records = cursor.fetchall()
            
            # Obtener los nombres de las columnas
            column_names = [desc[0] for desc in cursor.description]
            
            # Crear el DataFrame
            connectionpostgres = pd.DataFrame(records, columns=column_names)
            
            # Cerrar la conexión
            connection.close()
            
            # Verificar los primeros registros del DataFrame
            print(connectionpostgres.head())
            print(connectionpostgres.dtypes)
            
    except psycopg2.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
else:
    print("No se pudo establecer la conexión a la base de datos.")

# Convertir columna 'application_date' a datetime, si existe
if 'application_date' in connectionpostgres.columns:
    connectionpostgres['application_date'] = pd.to_datetime(connectionpostgres['application_date'], format='%Y-%m-%d')
    connectionpostgres['year'] = connectionpostgres['application_date'].dt.year

# Autenticación para Power BI
device_auth = DeviceCodeLoginAuthentication()

# Crear el visualizador de Power BI
PBI_visualize = QuickVisualize(get_dataset_config(connectionpostgres), auth=device_auth)

# Visualizar los datos y crear reportes interactivos
PBI_visualize

# Usar los IDs del grupo y del reporte para visualizar tu reporte en Power BI
group_id = '6e639392-873a-4246-8180-ebf0059a2c8f'  # ID del área de trabajo
report_id = 'eacfa305-26fb-404a-98a2-3ff0a8d79442'  # ID del informe
dataset_id = '56f862a3-8bdf-4927-a153-ed3a5ebee694'  # ID del conjunto de datos

try:
    report = Report(group_id=group_id, report_id=report_id, auth=device_auth)
    print("Reporte creado exitosamente.")
except Exception as e:
    print(f"Error al crear el reporte: {e}")
