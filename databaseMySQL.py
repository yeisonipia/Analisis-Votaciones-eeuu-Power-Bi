import mysql.connector as connector
from mysql.connector import Error
import pandas as pd

def conexion():
    try:
        conn = connector.connect(host='localhost', user='root', password='')
        print("Conexión exitosa")
        return conn
    except Error as e:
        print(f'Error: {e}')

def crearBaseDatos():
    conn = conexion()
    cursor = conn.cursor()

    try:
        cursor.execute('CREATE DATABASE IF NOT EXISTS VotacionesEUU')
        print("Base de datos creada")
        cursor.execute('USE VotacionesEUU')

        election = '''
        CREATE TABLE IF NOT EXISTS Election(
            idElection INT NOT NULL AUTO_INCREMENT,
            year INT,
            democrat INT,
            republic INT,
            other INT,
            code_county VARCHAR(50),
            PRIMARY KEY(idElection)
        )
        '''

        county = '''
        CREATE TABLE IF NOT EXISTS County (
            idCounty    INT NOT NULL AUTO_INCREMENT,
            code_county VARCHAR(50),
            county varchar(50),
            population INT,
            area DECIMAL(10,2),
            PRIMARY KEY(idCounty)
            )
            
        '''

        print('Creando Tablas...')
        cursor.execute(county)
        cursor.execute(election)
        print("Tablas creadas")

        conn.commit()
        cursor.close()
        conn.close()
    except Error as e:
        print(f'Error: {e}')

def insertarDatos():
    conn = conexion()
    cursor = conn.cursor()
    cursor.execute('USE VotacionesEUU')

    def insertarCounties():
        try:
            data_counties = pd.read_excel('counties_clean.xlsx')
            # Clean the data
            
            data_counties['population'] = data_counties['population'].fillna(0).astype(int)
            data_counties['area'] = data_counties['area'].fillna(0).astype(float)
            i = 0
            for  d in data_counties['county']:
                cursor.execute('''INSERT INTO County(code_county, county, population, area) 
                                  VALUES (%s, %s, %s, %s)''', 
                               (str(data_counties['codecounty'][i]), 
                                str(data_counties['county'][i]), 
                                int(data_counties['population'][i]), 
                                float(data_counties['area'][i])))
                i += 1
            
            print("Datos insertados")
        except Error as e:
            print(f'Error: {e}')
        except Exception as e:
            print(f'Error: {e}')
        
    

    def insertarElections():
        try:
            cursor = conn.cursor()
            data_elections = pd.read_json('elections.json')
            i = 0
            for d in data_elections['year']:

                cursor.execute('''insert into Election(year, democrat, republic, other, code_county) values(%s,%s,%s,%s,%s)''',
                                (
                                int(data_elections['year'][i]),
                                int(data_elections['democrat'][i]),
                                int(data_elections['republic'][i]),
                                int(data_elections['other'][i]),
                                str(data_elections['codecounty'][i])
                                )
                                )
                i += 1
            print("Datos insertados")
        except Error as e:
            print(f'Error: {e}')
        except Exception as e:
            print(f'Error: {e}')
        


    insertarCounties()
    insertarElections()
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == '__main__':
    crearBaseDatos()
    insertarDatos()



