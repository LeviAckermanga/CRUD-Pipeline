from flask import Flask, render_template, request, redirect, url_for
import pyodbc

app = Flask(__name__)

# Configura la conexión a la base de datos
SERVER = 'ws16-datafactory.westeurope.cloudapp.azure.com'
DATABASE = 'source_CDC'
USERNAME = 'azure'
PASSWORD = '123Dieg@.'

def get_db():
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    return pyodbc.connect(connection_string)

# Ruta principal para seleccionar la tabla
@app.route('/', methods=['GET', 'POST'])
def select_table():
    try:
        with get_db().cursor() as cursor:
            if request.method == 'POST':
                selected_table = request.form['table_name']
                return redirect(url_for('show_data', table=selected_table))

            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = cursor.fetchall()

        return render_template('select_table.html', tables=tables)
    except Exception as e:
        print(f"Error en la selección de la tabla: {e}")

# Ruta para mostrar datos
@app.route('/show_data/<table>', methods=['GET'])
def show_data(table):
    try:
        with get_db().cursor() as cursor:
            cursor.execute(f'SELECT * FROM {table}')
            data = cursor.fetchall()
            columns = [column[0] for column in cursor.description]

        return render_template('show_data.html', table=table, data=data, columns=columns)
    except Exception as e:
        print(f"Error al mostrar datos: {e}")

# Ruta para insertar datos
@app.route('/insert_data/<table>', methods=['GET', 'POST'])
def insert_data(table):
    try:
        if request.method == 'POST':
            # Obtener la información sobre las columnas de la tabla
            with get_db().cursor() as cursor:
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
                columns = cursor.fetchall()

            # Procesar los datos de inserción y realizar la operación en la base de datos
            # (Adaptar según la estructura de tu base de datos)
            form_data = {}
            for column in columns:
                column_name = column[0]
                form_data[column_name] = request.form[column_name]

            insert_query = f'INSERT INTO {table} ({", ".join(form_data.keys())}) VALUES ({", ".join(["?" for _ in form_data.values()])})'
            with get_db().cursor() as cursor:
                cursor.execute(insert_query, list(form_data.values()))

            return redirect(url_for('show_data', table=table))

        # Obtener la información sobre las columnas de la tabla para generar el formulario
        with get_db().cursor() as cursor:
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
            columns = cursor.fetchall()

        return render_template('insert_data.html', table=table, columns=columns)
    except Exception as e:
        print(f"Error al insertar datos: {e}")

# Ruta para actualizar datos
@app.route('/update_data/<table>', methods=['GET', 'POST'])
def update_data(table):
    try:
        with get_db().cursor() as cursor:
            cursor.execute(f'SELECT TOP 1 * FROM {table}')
            columns = [column[0] for column in cursor.description]

        if request.method == 'POST':
            try:
                # Obtener la columna de condición y su valor
                condition_column = request.form['condition_column']
                condition_value = request.form['condition_value']

                # Obtener la columna a actualizar y su nuevo valor
                column_to_update = request.form['column_to_update']
                new_value = request.form['new_value']

                # Construir la consulta de actualización de manera dinámica
                update_query = f'UPDATE {table} SET {column_to_update} = ? WHERE {condition_column} = ?'

                # Ejecutar la consulta de actualización
                with get_db().cursor() as cursor:
                    cursor.execute(update_query, (new_value, condition_value))

                return redirect(url_for('show_data', table=table))
            except Exception as e:
                print(f"Error al procesar los datos de actualización: {e}")
                raise  # Re-levanta la excepción para que podamos ver más detalles en la consola de Flask
        return render_template('update_data.html', table=table, columns=columns)
    except Exception as e:
        print(f"Error al actualizar datos: {e}")
        return f"Error al actualizar datos: {e}", 500


# Ruta para eliminar datos
@app.route('/delete_data/<table>', methods=['GET'])
def delete_data(table):
    try:
        # Realizar la operación de eliminación en la base de datos
        condition_column = request.args.get('condition_column')
        condition_value = request.args.get('condition_value')
        delete_query = f'DELETE FROM {table} WHERE {condition_column}=?'

        with get_db().cursor() as cursor:
            cursor.execute(delete_query, (condition_value,))

        return redirect(url_for('show_data', table=table))
    except Exception as e:
        print(f"Error al eliminar datos: {e}")

if __name__ == '__main__':
    app.run(debug=True)
