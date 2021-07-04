from .database import Database
import mysql.connector as connection
import csv
import logging as lg

class Mysql_Database(Database):
    def create_schema(self, host, username, password, schema_name):

        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True)
            query = f"CREATE DATABASE IF NOT EXISTS {schema_name}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.close()
            lg.info("Database Created")
            return ("Database Created")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def drop_schema(self, host, username, password, schema_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True)
            query = f"DROP DATABASE {schema_name};"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.close()
            lg.info("Database dropped")
            return("Database dropped")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            variables = ""
            for key, value in columns.items():
                if len(variables) != 0:
                    variables += ', '
                variables += key + " " + value
            query = f"""CREATE TABLE IF NOT EXISTS {table_name}  
                        (  
                        {variables}
                        )  """
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.close()
            lg.info(f"{table_name} table created")
            return(f"{table_name} table created")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            query = f"DROP TABLE {table_name}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.close()
            lg.info("Table dropped")
            return("Table dropped")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            column_names = ""
            column_values = ""
            for key, value in columns.items():
                if len(column_names) != 0:
                    column_names += ', '
                    column_values += ', '
                column_names += key
                column_values += f"'{value}'"

            query = f"""INSERT INTO {table_name}  ({column_names})
                        VALUES ({column_values}) """
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.commit()
            mydb.close()
            lg.info("Value Inserted")
            return("Value Inserted")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:

            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            cursor = mydb.cursor()
            column_names = ','.join(columns)
            with open(input_file_name, "r") as data:
                next(data)
                csv_data = csv.reader(data, delimiter="\n")
                for i in csv_data:
                    values = ""
                    for j in i[0].split(","):
                        if len(values) !=0:
                            values += ","
                        values += f"'{j}'"
                    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values}) "
                    lg.info(query)
                    cursor.execute(query)
            mydb.commit()
            mydb.close()
            lg.info("Value Inserted")
            return("Value Inserted")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            new_values = ""
            for key, value in updated_values.items():
                if len(new_values) != 0:
                    new_values += ', '
                new_values += f"{key}='{value}'"
            search_filters = ""
            for key, value in filters.items():
                if len(search_filters) != 0:
                    search_filters += ' OR '
                search_filters += f"{key}='{value}'"

            query = f"UPDATE {table_name} SET {new_values} WHERE {search_filters}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.commit()
            mydb.close()
            lg.info("Value Updated")
            return("Value Updated")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            query = f"SELECT * FROM {table_name}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            mydb.close()
            lg.info (result)
            return(result)
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            search_filters = ""
            for key, value in filters.items():
                if len(search_filters) != 0:
                    search_filters += ' OR '
                search_filters += f"{key} = '{value}'"
            query = f"SELECT * FROM {table_name} WHERE {search_filters}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            mydb.close()
            lg.info(result)
            return result
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            search_filters = ""
            for key, value in filters.items():
                if len(search_filters) != 0:
                    search_filters += ' OR '
                search_filters += f"{key} = '{value}'"
            query = f"DELETE FROM {table_name} WHERE {search_filters}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.commit()
            mydb.close()
            lg.info(f"Deleted Records having {search_filters}")
            return(f"Deleted Records having {search_filters}")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            query = f"DELETE FROM {table_name}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.commit()
            mydb.close()
            lg.info(f"Deleted Records")
            return(f"Deleted Records")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, database=schema_name, use_pure=True)
            query = f"TRUNCATE TABLE {table_name}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.close()
            lg.info("All Records Deleted using truncate")
            return("All Records Deleted using truncate")
        except Exception as e:
            mydb.close()
            lg.info(str(e))
            return(str(e))
