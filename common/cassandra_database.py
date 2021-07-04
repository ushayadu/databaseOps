from .database import Database
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import logging as lg

class Cassandra_Database(Database):

    def create_schema(self, host, username, password, schema_name):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            json = "{'class' : 'SimpleStrategy', 'replication_factor' : 2 }"
            query = f"CREATE KEYSPACE IF NOT EXISTS {schema_name} WITH REPLICATION = {json}"
            lg.info(query)
            session.execute(query)
            lg.info("Keyspace Created Successfully!!!")
            return ("Keyspace Created Successfully!!!")
        except Exception as error:
            lg.error(error)
            return error

    def drop_schema(self, host, username, password, schema_name):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            query = f"DROP KEYSPACE IF EXISTS {schema_name}"
            lg.info(query)
            session.execute(query)
            lg.info("Keyspace Dropped Successfully!!!")
            return("Keyspace Dropped Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            values = ""
            for key, value in columns.items():
                if len(values) != 0:
                    values += ','
                values += key + " " + value
            query = f"CREATE TABLE {table_name} ({values})"
            lg.info(query)
            session.execute(query)
            lg.info("Table Created Successfully!!!")
            return("Table Created Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            query=f"DROP TABLE {table_name}"
            lg.info(query)
            session.execute(query)
            lg.info("Table Dropped Successfully!!!")
            return("Table Dropped Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            col_name = ""
            col_value = ""
            for key, value in columns.items():
                if len(col_name) != 0:
                    col_name += ','
                    col_value += ','
                col_name += key
                col_value += f"'{value}'"
            query = f"INSERT INTO employee ({col_name}) VALUES ({col_value})"
            lg.info(query)
            session.execute(query)
            lg.info("Record Inserted Successfully!!!")
            return("Record Inserted Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            column_names = ','.join(columns)
            with open(input_file_name, "r") as data:
                next(data)
                csv_data = csv.reader(data, delimiter="\n")
                for i in csv_data:
                    column_values = ""
                    for val in i[0].split(","):
                        if len(column_values) != 0:
                            column_values += ','
                        column_values += f"'{val}'"
                    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({column_values}) "
                    lg.info(query)
                    session.execute(query)
            lg.info("Record Inserted Successfully!!!")
            return("Record Inserted Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            col_name = ""
            for key, value in updated_values.items():
                if len(col_name)!=0:
                    col_name += ","
                col_name += f"{key}='{value}'"
            col_fil = ""
            for key, value in filters.items():
                if len(col_fil) != 0:
                    col_fil += " AND "
                col_fil += f"{key} ='{value}'"
            query = f"UPDATE {table_name} SET {col_name} WHERE {col_fil}"
            lg.info(query)
            session.execute(query)
            lg.info("Record Updated Successfully!!!")
            return("Record Updated Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            query = f"SELECT * FROM {table_name}"
            lg.info(query)
            rows = session.execute(query)
            for row in rows:
                lg.info(row)
            lg.info("Record Retrieved Successfully!!!")
            return("Record Retrieved Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            col_fil = ""
            for key, value in filters.items():
                if len(col_fil) != 0:
                    col_fil += " AND "
                col_fil += f"{key} ='{value}'"
            query = f"SELECT * FROM {table_name} WHERE {col_fil} ALLOW FILTERING"
            lg.info(query)
            rows = session.execute(query)
            for row in rows:
                lg.info(row)
            lg.info("Record Retrieved Successfully!!!")
            return ("Record Retrieved Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            col_fil = ""
            for key, value in filters.items():
                if len(col_fil) != 0:
                    col_fil += " AND "
                col_fil += f"{key} ='{value}'"
            query =f"DELETE FROM {table_name} WHERE {col_fil}"
            lg.info(query)
            session.execute(query)
            lg.info("Record Deleted Successfully!!!")
            return("Record Deleted Successfully!!!")
        except Exception as error:
            lg.error(error)
            return(error)

    def delete_records(self, host, username, password, schema_name, table_name):
        lg.info("Please use delete_records_with_filter method")
        return("Please use delete_records_with_filter method")

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster([host], auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(schema_name)
            query = f"TRUNCATE {table_name}"
            lg.info(query)
            session.execute(query)
            lg.info("Table Truncated Successfully!!!")
            return "Table Truncated Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error


# host = "localhost"
# username = "cassandra"
# password = "cassandra"
# cas = Cassandra_Database()
# cas.create_schema(host,username,password,"Test")