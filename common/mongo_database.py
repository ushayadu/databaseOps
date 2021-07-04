from .database import Database
import pymongo
import csv
import logging as lg


class Mongo_Database(Database):

    def create_schema(self, host, username, password, schema_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            lg.info("Schema Created Successfully!!!")
            return "Schema Created Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def drop_schema(self, host, username, password, schema_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            client.drop_database(schema_name)
            lg.info("Schema Dropped Successfully!!!")
            return "Schema Dropped Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            lg.info("Table Created Successfully!!!")
            return "Table Created Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            collection.drop()
            lg.info("Table Dropped Successfully!!!")
            return "Table Dropped Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            collection.insert_one(columns)
            lg.info("Record Inserted Successfully!!!")
            return "Record Inserted Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            list_of_records = list()
            with open(input_file_name, "r") as data:
                next(data)
                csv_data = csv.reader(data, delimiter="\n")
                for i in csv_data:
                    record = dict()
                    for j in range(len(i[0].split(','))):
                        record[columns[j]] = i[0].split(',')[j]
                    list_of_records.append(record)
            collection.insert_many(list_of_records)
            lg.info("Records Inserted Successfully!!!")
            return "Records Inserted Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            collection.update_many(filters, {"$set": updated_values})
            lg.info("Record Updated Successfully!!!")
            return "Record Updated Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            resultset = collection.find()
            for i in resultset:
                lg.info(i)
            lg.info("Record Retrieved Successfully!!!")
            return "Record Retrieved Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            resultset = collection.find(filters)
            for i in resultset:
                lg.info(i)
            lg.info("Record Retrieved Successfully!!!")
            return "Record Retrieved Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            collection.delete_many(filters)
            lg.info("Records Deleted Successfully!!!")
            return "Records Deleted Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            collection.delete_many({})
            lg.info("Record Deleted Successfully!!!")
            return "Record Deleted Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            if username not in (None, "") and password not in (None, ""):
                connection_string = f"mongodb://{username}:{password}@{host}:27017/"
            else:
                connection_string = f"mongodb://localhost:27017/"
            lg.info(connection_string)
            client = pymongo.MongoClient(connection_string)
            database = client[schema_name]
            collection = database[table_name]
            collection.remove()
            lg.info("Table Truncated Successfully!!!")
            return "Table Truncated Successfully!!!"
        except Exception as error:
            lg.error(error)
            return error
