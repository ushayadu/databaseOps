from flask import Flask, jsonify, request
from common.mysql_database import Mysql_Database
from common.cassandra_database import Cassandra_Database
from common.mongo_database import Mongo_Database
import json
import logging as lg
import os
app = Flask(__name__)

mysqldb = Mysql_Database()
mycadb = Cassandra_Database()
mymodb = Mongo_Database()

if "logging" not in os.listdir():
    os.mkdir("logging")
lg.basicConfig(filename="logging/02_06_2021.log", level=lg.INFO, format="%(asctime)s-%(message)s")


@app.route("/", methods=["GET"])
def index():
    return jsonify("Hello World!")


@app.route("/mysql/create_schema", methods=["POST"])
def mysql_create_schema():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mysqldb.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/drop_schema", methods=["POST"])
def mysql_drop_schema():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mysqldb.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/create_table", methods=["POST"])
def mysql_create_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mysqldb.create_table(host, username, password, schema_name,table_name,**columns)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")


@app.route("/mysql/drop_table", methods=["POST"])
def mysql_drop_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysqldb.drop_table(host, username, password, schema_name,table_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/insert_record", methods=["POST"])
def mysql_insert_record():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mysqldb.insert_record(host, username, password, schema_name,table_name,**columns)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/insert_multiple_records", methods=["POST"])
def mysql_insert_multiple_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        file_name = request.values.get("input_file_name")
        columns = request.values.get("columns")
        columns = columns.replace('[','').replace(']','').replace("'","").split(",")
        print(type(columns))
        print(columns)
        result = mysqldb.insert_multiple_records(host, username, password, schema_name,table_name,file_name,*columns)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/update_records", methods=["POST"])
def mysql_update_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        filters = json.loads(filters)
        result = mysqldb.update_records(host, username, password, schema_name,table_name,filters,**columns)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/retrieve_records", methods=["POST"])
def mysql_retrieve_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysqldb.retrieve_records(host, username, password, schema_name,table_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/retrieve_records_with_filter", methods=["POST"])
def mysql_retrieve_records_with_filter():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        print(type(filters))
        filters = json.loads(filters)
        print(filters)
        result = mysqldb.retrieve_records_with_filter(host, username, password, schema_name,table_name,**filters)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")


@app.route("/mysql/delete_records_with_filter", methods=["POST"])
def mysql_delete_records_with_filter():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mysqldb.delete_records_with_filter(host, username, password, schema_name,table_name,**filters)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/delete_records", methods=["POST"])
def mysql_delete_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysqldb.delete_records(host, username, password, schema_name,table_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mysql/truncate_table", methods=["POST"])
def mysql_truncate_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mysqldb.truncate_table(host, username, password, schema_name,table_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/cassandra/create_schema", methods=["POST"])
def cassandra_create_schema():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mycadb.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/cassandra/drop_schema", methods=["POST"])
def cassandra_drop_schema():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mycadb.drop_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/cassandra/create_table", methods=["POST"])
def cassandra_create_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mycadb.create_table(host, username, password, schema_name, table_name, **columns)
        print(result)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/cassandra/drop_table", methods=["POST"])
def cassandra_drop_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mycadb.drop_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/cassandra/insert_record", methods=["POST"])
def cassandra_insert_record():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mycadb.insert_record(host, username, password, schema_name, table_name, **columns)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/cassandra/insert_multiple_records", methods=["POST"])
def cassandra_insert_multiple_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = columns.replace('[','').replace(']','').replace("'",'').split(",")
        file = request.values.get("input_file_name")
        result = mycadb.insert_multiple_records(host, username, password, schema_name, table_name,file, *columns)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/cassandra/update_records", methods=["POST"])
def cassandra_update_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        updated_values = request.values.get("updated_values")
        filters = json.loads(filters)
        updated_values = json.loads(updated_values)
        result = mycadb.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

@app.route("/cassandra/retrieve_records", methods=["POST"])
def cassandra_retrieve_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mycadb.retrieve_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

@app.route("/cassandra/retrieve_records_with_filter", methods=["POST"])
def cassandra_retrieve_records_with_filter():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mycadb.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")


@app.route("/cassandra/delete_records_with_filter", methods=["POST"])
def cassandra_delete_records_with_filter():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mycadb.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

@app.route("/cassandra/delete_records", methods=["POST"])
def cassandra_delete_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mycadb.delete_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")


@app.route("/cassandra/truncate_table", methods=["POST"])
def cassandra_truncate_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mycadb.truncate_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

#-----------------------------------------------------------------------------------------------------------------------

@app.route("/mongo/create_schema", methods=["POST"])
def mongo_create_schema():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mymodb.create_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mongo/drop_schema", methods=["POST"])
def mongo_drop_schema():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        result = mymodb.drop_schema(host, username, password, schema_name)
        return jsonify(result)
    else:
        return jsonify("This methood is not allowed!!!")

@app.route("/mongo/create_table", methods=["POST"])
def mongo_create_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mymodb.create_table(host, username, password, schema_name, table_name, **columns)
        print(result)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/mongo/drop_table", methods=["POST"])
def mongo_drop_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mymodb.drop_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/mongo/insert_record", methods=["POST"])
def mongo_insert_record():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = json.loads(columns)
        result = mymodb.insert_record(host, username, password, schema_name, table_name, **columns)
        print(result)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/mongo/insert_multiple_records", methods=["POST"])
def mongo_insert_multiple_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        columns = request.values.get("columns")
        columns = columns.replace('[','').replace(']','').replace("'",'').split(",")
        file = request.values.get("input_file_name")
        result = mymodb.insert_multiple_records(host, username, password, schema_name, table_name,file, *columns)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")

@app.route("/mongo/update_records", methods=["POST"])
def mongo_update_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        updated_values = request.values.get("updated_values")
        filters = json.loads(filters)
        updated_values = json.loads(updated_values)
        result = mymodb.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

@app.route("/mongo/retrieve_records", methods=["POST"])
def mongo_retrieve_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mymodb.retrieve_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

@app.route("/mongo/retrieve_records_with_filter", methods=["POST"])
def mongo_retrieve_records_with_filter():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mymodb.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")


@app.route("/mongo/delete_records_with_filter", methods=["POST"])
def mongo_delete_records_with_filter():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        filters = request.values.get("filters")
        filters = json.loads(filters)
        result = mymodb.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")

@app.route("/mongo/delete_records", methods=["POST"])
def mongo_delete_records():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mymodb.delete_records(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")


@app.route("/mongo/truncate_table", methods=["POST"])
def mongo_truncate_table():
    if request.method == "POST":
        host = request.values.get("host")
        username = request.values.get("username")
        password = request.values.get("password")
        schema_name = request.values.get("schema_name")
        table_name = request.values.get("table_name")
        result = mymodb.truncate_table(host, username, password, schema_name, table_name)
        return jsonify(result)
    else:
        return jsonify("This method is not supported!!!")


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000)
