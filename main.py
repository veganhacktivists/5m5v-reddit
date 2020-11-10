import mysql.connector
import yaml

with open("config.yaml") as f:
    config = yaml.load(f)

mydb = mysql.connector.connect(
    host=config["mysql_host"],
    user=config["mysql_user"],
    password=config["mysql_password"],
    database=config["mysql_database"])

def update_table(db, table_name, column_names, values):
    mycursor = db.cursor()
    sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s' for i in values])})"
    mycursor.execute(sql, tuple(values))

    db.commit()

