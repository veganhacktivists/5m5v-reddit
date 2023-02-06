import unittest
import mysql.connector
import yaml
from reddit_5m5v import Reddit5m5v


class MyTestCase(unittest.TestCase):
    with open("config.yaml", "rb") as f:
        config = yaml.safe_load(f)

    mydb = mysql.connector.connect(
        host=config["mysql_host"],
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=config["mysql_database"])

    cursor = mydb.cursor(buffered=True)

    def display_table(self, table_name):
        sql = f"SELECT * FROM {table_name}"
        MyTestCase.cursor.execute(sql)
        for row in MyTestCase.cursor.fetchall():
            print(row)

    def test_sql_injection(self):
        sql = '''CREATE TABLE EMPLOYEE(
           FIRST_NAME CHAR(20) NOT NULL,
           LAST_NAME CHAR(20),
           AGE INT,
           SEX CHAR(1),
           INCOME FLOAT
        )'''

        MyTestCase.cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
        MyTestCase.cursor.execute(sql)

        first_name = "--;"
        last_name = "sailor"
        age = 60
        sex = "F"
        income = 60000
        sql = f"""
            INSERT INTO EMPLOYEE 
            (FIRST_NAME, LAST_NAME, AGE, SEX, INCOME)
            VALUES (%s, %s, %s, %s, %s)"""

        MyTestCase.cursor.execute(
            sql,
            (first_name, last_name, age, sex, income))
        MyTestCase.mydb.commit()

        self.display_table("EMPLOYEE")

        bad_sql = f"""
            INSERT INTO EMPLOYEE 
            (FIRST_NAME, LAST_NAME, AGE, SEX, INCOME)
            VALUES ({first_name}, {last_name}, {age}, {sex}, {income})"""

        MyTestCase.cursor.execute(
            bad_sql)
        MyTestCase.mydb.commit()

        self.display_table("EMPLOYEE")

        MyTestCase.cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

    def test_run(self):
        reddit = Reddit5m5v("veganactivismbot", MyTestCase.mydb, test=True)
        reddit.main()
        self.display_table("reddit")
        self.display_table("reddit_comments")


if __name__ == '__main__':
    unittest.main()
