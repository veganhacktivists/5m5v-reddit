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

    cursor = mydb.cursor()

    def display_table(self, table_name):
        sql = f"select * from {table_name};"
        MyTestCase.cursor.execute(sql)
        for row in MyTestCase.cursor.fetchall():
            print(row)

    def test_run(self):
        reddit = Reddit5m5v("veganactivismbot", MyTestCase.mydb, test=True)
        reddit.main()
        self.display_table("reddit")
        self.display_table("reddit_comments")


if __name__ == '__main__':
    unittest.main()
