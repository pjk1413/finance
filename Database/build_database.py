from Data.config_read import config as get_values
import Database.database as database
from Database.database import database as database
import mysql.connector as connect

class build_database:
    def __init__(self):
        config = get_values()
        self.user = config.db_user
        self.host = config.db_host
        self.password = config.db_pass
        self.stock_db_name = config.stock_db_name
        self.sentiment_store_db_name = config.sentiment_db_name
        self.db_user_root = config.db_user_root
        self.db_root_pass = config.db_pass_root


    def build_database(self):
        self.root_prep()
        self.create_database()
        self.create_user()
        self.grant_all_user()
        self.flush_all()
        print("""
        ----------------------------------
        SUCCESSFULLY CREATED ALL DATABASES
        ----------------------------------
        """)


    def root_prep(self):
        sql_statement = "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()

            print(f"-> ROOT USER ALTERED")
            return True
        except connect.errors as err:
            print("WARNING - ERROR ALTERING ROOT USER:\n" + err)
            return False


    def create_database(self):
        sql_statement = f"CREATE DATABASE IF NOT EXISTS {self.stock_db_name};"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()

            print(f"-> CREATED DATABASE {self.stock_db_name} SUCCESSFULLY")

        except connect.errors as err:
            print("WARNING - ERROR CREATING DATABASE:\n" + err)


        sql_statement = f"CREATE DATABASE IF NOT EXISTS {self.sentiment_store_db_name};"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()

            print(f"-> CREATED DATABASE {self.sentiment_store_db_name} SUCCESSFULLY")

        except connect.errors as err:
            print("WARNING - ERROR CREATING DATABASE:\n" + err)



    def create_user(self):
        sql_statement = f"CREATE USER IF NOT EXISTS '{self.user}'@'%' IDENTIFIED BY '{self.password}';"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()
            print(f"-> CREATED USER {self.user} ON DATABASE {self.stock_db_name}")

        except connect.errors as err:
            print("WARNING - ERROR CREATING USER:\n" + err)


    def grant_all_user(self):
        sql_statement = f"GRANT ALL PRIVILEGES ON {self.stock_db_name}.* TO '{self.user}'@'%';"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()
            print("-> GRANTED USER ALL PRIVILEGES")

        except connect.errors as err:
            print("WARNING - ERROR GRANTING ALL TO USER:\n" + err)


        sql_statement = f"GRANT ALL PRIVILEGES ON {self.sentiment_store_db_name}.* TO '{self.user}'@'%';"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()
            print("-> GRANTED USER ALL PRIVILEGES")

        except connect.errors as err:
            print("WARNING - ERROR GRANTING ALL TO USER:\n" + err)



    def flush_all(self):
        sql_statement = "FLUSH PRIVILEGES;"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()
            print("-> FLUSHED ALL PRIVILEGES")

        except connect.errors as err:
            print("ERROR FLUSHING PRIVILEGES:\n" + err)

