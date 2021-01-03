from config_read import config as get_values
from Logger.logger import log_error
from Logger.logger import log_status
import mysql.connector as connect
import sys
from Database.utility import list_of_schema

# Create databases using root user
# Create a master user that can interact with each and every database with GRANT similar credentials
class build_database:
    def __init__(self):
        config = get_values()
        self.user = config.db_user
        self.host = config.db_host
        self.password = config.db_pass
        self.db_user_root = config.db_user_root
        self.db_root_pass = config.db_pass_root
        self.list_of_db = list_of_schema()
        self.char_set = config.character_set
        self.sentiment_db_name = config.sentiment_db_name


    def build_database(self):
        result = False
        result = self.root_prep()
        result = self.global_database_settings()
        result = self.create_user()

        for db in self.list_of_db:
            result = self.create_database(db)
            result = self.grant_all_user(db)

        result = self.flush_all()
        if result:
            log_status("Database setup/startup was successful", )
        else:
            print("""
            ERROR : Error during setup/startup of database \n \n
            Application will now exit...""")
            sys.exit(0)


    def root_prep(self):
        sql_statement = f"GRANT ALL PRIVILEGES ON *.* TO '{self.db_user_root}'@'%' WITH GRANT OPTION;"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()

            log_status("Root user altered - all privileges granted to root user")
            return True
        except:
            log_error("ERROR : Could not alter root user")
            return False


    def create_database(self, db_name):
        sql_statement = f"CREATE DATABASE IF NOT EXISTS {db_name};"
        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()

            log_status(f"Database created successfully {db_name}")
            return True
        except:
            log_error(f"ERROR : Could not create database {db_name}")
            return False

    def global_database_settings(self, io_capacity=1000):
        sql_statement = f"SET GLOBAL innodb_io_capacity={io_capacity};"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()
            log_status(f"Global innodb_capacity set to " + io_capacity)
            return True
        except:
            log_error(f"ERROR : Could not set innodb_capacity")
            return False

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
            log_status(f"User created successfully {self.user}")
            return True
        except connect.errors as err:
            log_error(f"ERROR : Could not create {self.user}")
            return False


    def grant_all_user(self, db_name):
        sql_statement = f"GRANT ALL PRIVILEGES ON {db_name}.* TO '{self.user}'@'%';"

        try:
            conn = connect.connect(
                host=f"{self.host}",
                user=f"{self.db_user_root}",
                password=f"{self.db_root_pass}",
            )
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            conn.close()
            log_status(f"USER: '{self.user}' granted all privileges on {db_name}")
            return True
        except:
            log_error(f"ERROR : Could not grant privileges to '{self.user}' on {db_name}")
            return False


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
            log_status("Flushed all privileges")
            return True
        except:
            log_error("ERROR : Could not flush privileges")
            return False

