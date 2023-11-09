import pymysql
from data.auth_data import *


class Database:
    @staticmethod
    def get_connection() -> pymysql.Connection:
        try:
            connect = pymysql.connect(
                host=host,
                port=3306,
                user=user,
                database=database,
                password=password,
                cursorclass=pymysql.cursors.DictCursor
            )
            return connect
        except Exception as ex:
            print(ex)

    def get_table(self, table: str) -> list[dict]:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                query = f"SELECT * FROM `{table}`"
                cursor.execute(query)
                rows = cursor.fetchall()
                return rows
        except Exception as ex:
            print(ex)
        finally:
            con.close()

    def insert_data(self, table: str, data: str, keys: str) -> str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                sql = f"INSERT INTO {table} {keys} VALUES {data}"
                cursor.execute(sql)
            con.commit()
            return "Successful insertion!"
        except Exception as ex:
            return f"Something went wrong! {ex}"
        finally:
            con.close()

    def delete_data(self, table: str, key: str, value: int) -> str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                sql = f"DELETE FROM {table} WHERE {key}={value}"
                cursor.execute(sql)
            con.commit()
            return "Successful delete!"
        except Exception as ex:
            return f"Something went wrong! {ex}"
        finally:
            con.close()

    def delete_data_pair(self, table: str, key1: str, key2: str,  value1: int,  value2: int) -> str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                sql = f"DELETE FROM {table} WHERE {key1}={value1} and {key2}={value2}"
                print(sql)
                cursor.execute(sql)
            con.commit()
            return "Successful delete!"
        except Exception as ex:
            return f"Something went wrong! {ex}"
        finally:
            con.close()

    def call_proc_active_count(self) -> list or str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                cursor.execute(
                    "CALL get_active_computers_count()"
                )
                res = cursor.fetchall()
                return res
        except Exception as ex:
            print(ex)
            return f"Something went wrong! {ex}"
        finally:
            con.close()

    def call_proc_active(self) -> list or str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                cursor.execute(
                    "CALL get_active_computers()"
                )
                res = cursor.fetchall()
                return res
        except Exception as ex:
            print(ex)
            return f"Something went wrong! {ex}"
        finally:
            con.close()

    def call_func_plan_price(self, plan: str) -> list or str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                cursor.execute(
                    f'SELECT get_active_plan_price("{plan}")'
                )
                res = cursor.fetchall()
                return res
        except Exception as ex:
            print(ex)
            return f"Something went wrong! {ex}"
        finally:
            con.close()

    def call_comp_count(self, comp: str) -> list or str:
        try:
            con = self.get_connection()
            with con.cursor() as cursor:
                cursor.execute(
                    f'SELECT get_component_count("{comp}")'
                )
                res = cursor.fetchall()
                return res
        except Exception as ex:
            print(ex)
            return f"Something went wrong! {ex}"
        finally:
            con.close()
