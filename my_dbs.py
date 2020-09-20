#!/user/bin/python
# _*_ coding:utf-8 _*_
import pymysql

from sshtunnel import SSHTunnelForwarder
import traceback

__author__ = "super.gyk"


class ControlDB(object):
    def __init__(self, ssh_username, ssh_password, ssh_ip, remote_addr, user, password, database):
        self.local_host = '127.0.0.1'  # 必须是127.0.0.1
        self.port = 10022
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_ip = ssh_ip
        self.remote_bind_address = (remote_addr, 3306)
        self.local_bind_address = ('0.0.0.0', 10022)
        self.user = user
        self.password = password
        self.database = database

    def connect_db(self):
        con_obj = pymysql.connect(
            host=self.local_host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )
        return con_obj

    def connect_db_by_ssh(self):
        with SSHTunnelForwarder(
                (self.ssh_ip, 22),
                ssh_username=self.ssh_username,
                ssh_password=self.ssh_password,
                remote_bind_address=self.remote_bind_address,
                local_bind_address=self.local_bind_address,
        ) as tunnel:
            print("connected")
            print(tunnel.is_alive)
            try:
                self.connect_db().ping(reconnect=True)
                print("db is connecting")
                # 获取光标
                cour = self.connect_db()
                print(cour)
                # 返回数据库对象
                return cour
                # return self.connect_db()

            except:
                traceback.print_exc()
                self.connect_db()
                print("db reconnect")

    def start(self, sql, type):
        cour = self.connect_db_by_ssh()
        if not cour:
            print("#######   mysql connect error   #########")
            return
        # 获取光标
        cur = cour.cursor()

        try:
            result = cur.execute(sql)
            if type:
                if type == 1:
                    rs = cur.fetchone()
                if type == 2:
                    rs = cur.fetchall()
                # return rs
                print(rs)
                print(result)
                print(cur.rowcount)
                cur.close()
                cour.close()
            else:
                cour.commit()
                print(result)
                cur.close()
                cour.close()
        except Exception as e:
            print(e)


if __name__ == "__main__":
    pass
