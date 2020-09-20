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
        return pymysql.connect(
            host=self.local_host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4'
        )

    def connect_db_by_ssh(self, sql, type):
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
                # 获取对象
                cour = self.connect_db()
                # 进行数据操作
                self.execute_db(cour, sql, type)
            except:
                traceback.print_exc()
                self.connect_db()
                print("db reconnect")

    def execute_db(self, cour, sql, type):
        if not cour:
            print("#######   mysql connect error   #########")
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
                cour.close()
            else:
                cour.commit()
                print(result)
                cour.close()

        except Exception as e:
            print(e)

    def start(self, mysql, type):
        my_sql = mysql
        self.connect_db_by_ssh(my_sql, type)
        print("执行完毕")


if __name__ == "__main__":
    ssh_username = 'xxxx'
    ssh_password = 'yxxx2'
    ssh_ip = 'xxx.xx.xx.xx'
    remote_addr = 'rm-xxx.xx.xx.xxx.com'
    user = 'xx'
    password = 'xxxx'
    database = 'xxx'
    db = ControlDB(ssh_username, ssh_password, ssh_ip, remote_addr, user, password, database)
    # sql = "SELECT * FROM policy_info;"
    sql = "SELECT * from policy_info where policy_no='86000020201600606813';"
    # sql = "update policy_info set status=8 where policy_no='86000020201600606813';"
    type = 1
    db.start(sql, type)
