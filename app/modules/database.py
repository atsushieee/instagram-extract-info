import pymysql

class SqlManipulator:
    def __init__(self, db_name):
        self.conn = pymysql.connect(host='localhost', user='root', passwd= None, db=db_name, charset='utf8mb4')
        self.cur = self.conn.cursor()

    def create_table(self, table_name):
        self.cur.execute("create table " + table_name + " (`id` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`))")
        self.cur.execute("ALTER TABLE " + table_name + " CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

    def delete_table(self, table_name):
        self.cur.execute("drop table " + table_name)

    def add_column(self, table_name, column_name, column_type):
        self.cur.execute("alter table " + table_name + " add " + column_name + " " + column_type)

    def delete_column(self, table_name):
        self.cur.execute("alter table " + table_name + " drop column id")

    def insert_into_table(self, table_name, insert_column, insert_value):
        self.cur.execute("insert into " + table_name + " (" + insert_column + ") values (" + insert_value +")")
        #print("novel record!!: " + insert_value)
        self.conn.commit()

    def select_records(self, table_name, insert_column, insert_value):
        self.cur.execute("select * from " + table_name + " where " + insert_column + " = " + insert_value)
        return self.cur.fetchall()

    def update_record(self, table_name, update_column, update_value, base_column, base_update, is_int=0):
        if is_int == 1:
            update_value = str(update_value)
        update_value = "'" + update_value + "'"
        #print("update " + table_name + " set " + update_column + " = " + update_value + " where " + base_column + " = " + base_update)
        self.cur.execute("update " + table_name + " set " + update_column + " = " + update_value + " where " + base_column + " = " + base_update)
        self.conn.commit()
