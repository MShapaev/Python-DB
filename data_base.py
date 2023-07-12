import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class Database:
    """ Предназначен для управления создаваемой базой данных. Для начала работы необходимо ввести название базы данных,
     имя пользователя и пароль Postgres"""

    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password

    def create_db(self):
        """Создает базу данных PostgreSQL с названием из database"""
        try:
            # Создаем новую базу данных
            connection = psycopg2.connect(user=self.user,
                                          # пароль, который указали при установке PostgreSQL
                                          password=self.password,
                                          host="127.0.0.1",
                                          port="5432")
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()
            sql_create_database = f'create database {self.database}'
            cursor.execute(sql_create_database)
            cursor.close()
        except (Exception, psycopg2.OperationalError) as error:
            print("Ошибка при работе с PostgreSQL", error)

    def get_connection(self):
        """Функция проверяет есть ли связь с базой данных, открывает ее и возвращает информацию о SQL версии"""
        try:
            connection = psycopg2.connect(database=self.database,
                                          user=self.user,
                                          password=self.password,
                                          host="127.0.0.1",
                                          port="5432")
            cursor = connection.cursor()
            print("Информация о сервере PostgreSQL")
            print(connection.get_dsn_parameters(), "\n")
            # Выполняем SQL запрос
            cursor.execute("SELECT version();")
            # Вывести 1 результат
            version = cursor.fetchone()
            print("Вы подключены к -  ", version, "\n")
            cursor.close()
            print("Соединение с PosgreSQL установлено")
            return connection
        except (Exception, psycopg2.OperationalError) as error:
            print("Ошибка при соединении с PosgreSQL")

    def create_table(self, table):
        """1. Функция принимает на вход данные для создания таблицы в синтаксисе postgreSQL"""
        connection = self.get_connection()
        cursor = connection.cursor()
        create_query = table
        cursor.execute(create_query)
        connection.commit()
        res = [i for i in table.split() if i[0].isupper() and i[-1].islower()]
        print(f'Таблица {res[0]} успешно создана')

    def add_client(self, insert):
        """2. Функция наполняет данными таблицу Clients"""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(insert)
            connection.commit()
            print(f'В таблицу Clients внесены данные')
            cursor.execute("SELECT * from clients")
            record = cursor.fetchall()
            print("Результат", record)
        except (Exception, psycopg2.errors.UniqueViolation) as error:
            print("""Данные не будут повторно добавлены в таблицу Clients,
            повторяющееся значение ключа нарушает ограничение уникальности""")
        finally:
            if connection:
                cursor.close()
                connection.close()

    def add_phone(self, insert):
        """3. Функция наполняет данными таблицу Phones"""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(insert)
            connection.commit()
            print(f'В таблицу Phones внесены данные')
            cursor.execute("SELECT * from phones")
            record = cursor.fetchall()
            print("Результат", record)
        except (Exception, psycopg2.errors.UniqueViolation) as error:
            print("""Данные повторно не будут добавлены в таблицу Phones, 
            так как повторяющееся значение ключа нарушает ограничение уникальности""")
        finally:
            if connection:
                cursor.close()
                connection.close()

    def get_id_clients(self, insert):
        """7. Функция, позволяющая найти клиента по его имени, фамилии, email или телефону"""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("""
                            SELECT c.id FROM clients c
                            LEFT JOIN phones p ON c.id = p.id 
                            WHERE c.name=%s OR c.surname=%s OR c.email=%s OR p.phone=%s ;
                            """, (insert, insert, insert, insert,))
            name_id = cursor.fetchone()[0]
            return name_id
        except (Exception, TypeError) as error:
            print("Данные уже добавлены, или введены неверные данные для поиска по ID ")
        finally:
            if connection:
                cursor.close()
                connection.close()

    def update_table(self, new, old):
        """4.Функция, позволяющая изменить данные о клиенте.,
         Пример, передаем код UPDATE в качестве аргумента: '''UPDATE phones SET phone=%s WHERE phone=%s;
        ''', (79991234567, 79039876543), где 79991234567 - номер телефона на который будет изменен 79039876543"""
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(new, old)
            print('Данные в таблице изменены')
            connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def delete_info(self, delete):
        """5 и 6. Удаляет телефон по Email клиента(так как только Email клиента может быть уникальным), из отношения
        Phones или каскадно удалит всю информацию о клиенте из отношения Clients по Email"""
        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(delete)
        connection.commit()
        count = cursor.rowcount
        print(count, "Запись успешно удалена")
        # Получить результат
        cursor.execute("SELECT * from phones")
        print("Результат", cursor.fetchall())
        cursor.close()
        connection.close()


if __name__ == "__main__":
    with open('password.txt') as f: # Запишите Postgresql пароль в файл password.txt перед началом работы
        password = f.readline()
    db_user = Database('clients', 'postgres', password)
    db_user.create_db()
    db_user.create_table('''CREATE TABLE IF NOT EXISTS Clients (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(60) NOT NULL,
                            surname VARCHAR(60) NOT NULL,
                            email VARCHAR(60) UNIQUE
                            );
                            ''')
    db_user.create_table('''CREATE TABLE IF NOT EXISTS Phones (
                                id INTEGER NOT NULL REFERENCES Clients(id) ON DELETE CASCADE,
                                phone VARCHAR(20) DEFAULT NULL UNIQUE
                                );
                                ''')
    #
    db_user.add_client("""INSERT INTO clients (name, surname, email)
                            VALUES ('Maksim', 'Shapaev', 'shapaev@ya.ru'),
                            ('Alena', 'Shapaeva', 'shapaeva@ya.ru');
                            """)
    db_user.add_phone("""INSERT INTO phones (id, phone)
                        VALUES (1, 79251234567),
                        (1, 79268765432),
                        (2, 79265556677);
                        """)
    db_user.update_table("""UPDATE clients SET name=%s WHERE id=%s;
                        """, ('Alenushka', db_user.get_id_clients('Alena')))
    db_user.update_table("""UPDATE clients SET surname=%s WHERE id=%s;
                            """, ('Ivanov', db_user.get_id_clients('Shapaev')))
    db_user.update_table("""UPDATE phones SET phone=%s WHERE phone=%s;
                             """, ('79111111111', '79251234567'))
    #
    db_user.delete_info("""DELETE FROM phones
                        WHERE  id = (SELECT p.id FROM phones p
                        JOIN clients c ON p.id = c.id
                        WHERE c.email = 'shapaev@ya.ru'
                        LIMIT 1);
                        """)  # Выполнит удаление только телефонов (всех имеющихся).
                            # Можно удалить конкретный (названию телефона).
    db_user.delete_info("""DELETE FROM clients
                            WHERE  id = (SELECT c.id FROM clients c
                            WHERE c.email = 'shapaeva@ya.ru'
                            LIMIT 1);
                            """)  # Выполнит каскадное удаление клиента из всех связанных отношений по email.
    #
    print(db_user.get_id_clients('shapaev@ya.ru'))  # Проверка функции поиска ID по любым данным клиента