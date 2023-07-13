import psycopg2


def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(20),
        lastname VARCHAR(30),
        email VARCHAR(254)
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phonenumbers(
        number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)
    return


def delete_db(cur):
    cur.execute("""
        DROP TABLE clients, phonenumbers CASCADE;
        """)

''' Функция, позволяющая добавить телефон для существующего клиента '''
def add_phone(cur, client_id, tel):
    cur.execute("""
        INSERT INTO phonenumbers(number, client_id)
        VALUES (%s, %s)
        """, (tel, client_id))
    return client_id

''' Функция, позволяющая добавить нового клиента '''
def add_client(cur, name=None, surname=None, email=None, tel=None):
    cur.execute("""
        INSERT INTO clients(name, lastname, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    cur.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1
        """)
    id = cur.fetchone()[0]
    if tel is None:
        return id
    else:
        add_phone(cur, id, tel)
        return id

''' Функция, позволяющая изменить данные о клиенте '''
def change_client(cur, id, name=None, surname=None, email=None):
    cur.execute("""
        SELECT * from clients
        WHERE id = %s
        """, (id, ))
    info = cur.fetchone()
    if name is None:
        name = info[1]
    if surname is None:
        surname = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE clients
        SET name = %s, lastname = %s, email =%s 
        where id = %s
        """, (name, surname, email, id))
    return id

''' Функция, позволяющая удалить телефон для существующего клиента '''
def delete_phone(cur, number):
    cur.execute("""
        DELETE FROM phonenumbers 
        WHERE number = %s
        """, (number, ))
    return number

''' Функция, позволяющая удалить существующего клиента '''
def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phonenumbers
        WHERE client_id = %s
        """, (id, ))
    cur.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id

''' Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону '''
def find_client(cur, name=None, surname=None, email=None, tel=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if surname is None:
        surname = '%'
    else:
        surname = '%' + surname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if tel is None:
        cur.execute("""
            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s
            """, (name, surname, email))
    else:
        cur.execute("""
            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (name, surname, email, tel))
    rows = cur.fetchall()
    if not rows:
        return 'Такой клиент не найден'
    render_rows(rows)

''' функция, котрая отдает таблицу клиентов с телефонами из БД'''
def print_all():
    print('Список клиентов:')
    curs.execute("""
                    SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                    LEFT JOIN phonenumbers p ON c.id = p.client_id
                    ORDER by c.id
                    """)
    render_rows(curs.fetchall())

''' функция, которая выводит на экран список типа [(), (), .. ] в виде строк на экран '''
def render_rows(res_list):
    for j in range(len(res_list)):
        res = ", ".join([str(i) for i in res_list[j]])
        print(res)



if __name__ == '__main__':
    with open('password.txt') as f:  # Запишите Postgresql пароль в файл password.txt перед началом работы
        password = f.readline()
    with psycopg2.connect(database="test2", user="postgres", password=password) as conn:
        with conn.cursor() as curs:
            # Удаление таблиц перед запуском (в случае, если таблица уже создана)
            delete_db(curs)
            # 1. Cоздание таблиц
            create_db(curs)
            print("База данных создана")
            # 2. Добавляем 5 клиентов
            print("Добавлен клиент id: ",
                  add_client(curs, "Максим", "Шапаев", "shapaev@ya.ru"))
            print("Добавлен клиент id: ",
                  add_client(curs, "Иван", "Иванов", "ivanov@ya.ru", 79991111111))
            print("Добавлен клиент id: ",
                  add_client(curs, "Петр", "Петров", "petrov0ya.ru", 79992222222))
            print("Добавлен клиент id: ",
                  add_client(curs, "Сидор", "Сидоров", "sidorov@ya.ru", 79993333333))
            print("Добавлен клиент id: ",
                  add_client(curs, "Alena", "Shapaeva", "shapaeva@gmail.com"))
            print("Внесенные данные:")
            print_all()

            # 3. Добавляем клиенту номер телефона
            print("Телефон добавлен клиенту id: ",
                  add_phone(curs, 1, 79990000000))
            print("Телефон добавлен клиенту id: ",
                  add_phone(curs, 2, 74951234567))
            print_all()

            # 4. Изменим данные клиента
            print("Изменены данные клиента id: ",
                  change_client(curs, 4, "Sidor", None, 'sidor-sidorov@gmail.com'))
            print_all()

            # 5. Удаляем клиенту номер телефона
            print("Удалён телефон c номером: ",
                  delete_phone(curs, '79992222222'))
            print_all()

            # 6. Удалим клиента номер 3
            print("Клиент удалён с id: ",
                  delete_client(curs, 3))
            print_all()

            # 7. Найдём клиента
            print('Найденный клиент по имени:')
            print(find_client(curs, 'Иван'))

            print('Найденный клиент по email:')
            print(find_client(curs, None, None, '111@gmail.com'))


            print('Найденный клиент по имени, фамилии и email:')
            print(find_client(curs, 'Alena', 'Shapaeva',
                               'shapaeva@gmail.com', None))

            print('Найденный клиент по фамилии и телефону:')
            print(find_client(curs, None, 'Сидоров', None, '79993333333'))
