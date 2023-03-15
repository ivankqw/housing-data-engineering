import sqlalchemy as db

def create_table():
    # Create a MySQL engine
    engine = db.create_engine('mysql+mysqlconnector://user:password@mysql-1:3306/db')

    conn = engine.connect()

    # Create a table
    conn.execute("CREATE TABLE IF NOT EXISTS my_table (id INT NOT NULL, name VARCHAR(255), PRIMARY KEY (id));")


if __name__ == "__main__":
    create_table()
    