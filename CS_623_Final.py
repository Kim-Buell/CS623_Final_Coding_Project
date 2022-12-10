import psycopg2
from tabulate import tabulate

print("Beginning")

# Change the credentials and the name of the database
# create table student(id integer, name varchar(10), primary key(id))

con = psycopg2.connect(
    host="localhost",
    database="cs623finalproject",
    user="postgres",
    password="Slice#159632!")

print(con)

#For isolation: SERIALIZABLE
con.set_isolation_level(3)

#For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    # QUERY
    #cur.execute("insert into student values (10, 'stud1')")
    cur.execute("CREATE TABLE Product(prodid CHAR(2), pname VARCHAR(30), price DECIMAL)")
    cur.execute("ALTER TABLE Product ADD CONSTRAINT pk_product PRIMARY KEY (prodid)")
    cur.execute("CREATE TABLE Depot(depid VARCHAR(3), addr VARCHAR(30), volume INTEGER)")
    cur.execute("ALTER TABLE Depot ADD CONSTRAINT pk_depot PRIMARY KEY (depid)")
    cur.execute("CREATE TABLE Stock (prodid CHAR(2), depid VARCHAR(3), quantity INTEGER)")
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT pk_stock PRIMARY KEY (prodid, depid)")
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT fk_prodid FOREIGN KEY (prodid) REFERENCES Product")
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT fk_depid FOREIGN KEY (depid) REFERENCES Depot ON UPDATE CASCADE")
    cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('p1', 'tape', 2.5)")
    cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('p2', 'tv', 250)")
    cur.execute("INSERT INTO Product (prodid, pname, price) VALUES ('p3', 'ver', 8)")
    cur.execute("INSERT INTO Depot (depid, addr, volume) VALUES ('d1', 'New York', 9000)")
    cur.execute("INSERT INTO Depot (depid, addr, volume) VALUES ('d2', 'Syracuse', 6000)")
    cur.execute("INSERT INTO Depot (depid, addr, volume) VALUES ('d4', 'New York', 2000)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd1', 1000)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd2', -100)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p1', 'd4', 1200)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p3', 'd1', 3000)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p3', 'd4', 2000)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p2', 'd4', 1500)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p2', 'd1', -400)")
    cur.execute("INSERT INTO Stock (prodid, depid, quantity) VALUES ('p2', 'd2', 2000)")
    cur.execute("UPDATE Depot SET depid = 'dd1' where depid = 'd1'")


except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back before start of transactions")
    con.rollback()
finally:
    if con:
        con.commit()
        cur.close
        con.close
        print("PostgreSQL connection is now closed")

print("End")