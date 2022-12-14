import psycopg2
from tabulate import tabulate

print("Beginning")

# Change the credentials and the name of the database
con = psycopg2.connect(
    host="localhost",
    database="cs623finalproject",
    user="postgres",
    password=##ENTER PASSWORD)

print(con)

#For isolation: SERIALIZABLE
con.set_isolation_level(3)

#For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    # QUERY
    cur.execute("ALTER TABLE Stock DROP CONSTRAINT fk_depid")
    cur.execute("ALTER TABLE Stock ADD CONSTRAINT fk_depid FOREIGN KEY (depid) REFERENCES Depot ON UPDATE CASCADE")
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