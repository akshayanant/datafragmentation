#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    command1 = """ CREATE TABLE {0} (
                    UserID INTEGER,
                    dummy1 CHAR,
                    MovieID INTEGER,
                    dummy2 CHAR,
                    Rating REAL,
                    dummy3 CHAR,
                    TimeStamp BIGINT)
                    """.format(ratingstablename)

    command2 = """ COPY {0} 
                   FROM '{1}' (DELIMITER(":"))""".format(ratingstablename,ratingsfilepath)
    command3 ="""ALTER TABLE {0}
                 DROP COLUMN dummy1,
                 DROP COLUMN dummy2,
                 DROP COLUMN dummy3,
                 DROP COLUMN TimeStamp
                  """.format(ratingstablename)
    with openconnection.cursor() as cur:
            cur.execute(command1)
            cur.execute(command2)
            cur.execute(command3)


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    count_rows = """ SELECT COUNT(*) FROM {0}""".format(ratingstablename)
    prefix = "range_part"
    with openconnection.cursor() as cur:
        cur.execute(count_rows)
        count = int(cur.fetchone()[0])
        low = 0
        incr = 5.0/numberofpartitions
        table_name = prefix + "0"
        firstpart = """CREATE TABLE {0} AS ( 
                            SELECT 
                                *
                            FROM 
                               {1} 
                            WHERE 
                                rating>={2} and rating <={3} 
                            )
                            """.format(table_name,ratingstablename,low,incr)
        cur.execute(firstpart)
        low = incr
        for i in range(1,numberofpartitions):
            high = low+incr
            table_name = prefix + str(i)
            partition = """CREATE TABLE {0} AS ( 
                            SELECT 
                                * 
                            FROM 
                                {1} 
                            WHERE 
                                rating>{2} and rating <={3}
                            )
                            """.format(table_name,ratingstablename,low,high)

            cur.execute(partition)
            low = low+incr+0.1




def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    prefix = "rrobin_part"
    tables = []
    with openconnection.cursor() as cur:
        for i in range(numberofpartitions):
            table_name = prefix+str(i)
            tables.append(table_name)
            create_table = """CREATE TABLE {0} AS ( 
                                SELECT 
                                    * 
                                FROM
                                    {1} 
                                LIMIT
                                     0 )
                                """.format(table_name,ratingstablename)
            cur.execute(create_table)
        select_rows = """SELECT 
                            * 
                        FROM
                            {0}""".format(ratingstablename)
        cur.execute(select_rows)
        rows = cur.fetchall()
        index = 0
        for i in range(len(rows)):
            table_name = tables[index]
            insert_into = """INSERT INTO {0} VALUES(
                            {1},{2},{3}
                            )
                          """.format(table_name,rows[i][0],rows[i][1],rows[i][2])
            cur.execute(insert_into)
            index = (index+1)%numberofpartitions

    

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    prefix = "rrobin_part"
    with openconnection.cursor() as cur:
        insert_into = """INSERT INTO {0} VALUES(
                                {1},{2},{3}
                                )
                            """.format(ratingstablename,userid,itemid,rating)
        cur.execute(insert_into)
        get_tables = """SELECT 
                            table_name
                        FROM 
                            information_schema.tables 
                        WHERE 
                            table_schema = 'public' """
        cur.execute(get_tables)
        tables = cur.fetchall()
        rr_tables = 0
        for table in tables:
            if(table[0].find(prefix)==0):
                rr_tables = rr_tables+1
        get_size = """SELECT
                         COUNT(*)
                      FROM 
                         {0}""".format(prefix+str(0))
        cur.execute(get_size)
        first_size = cur.fetchone()[0]
        for i in range(1,rr_tables):
            get_size = """SELECT 
                            COUNT(*)
                          FROM
                            {0}""".format(prefix+str(i))
            cur.execute(get_size)
            cur_size = cur.fetchone()[0]
            if(cur_size<first_size):
                insert_into = """INSERT INTO {0} VALUES(
                                {1},{2},{3}
                                )
                            """.format(prefix+str(i),userid,itemid,rating)
                cur.execute(insert_into)
                return

        insert_into = """INSERT INTO {0} VALUES(
                                {1},{2},{3}
                            )
                        """.format(prefix+str(0),userid,itemid,rating)
        cur.execute(insert_into)


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    prefix = "range_part"
    with openconnection.cursor() as cur:
        insert_into = """INSERT INTO {0} VALUES(
                                {1},{2},{3}
                                )
                            """.format(ratingstablename,userid,itemid,rating)
        cur.execute(insert_into)
        get_tables = """SELECT 
                            table_name
                        FROM 
                            information_schema.tables
                        WHERE
                             table_schema = 'public' """
        cur.execute(get_tables)
        tables = cur.fetchall()
        rage_tables = 0
        for table in tables:
            if(table[0].find(prefix)==0):
                rage_tables = rage_tables+1
        incr = 5.0/rage_tables
        high = incr
        index =0
        while(high<=5.0):
            if(rating<=high):
                insert_into = """INSERT INTO {0} VALUES(
                                {1},{2},{3}
                                )
                                """.format(prefix+str(index),userid,itemid,rating)
                cur.execute(insert_into)
                return
            high+=incr
            index+=1


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    print "deletepartitionsandexit"
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    print("deleteTables")
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
