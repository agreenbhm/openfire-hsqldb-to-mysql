import datetime
import jaydebeapi
from jaydebeapi import _DEFAULT_CONVERTERS, _java_to_py
import sqlite3
from config import *

_DEFAULT_CONVERTERS.update({'BIGINT': _java_to_py('longValue')})

class openfireMySQL:

    def __init__(self):
        self.mysqlOFCon = jaydebeapi.connect('com.mysql.jdbc.Driver',
                                             ['jdbc:mysql://' + destDBHost + ':' + destDBPort + '/' + destDBname,
                                              destUser, destPw], ['hsqldb.jar', 'mysql.jar'], )
        self.mysqlOFCur = self.mysqlOFCon.cursor()

    def getMySQLTableNames(self):
        self.mysqlCon = jaydebeapi.connect('com.mysql.jdbc.Driver',
                                 ['jdbc:mysql://' + destDBHost + ':' + destDBPort,
                                     destUser, destPw], ['hsqldb.jar', 'mysql.jar'],)
        self.mysqlCur = self.mysqlCon.cursor()
        self.mysqlCur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s'" % destDBname)
        self.tables = self.mysqlCur.fetchall()
        self.mysqlCon.close()
        self.tableList = []
        for table in self.tables:
            self.tableList.append(str(table[0]))
        return self.tableList

    def addToMySQL(self, table, columns, data):
        data = str(data)
        data = data.replace("'None', ", "null, ").replace(", 'None'", ", null")\
            .replace(", 'None', ", ", null ,").replace("('None')", "(null)")

        data = data.replace("None, ", "null, ").replace(", None", ", null") \
            .replace(", None, ", ", null ,").replace("(None)", "(null)")

        data = data.replace("L, ", ", ").replace("L)", ")")

        query = "INSERT INTO %s %s VALUES %s" % (table, columns, data)
        try:
            self.mysqlOFCur.execute(query)
        except Exception as e:
            print '\nException: ' + str(e)
            print '\nQuery: ' + query
            exit(1)

    def MySQLClose(self):
        self.mysqlOFCon.close()

def main():
    ofMySQL = openfireMySQL()
    mySQLTables = ofMySQL.getMySQLTableNames()
    hsqlCon = jaydebeapi.connect('org.hsqldb.jdbcDriver',
                             ['jdbc:hsqldb:file:' + sourceDBFile,
                              sourceUser, sourcePw], ['hsqldb.jar', 'mysql.jar'],)
    hsqlCur = hsqlCon.cursor()
    hsqlCur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_SCHEM = '%s'" % sourceDBname)
    tables = hsqlCur.fetchall()

    hsqlTableList = []
    for table in tables:
        hsqlCur.execute("SELECT COUNT(*) FROM %s.%s" % (sourceDBname, table[0]))
        hsqlTableList.append({'oldname': str(table[0]), 'rowcount': list(hsqlCur.fetchone())[0]})

    hsqlTableList[:] = [new for new in hsqlTableList if new.get('rowcount') != 0]

    for hsqlDict in hsqlTableList:
        for mySQLTable in mySQLTables:
            if hsqlDict['oldname'] == str(mySQLTable).upper():
                hsqlDict['newname'] = mySQLTable

    sqliteCon = sqlite3.connect('hsqldbToMySQL.db')
    sqliteCur = sqliteCon.cursor()
    sqliteCur.execute("CREATE TABLE IF NOT EXISTS status (tableName TEXT UNIQUE, finished TEXT, lastRow INT)")
    sqliteCon.commit()
    sqliteCur.execute("SELECT * FROM status")
    previouslyProcessedRaw = sqliteCur.fetchall()
    previouslyProcessed = {}
    for prev in previouslyProcessedRaw:
        previouslyProcessed[str(prev[0])] = {'finished': str(prev[1]), 'lastRow': prev[2]}

    for table in hsqlTableList:
        startingRow = 0
        if table['newname'] in previouslyProcessed:
            if previouslyProcessed[table['newname']]['finished'] == 'y':
                continue
            startingRow = previouslyProcessed[table['newname']]['lastRow'] + 1
        print '\n' + "Processing table: " + table['newname']
        startTime = datetime.datetime.now()
        percent = 0
        timeremaining = 'Calculating...'

        hsqlCur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_SCHEM = '%s' "
                        "AND TABLE_NAME = '%s'" % (sourceDBname, table['oldname']))
        columns = hsqlCur.fetchall()
        columns[:] = [str(x[0]) for x in columns]
        cleanColumns = ()
        for x in columns:
            cleanColumns = cleanColumns + (x,)
        cleanColumns = str(cleanColumns).replace("'", "")
        for i in xrange(startingRow, table['rowcount']):
            if table['rowcount'] > 100 and i % 100 == 0:
                percentChange = False
                newpercent = ((i * 100) / table['rowcount'])
                print "Completed " + str(i) + " records out of " + str(table['rowcount']) + "; " \
                      + str(percent) + "%" + "; Time remaining: " + str(timeremaining)
                if newpercent > percent:
                    percentChange = True
                    percent = newpercent
                if percent > 0 and percentChange:
                    timeremaining = (((datetime.datetime.now() - startTime) /
                         percent) * (100 - percent))

            hsqlCur.execute("SELECT LIMIT %s 1 * FROM %s" % (i, table['oldname']))
            data = hsqlCur.fetchone()
            cleanData = ()
            for x in data:
                if type(x) == unicode:
                    x = x.encode('ascii', 'ignore')
                elif type(x) == long:
                    x = int(x)
                cleanData = cleanData + (x,)
            try:
                ofMySQL.addToMySQL(table['newname'], cleanColumns, cleanData)
                sqliteCur.execute("INSERT OR REPLACE INTO status VALUES (?, ?, ?) ", (table['newname'], 'n', i))
                sqliteCon.commit()
            except Exception as e:
                print '\nException: ' + str(e)
                print '\nClean Data: ' + str(cleanData)
                sqliteCur.execute("INSERT OR REPLACE INTO status VALUES (?, ?, ?) ", (table['newname'], 'n', i))
                sqliteCon.commit()
                exit(1)

        sqliteCur.execute("INSERT OR REPLACE INTO status VALUES (?, ?, ?) ", (table['newname'], 'y', 0))
        sqliteCon.commit()

    ofMySQL.MySQLClose()

if __name__ == "__main__":
    main()