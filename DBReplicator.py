import subprocess
import mysql.connector
import time
import DBConnection
import config

class DBReplicator:
    ''''Common class for DBReplicator'''
#constructor
    def __init__(self):
        self.__mysqlConn1 = DBConnection.Mysql(3311)
        self.__newLogPos = ""
        self.__newLogFile = ""
        self.__oldLogPos = ""
        self.__oldLogFile = ""
#destructor
    def __del__(self):
        if self.__mysqlConn1 is not None:
            del self.__mysqlConn1

#gets the new log coordinates by querying master database using "SHOW MASTER STATUS"
    def _getNewLogCoordinates(self):
        self.__mysqlConn1._open()
        q = "SHOW MASTER STATUS"
        res = self.__mysqlConn1._get_query(q)[0]
        if res is None:
            exit()
#        print(res)
        self.__mysqlConn1._close()
        self.__newlogFile=res[0]
        self.__newLogPos=str(res[1])
        print("new log coordinates : " + res[0] + "\t" + str(res[1]))

#gets the old log coordinates that are saved in the COMMON_FILE, which maintains the last log coordinates that have been successfully updated
    def _getOldLogCoordinates(self):
        with open(config.COMMON_FILE, 'r') as fp:
            line=fp.read()
        l = line.split("\t")
        self.__oldLogFile=l[0]
        self.__oldLogPos=l[1]
        print("old log coordinates : " + line)

#this will write the string provided in the fuunction to COMMON_FILE. If no string is provided as a parameter, then a new string made from new log position and file name would be written to COMMON_FILE
    def _write_to_common_file(self, s=""):
        if self.__newLogFile is None or self.__newLogPos is None:
            print("no logs to be writted")
            return
        if s == "":
            s = self.__newlogFile + "\t" + self.__newLogPos
        with open(config.COMMON_FILE, 'w') as fp:
            line=fp.write(s)

#	this will check if any change has happened to the master db since the last update to slave db.
#	if no chage has happened then nothing is done
#	else slave is updated and new log coordinates are written to COMMON_FILE
    def _update_if_required(self):
        oldlogFileName=self.__oldLogFile.split(".")[0]
        oldLogFileIdx=int(self.__oldLogFile.split(".")[1])
        newLogFileName=self.__newlogFile.split(".")[0]
        newLogFileIdx=int(self.__newlogFile.split(".")[1])
        postCommand=["|",config.EXE_DIR+"mysql","-u","root","-P","3312","-proot@123"]
        if newLogFileIdx > oldLogFileIdx:
            diff = len(self.__newlogFile) - len(newLogFileName) - 1
            x = "0" + str(diff) + "d"
            i = oldLogFileIdx
            while i <= newLogFileIdx:
                command=[]
                command.append(config.EXE_DIR + "mysqlbinlog.exe")
                if i == newLogFileIdx:
                    command.append("--stop-position=" + self.__newLogPos)
                elif i == oldLogFileIdx:
                    command.append("--start-position=" + self.__oldLogPos)
                filename = newLogFileName + "." + format(i, x)
                command.append(config.BINLOGDIR1 + filename)
                command.extend(postCommand)
                try:
                    subprocess.call(command,shell=True)
                except:
                    print("mysqlbinlog command failed at: file : " + filename + ", log position " + self.__oldLogPos if i == oldLogFileIdx else "0")
                    s = filename + "\t" + self.__oldLogPos if i == oldLogFileIdx else "0"
                    self._write_to_common_file(s)
                    return
                else:
                    i = i + 1
            self._write_to_common_file()
        elif newLogFileIdx == oldLogFileIdx:
            if int(self.__newLogPos) > int(self.__oldLogPos):
                command=[]
                command.append(config.EXE_DIR + "mysqlbinlog.exe")
                command.append("--start-position=" + str(self.__oldLogPos))
                command.append("--stop-position=" + str(self.__newLogPos))
                command.append(config.BINLOGDIR1+self.__newlogFile)
                command.extend(postCommand)
                try:
                    subprocess.call(command,shell=True)
                except:
                    print("mysqlbinlog command failed at: file : " + self.__newlogFile + ", log position " + self.__oldLogPos )
                    s = self.__newLogFile + "\t" + self.__oldLogPos
                    self._write_to_common_file(s)
                    return
                else:
                    self._write_to_common_file()
            else:
                print("update not required")

dbReplicator = DBReplicator()
while True:
    dbReplicator._getOldLogCoordinates()
    dbReplicator._getNewLogCoordinates()
    dbReplicator._update_if_required()
    time.sleep(5)

