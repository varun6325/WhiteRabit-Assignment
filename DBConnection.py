import mysql.connector


	# cnx1 = mysql.connector.connect(host='localhost',user='root',password='root@123',port=3311)
	# cursor1=cnx1.cursor()
	# cursor1.execute("FLUSH TABLES WITH READ LOCK")
	# cursor1.execute("SHOW MASTER STATUS")
	# res=cursor1.fetchone()
	# cursor1.execute("UNLOCK TABLES")


class Mysql(object):
	__host = None
	__user = None
	__password = None
	__session = None
	__port = None
	__connection = None

	def __init__(self, port=3308, host='localhost', user='root', password='root@123'):
		self.__host = host
		self.__user = user
		self.__password = password
		self.__port = port

	def __del__(self):
		if self.__session:
			self.__session.close()
		if self.__connection:
			self.__connection.close()

# opens a new connection
	def _open(self):
		try:
			cnx = mysql.connector.connect(host=self.__host, user=self.__user, password=self.__password, port=self.__port)
			self.__connection = cnx
			self._create_session(True)
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
				print('Something is wrong with your user name or password')
			else:
				print(err)
			self.__connection = None
			self.__session = None
			exit()

#close the existing connection
	def _close(self):
		try:
			if self.__session is not None:
				self.__session.close()
			if self.__connection is not None:
				self.__connection.close()
		except Exception as e:
			print("exception occurred : " )

# performs a normal query which does not return anything like applying locks to database or tables
	def _nquery(self, q):
		if self.__connection is None:
			print("connection is not established")
			return False
		elif self.__session is None:
			print("no session is active. In order to create a session, call _create_session() ")
			return False
		else:
			try:
				self.__session.execute(q)
#				print("query succeeded")
				return True
			except:
				print("query failed : " + q)
				return False

#used for queries to get anything from server
	def _get_query(self, q):
		result = None
		if self.__connection is None:
			print("connection is not established")
		elif self.__session is None:
			print("no session is active. In order to create a session, call _create_session() ")
		else:
			try:
				self.__session.execute(q)
				result = self.__session.fetchall()
#				print("query succeeded")
			except:
				print("query failed : " + q)
				result = None
		return result

#creates a new cursor object if not already there
	def _create_session(self, isBuffered=True):
		if self.__connection is not None:
			if self.__session is not None:
				self.__session.close()
			try:
				self.__session = self.__connection.cursor(buffered=isBuffered)
			except:
				print("Error occurred")
		else:
			print("can't create session as connection is not established with server")

# db = Mysql(3311)
# db._open()
# db._nquery("FLUSH TABLES WITH READ LOCK")
# result=db._get_query("SHOW MASTER STATUS")
# print(result)
# db._nquery("UNLOCK TABLES")
# del db
