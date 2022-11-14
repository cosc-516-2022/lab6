from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class Neo4jApp:

	uri = "neo4j+s://dffc1342.databases.neo4j.io" #"neo4j+s://<Bolt url for Neo4j Aura instance>"
	user = "neo4j"
	password = "oYXwXqKvX4qA2eI8g49rpMSPu27kJ0ebTNoysPVUQvc"

	def __init__(self):
		self.driver = None

	def connect(self, uri=None, user=None, password=None):
		if uri is None:
			uri = self.uri
		if user is None:
			user = self.user
		if password is None:
			password = self.password
		
		# TODO : Complete Method to make a connection to the database
		self.driver = GraphDatabase.driver(uri, auth=(user, password))
	
		
	def close(self):
		# TODO : Complete Method to close the connection to the database
		self.driver.close()

	@staticmethod
	def _load1(tx):
		# TODO : Remove Function, contains solution
		query = (
			"CREATE (a:Actor:Alien { bio:'Extraterrestrial', born:'1200-01-01', bornIn:'Mars', name:'ET' }) "
			"WITH a "
			"MATCH (m1:Movie),(m2:Movie) WHERE m1.title = 'E.T. the Extra-Terrestrial' AND m2.title = 'Jumanji' "
			"CREATE (a)-[:ACTED_IN]->(m1) "
			"CREATE (a)-[:ACTED_IN]->(m2) "
			"RETURN a"
		)
		result = tx.run(query)
		try:
			return [{"a": row["a"]["name"]} for row in result]
		# Capture any errors along with the query and data for traceability
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise
	
	@staticmethod
	def _load2(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH (p:Person),(m:Movie) WHERE ID(p) = 34047 AND m.title = 'Jumanji' "
			"CREATE (p)-[:REVIEWED {rating:90, summary:'It was fun to watch in 3D'}]->(m) "
			"RETURN p"
		)
		result = tx.run(query)
		try:
			return [{"p": row["p"]["name"]} for row in result]
		# Capture any errors along with the query and data for traceability
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def loadData(self):
		# TODO : Write function to load data as per given instructions
		"""
		load data as per given instructions

		Keyword arguments:
		None

		:throws: Exception if an error occurs.
		"""
		#load 1
		with self.driver.session(database="neo4j") as session:
			result = session.execute_write(self._load1)

		#load 2
		with self.driver.session(database="neo4j") as session:
			result = session.execute_write(self._load2)

		#for row in result:
		#	print(result)
	

	@staticmethod
	def _return_movie_name_and_year(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH(m:Movie) RETURN m.title as title, m.year as year ORDER BY year"
			)
		result = tx.run(query)
		try:
			return [(row["title"], row["year"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query1(self):
		# TODO : Write query1() to display names of all movies and the year it released. 
		"""Returns names of all movies and the year it released. 

		Keyword arguments:
		None

		:return: list of tuples, where each tuple contains (movie name, year)
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._return_movie_name_and_year)
		
		compString = ""
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "("+str(r[0])+","+str(r[1])+")"
			else:
				compString = compString +","+ "("+str(r[0])+","+str(r[1])+")"
		with open("./data.txt", "w",  encoding="utf-8") as f:
			f.write(compString)
			
		return result

	@staticmethod
	def _return_actor_come_directors(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH(x:Actor)-[p:DIRECTED]->(m:Movie) return DISTINCT x.name as name ORDER BY name"
			)
		result = tx.run(query)
		try:
			return [(row["name"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query2(self):
		# TODO :  Write query2() to display the names of the actors who have also directed at least one movie. No Duplicates. 
		"""Returns names of all actors who have directed atleast one movie. 

		Keyword arguments:
		None

		:return: list of string
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._return_actor_come_directors)

		compString = ""
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "['"+ str(r) + "'"
			else:
				compString = compString +",'"+ str(r) +"'"
		compString = compString + "]"
		with open("./data.txt", "w",  encoding="utf-8") as f:
			f.write(compString)
		print(compString)

		#print(result)

		return result

	@staticmethod
	def _return_num_actors_bornin_canada(tx):
		# TODO : Remove Function, contains solution
		query = (
			"match(a:Actor) where a.bornIn contains 'Canada' return Count(*) as total"
			)
		result = tx.run(query)
		try:
			return [(row["total"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query3(self):
		# TODO :  Write query3() to count the number of actors present in the database, who were born in Canada.  
		"""Returns total number of actors who were born in Canada. 

		Keyword arguments:
		None

		:return: total count
		:rtype: int
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._return_num_actors_bornin_canada)
		return int(result[0])

	@staticmethod
	def _return_user_average_rating(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH(u:User)-[r:RATED]->(m:Movie) WITH avg(r.rating) as averageRating, u.name as name return name, round(averageRating * 100) / 100 as averageRating ORDER BY averageRating DESC"
			)

		result = tx.run(query)
		try:
			return [(row["name"], row["averageRating"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query4(self):
		# TODO :  Write query4() to compute the average rating given (excludes reviews) by an user 
		# for all movies sorted by descending order. Average rating rounded to 2 decimal places.
		"""Returns names of all users and their average rating sorted by descending order. 

		Keyword arguments:
		None

		:return: list of tuples, where each tuple contains (user name, average rating)
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._return_user_average_rating)

		compString = ""
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "[("+str(r[0])+","+str(r[1])+")"
			else:
				compString = compString +","+ "("+str(r[0])+","+str(r[1])+")"
		compString = compString+"]"
		with open("./data.txt", "w",  encoding="utf-8") as f:
			f.write(compString)

		print(compString)
		return result
	
	@staticmethod
	def _person_loved_movies(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH(p:Person)-[r:REVIEWED]->(m:Movie) Where r.rating > 50 and r.summary CONTAINS 'fun' return p.name as name, m.title as title ORDER BY name, title DESC"
			)
		result = tx.run(query)
		try:
			return [(row["name"], row["title"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query5(self):
		# TODO :  Write query5() to to return person (name only) and movie name
		# Where it's associated review (by the same person) has
		# rating > 50 and the review has atleast one mention of the word "fun" ordered by person name (asc), movie name (desc)
		"""Returns names of all person and respective movies. 

		Keyword arguments:
		None

		:return: list of tuples, where each tuple contains (person name, movie)
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._person_loved_movies)

		print(result)
		return result

	@staticmethod
	def _num_second_order_conn(tx, name):
		# TODO : Remove Function, contains solution
		query = (
			"match(p:Person{name:$person_name})-[f:FOLLOWS*1]->(p1:Person) return count(*) as total"
			)
		result = tx.run(query, person_name=name)
		try:
			return [(row["total"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query6(self, name):
		# TODO : Count the number of second order connections for a given person
		"""Returns total number of second order connections for a given person.

		Keyword arguments:
		name : str

		:return: total count
		:rtype: int
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._num_second_order_conn, name)

		print(int(result[0]))
		return int(result[0]) 


if __name__ == "__main__":
	# Aura queries use an encrypted connection using the "neo4j+s" URI scheme
	uri = "neo4j+s://dffc1342.databases.neo4j.io" #"neo4j+s://<Bolt url for Neo4j Aura instance>"
	user = "neo4j"
	password = "oYXwXqKvX4qA2eI8g49rpMSPu27kJ0ebTNoysPVUQvc"
	#app = Neo4jApp()
	#app.connect()
	#app.loadData()
	#app.query1()
	#app.query2()
	#app.query3()
	#app.query4()
	#app.query5()
	#app.query6("Paul Blythe")
	#app.close()