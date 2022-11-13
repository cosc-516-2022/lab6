from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:

	def __init__(self):
		self.driver = None

	def connect(self, uri, user, password):
		# TODO : Complete Method to make a connection to the database
		self.driver = GraphDatabase.driver(uri, auth=(user, password))
	
		
	def close(self):
		# TODO : Complete Method to close the connection to the database
		self.driver.close()

	@staticmethod
	def _return_movie_name_and_year(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH(m:Movie) RETURN m.title as title, m.year as year"
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
		"""Display names of all movies and the year it released. 

		Keyword arguments:
		None

		:return: list of tuples, where each tuple contains (movie name, year)
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_write(self._return_movie_name_and_year)

		return result

	@staticmethod
	def _return_actor_come_directors(tx):
		# TODO : Remove Function, contains solution
		query = (
			"MATCH(x:Actor)-[p:DIRECTED]->(m:Movie) return x.name as name"
			)
		result = tx.run(query)
		try:
			return [(row["name"]) for row in result]
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def query2(self):
		# TODO :  Write query2() to display the names of the actors who have also directed at least one movie. 
		"""Display names of all actors who have directed atleast one movie. 

		Keyword arguments:
		None

		:return: list of string
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_write(self._return_actor_come_directors)

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
		"""Display total number of actors who were born in Canada. 

		Keyword arguments:
		None

		:return: total count
		:rtype: int
		:throws: Exception if an error occurs.
		"""
		with self.driver.session(database="neo4j") as session:
			result = session.execute_write(self._return_num_actors_bornin_canada)
		return int(result[0])

	
	

	"""def create_friendship(self, person1_name, person2_name):
		with self.driver.session(database="neo4j") as session:
			# Write transactions allow the driver to handle retries and transient errors
			result = session.execute_write(
				self._create_and_return_friendship, person1_name, person2_name)
			for row in result:
				print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))

	@staticmethod
	def _create_and_return_friendship(tx, person1_name, person2_name):
		# To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
		# The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
		query = (
			"CREATE (p1:Person { name: $person1_name }) "
			"CREATE (p2:Person { name: $person2_name }) "
			"CREATE (p1)-[:KNOWS]->(p2) "
			"RETURN p1, p2"
		)
		result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
		try:
			return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
					for row in result]
		# Capture any errors along with the query and data for traceability
		except ServiceUnavailable as exception:
			logging.error("{query} raised an error: \n {exception}".format(
				query=query, exception=exception))
			raise

	def find_person(self, person_name):
		with self.driver.session(database="neo4j") as session:
			result = session.execute_read(self._find_and_return_person, person_name)
			for row in result:
				print("Found person: {row}".format(row=row))

	@staticmethod
	def _find_and_return_person(tx, person_name):
		query = (
			"MATCH (p:Person) "
			"WHERE p.name = $person_name "
			"RETURN p.name AS name"
		)
		result = tx.run(query, person_name=person_name)
		return [row["name"] for row in result]"""


if __name__ == "__main__":
	# Aura queries use an encrypted connection using the "neo4j+s" URI scheme
	uri = "neo4j+s://dffc1342.databases.neo4j.io" #"neo4j+s://<Bolt url for Neo4j Aura instance>"
	user = "neo4j"
	password = "oYXwXqKvX4qA2eI8g49rpMSPu27kJ0ebTNoysPVUQvc"
	app = App()
	app.connect(uri, user, password)
	app.query1()
	app.query2()
	app.query3()
	#app.create_friendship("Alice", "David")
	#app.find_person("Alice")
	app.close()