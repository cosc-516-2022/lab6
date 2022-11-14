from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class Neo4jApp:

	# Replace this with your personal uri, user (default: neo4j), password
	uri = "neo4j+s://########.databases.neo4j.io" #"neo4j+s://<Bolt url for Neo4j Aura instance>"
	user = "neo4j"
	password = "#######################"

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
	
		
	def close(self):
		# TODO : Complete Method to close the connection to the database
		return # remove


	def loadData(self):
		# TODO : Write function to load data as per given instructions
		"""
		load data as per given instructions

		Keyword arguments:
		None

		:throws: Exception if an error occurs.
		"""


	def query1(self):
		# TODO : Write query1() to display names of all movies and the year it released. 
		"""Returns names of all movies and the year it released. 

		Keyword arguments:
		None

		:return: list of tuples, where each tuple contains (movie name, year)
		:rtype: list
		:throws: Exception if an error occurs.
		"""


	def query2(self):
		# TODO :  Write query2() to display the names of the actors who have also directed at least one movie. No Duplicates. 
		"""Returns names of all actors who have directed atleast one movie. 

		Keyword arguments:
		None

		:return: list of string
		:rtype: list
		:throws: Exception if an error occurs.
		"""
		

	def query3(self):
		# TODO :  Write query3() to count the number of actors present in the database, who were born in Canada.  
		"""Returns total number of actors who were born in Canada. 

		Keyword arguments:
		None

		:return: total count
		:rtype: int
		:throws: Exception if an error occurs.
		"""
		

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


	def query6(self, name):
		# TODO : Count the number of second order connections for a given person
		"""Returns total number of second order connections for a given person.

		Keyword arguments:
		name : str

		:return: total count
		:rtype: int
		:throws: Exception if an error occurs.
		"""


if __name__ == "__main__":
	# Aura queries use an encrypted connection using the "neo4j+s" URI scheme
	app = Neo4jApp()
	app.connect()
	#app.loadData() # Run only once to avoid duplicate data.
	app.query1()
	app.query2()
	app.query3()
	app.query4()
	app.query5()
	app.query6("Paul Blythe")
	app.close()