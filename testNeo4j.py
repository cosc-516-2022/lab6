import unittest
from AssignmentNeo4j import Neo4jApp 

def square(n):
	return n*n
 
def cube(n):
	return n*n*n
 
class Test(unittest.TestCase):

	#app = Neo4jApp()
	#app.connect()
	#app.loadData()

	def query1(self):
		app = Neo4jApp()
		app.connect()

		compareString = ""
		result = app.query1()
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "("+str(r[0])+","+str(r[1])+")"
			else:
				compString = compString +","+ "("+str(r[0])+","+str(r[1])+")"
		self.assertEquals("", compareString)

		app.close()
 
	def query2(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		self.assertEquals(1,1)

		app.close()

	def query3(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		self.assertEquals(1,1)
		
		app.close()
 
	def query4(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		self.assertEquals(1,1)
		
		app.close()

	def query5(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		self.assertEquals(1,1)
		
		app.close()
 
	def query6(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		self.assertEquals(1,1)
		
		app.close()