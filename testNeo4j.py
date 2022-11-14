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

	def test_query1(self):
		app = Neo4jApp()
		app.connect()

		compareString = "" #stores pre-computed solution
		with open('./misc/query1.txt',mode='r', encoding="utf-8") as file:
			# read all lines at once
			compareString = file.read()
		compString = ""
		result = app.query1()
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "("+str(r[0])+","+str(r[1])+")"
			else:
				compString = compString +","+ "("+str(r[0])+","+str(r[1])+")"
		self.assertEquals(compString, compareString)
		app.close()
 
	def test_query2(self):
		app = Neo4jApp()
		app.connect()
		
		# Test here
		compareString = "" #stores pre-computed solution
		with open('./misc/query2.txt',mode='r', encoding="utf-8") as file:
			# read all lines at once
			compareString = file.read()
		compString = ""
		result = app.query2()
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "['"+ str(r) + "'"
			else:
				compString = compString +",'"+ str(r) +"'"
		compString = compString + "]"
		self.assertEquals(compString, compareString)


		app.close()

	def test_query3(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		result = app.query3()
		self.assertEquals(result, 451)
		
		app.close()
 
	def test_query4(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		compareString = "" #stores pre-computed solution
		with open('./misc/query4.txt',mode='r', encoding="utf-8") as file:
			# read all lines at once
			compareString = file.read()
		compString = ""
		result = app.query4()
		for index,r in enumerate(result):
			if index == 0:
				compString = compString + "[("+str(r[0])+","+str(r[1])+")"
			else:
				compString = compString +","+ "("+str(r[0])+","+str(r[1])+")"
		compString = compString+"]"
		self.assertEquals(compString, compareString)
		
		app.close()

	def test_query5(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		compareTo = [('Angela Scope', 'The Replacements'), ('Jessica Thompson', 'The Replacements'), ('Jessica Thompson', 'Jumanji')]
		result = app.query5()
		self.assertEquals(result, compareTo)
		
		app.close()
 
	def test_query6(self):
		app = Neo4jApp()
		app.connect()

		# Test here
		self.assertEquals(app.query6("Paul Blythe"),1)
		
		app.close()
	
	
if __name__ == "__main__":
	unittest.main()