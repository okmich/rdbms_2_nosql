import os
import pickle


class Student:

	def __init__(self, id, first_name, last_name, email, city, age):
		self.id = id
		self.first_name = first_name
		self.last_name = last_name
		self.email = email
		self.city = city
		self.age = age


def read_personal_records(filename):
	'''
		reads the filename file on disk and parse each link to an
		instance of Student class
		returns all students
	'''
	all_records = []
	for line in open(filename, 'r'):
		parts = line.split(',')
		s = Student(int(parts[0]), parts[1], parts[2],
		            parts[3], parts[4], int(parts[5]))
		all_records.append(s)

	return all_records


if __name__ == '__main__':
    # directory to read from
	directory = 'F:\\Classes\\sql-nosql-hadoop\\rdbms_2_nosql\\python-pickle'
	# read file and parse into objects using the read_personal_records
	people = read_personal_records(os.path.join(directory, 'people.csv'))
	# write the list of people to disk using pickle library
	file_to_write = open(os.path.join(directory, 'people_pickled'), 'w+b')
	pickle.dump(people, file_to_write) 
    # read back what we wrote
	file_to_read = open(os.path.join(directory, 'people_pickled'), 'r+b')
	another_people = pickle.load(file_to_read)
	for p in another_people:
    		print(p.__dict__)
