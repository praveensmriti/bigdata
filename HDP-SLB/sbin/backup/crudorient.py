#!/usr/bin/env python

import pyorient
client = pyorient.OrientDB("localhost", 2424)  # host, port

# open a connection (username and password)
client.connect("root", "Seema0210")



# create a database
try:
    client.db_create("animals", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
except:
    print "Passing db create error..."

# select to use that database
client.db_open("animals", "admin", "admin")

# Create the Vertex Animal

try:
    client.command("create class Animal extends V")
except:
    print "Passing create Animal class error ..."

# Insert a new value
#client.command("insert into Animal set name = 'rat', specie = 'rodent'")
client.command("insert into Animal content {}".format("{'name': 'rat','specie': 'rodent','address': {'city': 'sunnyvale','country': 'US'}}")) 

# query the values 
client.query("select * from Animal")

# Create the vertex and insert the food values

try:
    client.command('create class Food extends V')
except:
    print "Passing create class Food error ...."

client.command("insert into Food set name = 'pea', color = 'green'")

# Create the edge for the Eat action
try:
    client.command('create class Eat extends E')
except:
    print "Passing create class Eat error ...."

# Lets the rat likes to eat pea
'''eat_edges = client.command("""
        create edge Eat
        from (select from Animal where name = 'rat')
        to (select from Food where name = 'pea')
 """)'''

#eat_edges = client.command('create edge Eat from #11:0 to #12:0')

eat_edges = client.command("create edge Eat from (select from Animal where name = 'rat')\
                                              to (select from Food where name = 'pea')")

print "Created the edge ..."

# Who eats the peas?
pea_eaters = client.command("select expand( in( Eat )) from Food where name = 'pea'")
print "pea_eaters {}".format(pea_eaters)
for animal in pea_eaters:
    print(animal.name, animal.specie)

# What each animal eats?
animal_foods = client.command("select expand( out( Eat )) from Animal")
print "Animal Foods {}".format(animal_foods)
for food in animal_foods:
    animal = client.query(
                "select name from ( select expand( in('Eat') ) from Food where name = 'pea' )"
            )[0]
    print(food.name, food.color, animal.name)
