# -*- coding: utf-8 -*-
__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os

os.environ['DEBUG'] = "0"
os.environ['DEBUG_VERBOSE'] = "0"

import pyorient


class GraphUsageTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GraphUsageTestCase, self).__init__(*args, **kwargs)
        self.client = None
        self.cluster_info = None

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")

        db_name = "animals"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = self.client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                                       pyorient.STORAGE_TYPE_MEMORY)
            pass

        self.cluster_info = self.client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

    def testGraph(self):

        # Create the Vertex Animal
        self.client.command("create class Animal extends V")

        # Insert a new value
        self.client.command("insert into Animal set name = 'rat', specie = 'rodent'")

        # query the values 
        animal = self.client.query("select * from Animal")[0].oRecordData
        assert 'specie' in animal
        assert 'name' in animal

        # add data for tests

        self.client.command("insert into Animal set name = 'capybara', specie = 'rodent'")
        self.client.command("insert into Animal set name = 'chipmunk', specie = 'rodent'")
        self.client.command("insert into Animal set name = 'wolf', specie = 'canis'")
        self.client.command("insert into Animal set name = 'coyote', specie = 'canis'")

        # parameterized query

        sql = "select * from animal where name = ? or name = ?"

        params = ['rat', 'chipmunk']
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 2
        params = ['capybara', 'chipmunk']
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 2
        params = ['squirrel', 'hamster']
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 0
        params = ['capybara']
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 1
        params = []
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 0

        sql = "select * from animal where specie = ?"

        params = ['canis']
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 2

        animals = self.client.query_parameterized(sql, params, 1)
        assert len(animals) == 1

        sql = "select * from animal where name = ? and specie = ?"

        params = ['wolf', 'canis']
        animals = self.client.query_parameterized(sql, params)
        assert len(animals) == 1


        # Create the vertex and insert the food values

        self.client.command('create class Food extends V')
        self.client.command("insert into Food set name = 'pea', color = 'green'")

        # Create the edge for the Eat action
        self.client.command('create class Eat extends E')

        # Lets the rat likes to eat pea
        eat_edges = self.client.command(
            "create edge Eat from ("
            "select from Animal where name = 'rat'"
            ") to ("
            "select from Food where name = 'pea'"
            ")"
        )

        # Who eats the peas?
        pea_eaters = self.client.command("select expand( in( Eat )) from Food where name = 'pea'")
        for animal in pea_eaters:
            print(animal.name, animal.specie)
        'rat rodent'

        # What each animal eats?
        animal_foods = self.client.command("select expand( out( Eat )) from Animal")
        for food in animal_foods:
            animal = self.client.query(
                        "select name from ( select expand( in('Eat') ) from Food where name = 'pea' )"
                    )[0]
            print(food.name, food.color, animal.name)
        'pea green rat'