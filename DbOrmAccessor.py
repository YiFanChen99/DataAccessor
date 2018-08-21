# -*- coding: utf-8 -*-
from peewee import *
import unittest
import datetime


db = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = db


class _MockFlesh(BaseModel):
    date = DateField(default=datetime.datetime.now)
    count = FloatField(default=1.0)


class _BaseModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.init("./DbOrmAccessorUnittest.db")

    def test_create_and_delect(self):
        self.assertEqual(2, _MockFlesh.select().count())

        new_instance = _MockFlesh.create(count=0.4)
        self.assertEqual(3, _MockFlesh.select().count())
        new_instance.delete_instance()
        self.assertEqual(2, _MockFlesh.select().count())

        _MockFlesh.create(date="2018-07-20")
        _MockFlesh.create(date="2018-07-20", count=0.9)
        query = _MockFlesh.delete().where(_MockFlesh.date == "2018-07-20")
        query.execute()
        self.assertEqual(2, _MockFlesh.select().count())

    def test_select_with_condition(self):
        self.assertEqual(1, _MockFlesh.select().where(_MockFlesh.count < 0.4).count())
        self.assertEqual(2, _MockFlesh.select().where(_MockFlesh.count < 0.6).count())

        self.assertEqual(2, _MockFlesh.select().where(
            (_MockFlesh.count == 0.1) | (_MockFlesh.date == "2018-08-21")).count())


if __name__ == "__main__":
    unittest.main()
