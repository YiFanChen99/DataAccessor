# -*- coding: utf-8 -*-
from peewee import *
import unittest
import datetime


db = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = db

    @classmethod
    def get_column_names(cls):
        meta_data = db.get_columns(cls.__name__)
        return [datum.name for datum in meta_data]

    @classmethod
    def atomic(cls):
        return db.atomic()

    @classmethod
    def create(cls, **kwargs):
        """
        Delegate create with IntegrityError wrapped.
        """
        try:
            return super().create(**kwargs)
        except IntegrityError as ex:
            raise ValueError("IntegrityError") from ex


class _MockFlesh(BaseModel):
    date = DateField(default=datetime.datetime.now)
    count = FloatField(default=1.0)


class _BaseModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.init("./DbOrmAccessorUnittest.db")

    def test_create_and_delect(self):
        self.assertEqual(2, _MockFlesh.select().count())

        with db.atomic() as txn:
            new_instance = _MockFlesh.create(count=0.4)
            self.assertEqual(3, _MockFlesh.select().count())
            new_instance.delete_instance()
            self.assertEqual(2, _MockFlesh.select().count())

            _MockFlesh.create(date="2018-07-20")
            _MockFlesh.create(date="2018-07-20", count=0.9)
            query = _MockFlesh.delete().where(_MockFlesh.date == "2018-07-20")
            query.execute()
            self.assertEqual(2, _MockFlesh.select().count())

            txn.rollback()

    def test_select_with_condition(self):
        self.assertEqual(1, _MockFlesh.select().where(_MockFlesh.count < 0.4).count())
        self.assertEqual(2, _MockFlesh.select().where(_MockFlesh.count < 0.6).count())

        self.assertEqual(2, _MockFlesh.select().where(
            (_MockFlesh.count == 0.1) | (_MockFlesh.date == "2018-08-21")).count())

    def test_get_columns_name(self):
        self.assertEqual(['id', 'date', 'count'], _MockFlesh.get_column_names())


if __name__ == "__main__":
    unittest.main()
