# -*- coding: utf-8 -*-
import sqlite3
import unittest


class DbAccessor(object):
    def __init__(self, path=""):
        object.__init__(self)
        self.sql = sqlite3.connect(path)

    def commit(self):
        self.sql.commit()

    def rollback(self):
        self.sql.rollback()

    def execute(self, query):
        return self.sql.execute(query)

    def select(self, table_name, column_names="*", conditions=1):
        return self.sql.execute("select {0} from {1} where {2}".format(
            ','.join(column_names), table_name, conditions
        ))

    def insert(self, table_name, *data):
        return self.sql.execute("insert into {0} values({1})".format(
            table_name, self._convert_data_to_insert_query(*data))
        )

    def update(self, table_name, column_names, data, conditions=1):
        return self.sql.execute("update {0} set {1} where {2}".format(
            table_name, self._convert_data_to_update_query(column_names, data), conditions
        ))

    def delete(self, table_name, conditions=1):
        return self.sql.execute("delete from {0} where {1}".format(
            table_name, conditions
        ))

    @staticmethod
    def _convert_datum_to_query(datum):
        if datum == 'None' or datum is None:
            return '\"\"'

        try:  # 若為數值類資訊，不必加「"」
            int(datum)
            return str(datum)
        except (ValueError, AttributeError):  # 為字串資料時
            return '\"' + DbAccessor._convert_to_str(datum) + '\"'

    @staticmethod
    def _convert_data_to_insert_query(*data):
        """ 組成「"x1,x2,...,xn"」的字串回傳 """
        return ','.join([DbAccessor._convert_datum_to_query(datum) for datum in data])

    @staticmethod
    def _convert_data_to_update_query(column_names, data):
        """ 組成「"col1=val1,col2=val2,...,coln=valn"」的字串回傳 """
        if len(column_names) != len(data):
            raise sqlite3.OperationalError("Different list lens")

        pairs = map(lambda x, y: x + "=" + DbAccessor._convert_datum_to_query(y), column_names, data)
        return ','.join(pairs)

    @staticmethod
    def _convert_to_str(value):
        if value is None:
            return ''
        else:
            return value


def print_records(records):
    """ For debugging """
    for row in records:
        print(row)


class _DbAccessorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.accessor = DbAccessor("./DbAccessorUnittest.db")

    def tearDown(self):
        self.accessor.rollback()

    def test_select_all(self):
        records = self.accessor.select("Flesh").fetchall()
        self.assertEqual(6, len(records))  # rows
        self.assertEqual(3, len(records[0]))  # columns

    def test_select_partial_columns(self):
        first_record = self.accessor.select("Flesh", column_names=["Date"]).fetchone()
        self.assertEqual(1, len(first_record))

        first_record = self.accessor.select("Flesh", ["Id", "Count"]).fetchone()
        self.assertEqual(2, len(first_record))

        first_record = self.accessor.select("Flesh", ["*"]).fetchone()
        self.assertEqual(3, len(first_record))
        first_record = self.accessor.select("Flesh", "*").fetchone()
        self.assertEqual(3, len(first_record))

    def test_select_with_conditions(self):
        records = self.accessor.select("Flesh", conditions="Id>4").fetchall()
        self.assertEqual(2, len(records))

        records = self.accessor.select("Flesh", ["*"], "Id=9").fetchall()
        self.assertEqual(0, len(records))

    def test_update(self):
        records = self.accessor.select("Flesh", conditions="Date=\"2018-07-29\"").fetchall()
        self.assertEqual(1, len(records))  # pre-check

        self.accessor.update("Flesh", ["Date"], ["2018-07-29"], "Id>4")

        records = self.accessor.select("Flesh", conditions="Date=\"2018-07-29\"").fetchall()
        self.assertEqual(3, len(records))

    def test_insert(self):
        records = self.accessor.select("Flesh", conditions="Date=\"2018-07-29\"").fetchall()
        self.assertEqual(1, len(records))  # pre-check

        self.accessor.insert("Flesh", 15, "2018-07-29", 0.1)
        self.accessor.insert("Flesh", 16, "2018-07-30", 0.6)

        records = self.accessor.select("Flesh", conditions="Date=\"2018-07-29\"").fetchall()
        self.assertEqual(2, len(records))

    def test_insert_duplicated_Id(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.accessor.insert("Flesh", 4, "2018-07-29", 0.1)

    def test_delete(self):
        records = self.accessor.select("Flesh", conditions="Date=\"2018-07-29\"").fetchall()
        self.assertEqual(1, len(records))  # pre-check

        self.accessor.delete("Flesh", "Id=1")
        records = self.accessor.select("Flesh", conditions="Date=\"2018-07-29\"").fetchall()
        self.assertEqual(0, len(records))

        self.accessor.delete("Flesh")
        records = self.accessor.select("Flesh").fetchall()
        self.assertEqual(0, len(records))


if __name__ == "__main__":
    unittest.main()
