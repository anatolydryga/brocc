import tempfile
import unittest
import sqlite3
import os
from StringIO import StringIO

from brocclib.taxonomy_db import TaxDB

gi_taxid = """\
2	1
3	1
4	2
5	2
"""

# taxid is not in names and nodes thus cannot be proccessed
gi_taxid_with_invalid_taxid = """\
2	1
4	2
100	123456789
"""

names = """\
1	|	all	|		|	synonym	|
1	|	root	|		|	scientific name	|
2	|	Bacteria	|	Bacteria <prokaryote>	|	scientific name	|
2	|	Monera	|	Monera <Bacteria>	|	in-part	|
2	|	Procaryotae	|	Procaryotae <Bacteria>	|	in-part	|
2	|	not Bacteria Haeckel 1894	|		|	synonym	|
"""

nodes = """\
1	|	1	|	no rank	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
2	|	131567	|	superkingdom	|		|	0	|	0	|	11	|	0	|	0	|	0	|	0	|	0	|		|
6	|	335928	|	genus	|		|	0	|	1	|	11	|	1	|	0	|	1	|	0	|	0	|		|
"""

class Test_TaxDB(unittest.TestCase):
    def setUp(self):
        _, self.db = tempfile.mkstemp()
        self.tax_db = TaxDB(self.db, StringIO(gi_taxid), StringIO(nodes), StringIO(names))

    def tearDown(self):
        os.remove(self.db)
        self.tax_db.close()
    
    def test_init_db(self):
        conn = sqlite3.connect(self.db)
        obs = list(conn.execute(
            'SELECT name FROM sqlite_master WHERE type = "table"'))
        tables = [x[0] for x in obs]
        self.assertEqual(set(tables), set(["gi_taxid", "nodes"]))

    def test_parse_names(self):
        f = StringIO(names)
        obs = list(self.tax_db._parse_names(f))
        self.assertEqual(obs, [["1", "root"], ["2", "Bacteria"]])

    def test_parse_nodes(self):
        f = StringIO(nodes)
        obs = list(self.tax_db._parse_nodes(f))
        self.assertEqual(obs, [
            ["1", "1", "no rank"],
            ["2", "131567", "superkingdom"],
            ["6", "335928", "genus"]])

    def test_insert_nodes_names(self):
        conn = sqlite3.connect(self.db)
        obs = list(conn.execute('SELECT * FROM nodes'))
        exp = [
            (1, 1, "no rank", "root"),
            (2, 131567, "superkingdom", "Bacteria")
            ]
        self.assertEqual(obs, exp)

    def test_taxid_search(self):
        self.assertTrue(self.tax_db._is_taxid_exist(1))
        self.assertTrue(self.tax_db._is_taxid_exist(2))
        self.assertFalse(self.tax_db._is_taxid_exist(3))

    def test_parse_gi_taxid(self):
        f = StringIO(gi_taxid)
        obs = list(self.tax_db._parse_gi_taxid(f))
        exp = [["2", "1"], ["3", "1"], ["4", "2"], ["5", "2"]]
        self.assertEqual(obs, exp)

    def test_insert_gi_taxid(self):
        conn = sqlite3.connect(self.db)
        obs = list(conn.execute('SELECT * FROM gi_taxid'))
        exp = [(2, 1), (3, 1), (4, 2), (5, 2)]
        self.assertEqual(obs, exp)

    def test_insert_taxid_with_invalid_taxid(self):
        _, db = tempfile.mkstemp()
        tax_db = TaxDB(db, StringIO(gi_taxid_with_invalid_taxid), StringIO(nodes), StringIO(names))
        conn = sqlite3.connect(db)
        obs = list(conn.execute('SELECT * FROM gi_taxid'))
        exp = [(2, 1), (4, 2)]
        self.assertEqual(obs, exp)
        
if __name__ == "__main__":
    unittest.main()
