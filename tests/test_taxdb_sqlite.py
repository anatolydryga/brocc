import tempfile
import unittest
import sqlite3
import os
from StringIO import StringIO

from brocclib.taxdb_sqlite import TaxDB_sqlite
from brocclib.taxonomy_db import TaxDB

gi_taxid = """\
409692259	10407
"""

names = """\
1	|	root	|		|	scientific name	|
10239	|	Viruses	|		|	scientific name	|
35268	|	Retro-transcribing viruses	|		|	scientific name	|
10404	|	Hepadnaviridae	|		|	scientific name	|
10405	|	Orthohepadnavirus	|		|	scientific name	|
10407	|	Hepatitis B virus	|		|	scientific name	|
"""

nodes = """\
1	|	1	|	no rank	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
10239	|	1	|	superkingdom	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
35268	|	10239	|	no rank	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
10404	|	35268	|	family	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
10405	|	10404	|	genus	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
10407	|	10405	|	species	|		|	8	|	0	|	1	|	0	|	0	|	0	|	0	|	0	|		|
"""

class Test_TaxDB_sqlite(unittest.TestCase):

    def setUp(self):
        _, self.db = tempfile.mkstemp()
        tax_db_builder = TaxDB(self.db, StringIO(gi_taxid), StringIO(nodes), StringIO(names))
        tax_db_builder.close()
        self.tax_db_sqlite = TaxDB_sqlite(self.db)

    def tearDown(self):
        os.remove(self.db)
        self.tax_db_sqlite.close()
    
    def test_get_taxon_id(self):
        self.assertEqual(self.tax_db_sqlite.get_taxon_id(409692259), 10407)
        self.assertEqual(self.tax_db_sqlite.get_taxon_id(123), None)

    def test_get_lineage_no_such_taxid(self):
        self.assertEqual(self.tax_db_sqlite.get_lineage(123456789), None)

    def test_get_lineage_for_root(self):
        self.assertEqual(self.tax_db_sqlite.get_lineage(1), {})

    def test_get_lineage_for_viruses(self):
        lineage = self.tax_db_sqlite.get_lineage(10239)
        self.assertEqual(len(lineage), 1)
        self.assertTrue("superkingdom" in lineage)
        self.assertEqual(lineage["superkingdom"], "Viruses")

    def test_get_lineage_for_hepatitis(self):
        lineage = self.tax_db_sqlite.get_lineage(10407)
        self.assertEqual(len(lineage), 4)
        self.assertFalse("no rank" in lineage)
        self.assertTrue("superkingdom" in lineage)
        self.assertTrue("family" in lineage)
        self.assertTrue("genus" in lineage)
        self.assertTrue("species" in lineage)
        self.assertEqual(lineage["superkingdom"], "Viruses")
        self.assertEqual(lineage["family"], "Hepadnaviridae")
        self.assertEqual(lineage["genus"], "Orthohepadnavirus")
        self.assertEqual(lineage["species"], "Hepatitis B virus")
        
if __name__ == "__main__":
    unittest.main()
