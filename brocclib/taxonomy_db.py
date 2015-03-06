#!/usr/bin/python

import optparse
import sqlite3
import os.path
import itertools
import zipfile

class TaxDB:

    SCHEMA = """\
    PRAGMA foreign_keys = ON;

    DROP TABLE IF EXISTS nodes;
    DROP TABLE IF EXISTS gi_taxid;

    CREATE TABLE nodes(
      taxid INT NOT NULL PRIMARY KEY,
      parent_id INT NOT NULL,
      rank TEXT,
      name TEXT);

    CREATE TABLE gi_taxid(
           gi INT NOT NULL PRIMARY KEY,
        taxid INT NOT NULL,
         FOREIGN KEY(taxid) REFERENCES nodes(taxid));
    """

    temp_tables = """\
    CREATE TEMP TABLE dmp_nodes (
      tax_id INT NOT NULL PRIMARY KEY,
      parent_id INT NOT NULL,
      rank TEXT);
 
    CREATE TEMP TABLE dmp_names (
      tax_id INT NOT NULL PRIMARY KEY,
      name TEXT);
    """

    merge_temp_tables = """\
    INSERT INTO nodes 
        SELECT dmp_nodes.tax_id, parent_id, rank, name
        FROM dmp_nodes
        INNER JOIN dmp_names
        ON dmp_nodes.tax_id = dmp_names.tax_id
    """

    def __init__(self, database, gi_taxid, nodes, names):
        """Create a new SQLite3 database for the NCBI taxonomy.
        Args:
            database string with filename for a database
            gi_taxid filehandle for gi_taxid_{prot|nucl}.dmp file
            nodes filehandle for nodes.dmp file(from taxdmp.zip)
            names filehandle for names.dmp file(from taxdmp.zip)

        Note:
            all files are available from NCBI taxonomy DB ftp site
        """
        print "Starting creation of DB."
        self.db_conn = sqlite3.connect(database)
        self.db_conn.executescript(self.SCHEMA)
        self.db_conn.executescript(self.temp_tables)
        print "Starting creation of nodes-names table."
        self._insert_nodes_names(nodes, names)
        print "nodes-names table created."
        print "Starting creation of gi-taxid table."
        self._insert_gi_taxid(gi_taxid)
        print "gi-taxid table created."
        self.db_conn.commit()
        print "DB created."

    def close(self):
        self.db_conn.close()

    def _parse_gi_taxid(self, f):
        counter = 1
        for line in f:
            line = line.rstrip()
            if counter % 10000 == 0:
                print "processed " + str(counter) + " # of gi-taxid pairs."
            counter += 1
            if line:
                split_line = line.split("\t")
                if len(split_line) != 2:
                    raise ValueError("GI-taxid file should have 2 columns per line.")
                taxid = int(split_line[1])
                if self._is_taxid_exist(taxid):
                    yield split_line

    def _parse_ncbi_dmp(self, f):
        for line in f:
            line = line.rstrip("\t|\n")
            if line:
                yield line.split("\t|\t")

    def _parse_names(self, f):
        rows = self._parse_ncbi_dmp(f)
        return (r[0:2] for r in rows if len(r) >= 4 and r[3] == "scientific name")

    def _parse_nodes(self, f):
        rows = self._parse_ncbi_dmp(f)
        return (r[0:3] for r in rows if len(r) >= 3)

    def _insert_many(self, input_for_parser, parse_fcn, insert_sql):
        rows = parse_fcn(input_for_parser)
        self.db_conn.execute("PRAGMA foreign_keys = ON;")
        self.db_conn.executemany(insert_sql, rows)
        self.db_conn.commit()

    def _insert_names(self, f):
        sql = "INSERT INTO dmp_names VALUES (?,?)"
        return self._insert_many(f, self._parse_names, sql)

    def _insert_nodes(self, f):
        sql = "INSERT INTO dmp_nodes VALUES (?,?,?)"
        return self._insert_many(f, self._parse_nodes, sql)

    def _insert_nodes_names(self, nodes, names):
        self._insert_nodes(nodes)
        self._insert_names(names)
        self.db_conn.executescript(self.merge_temp_tables)

    def _insert_gi_taxid(self, f):
        sql = "INSERT INTO gi_taxid VALUES (?,?)"
        self._insert_many(f, self._parse_gi_taxid, sql)

    def _is_taxid_exist(self, taxid):
        sql = "SELECT COUNT(*) FROM nodes WHERE taxid = ?"
        ans = self.db_conn.execute(sql, (taxid, ))
        row = ans.fetchone()
        return len(row) > 0 and row[0] == 1


def unzip_one_file(archive):
    """ unzipping archive.

    Raises:
        ValueError if archive does not have exactly 1 file.

    Note:
        Side effect: file is unzipped in the current directory
    """
    zip_archive = zipfile.ZipFile(archive)
    files = zip_archive.namelist()
    if len(files) != 1:
        raise ValueError("Zip archive " + archive + " should only have 1 file.")
    singleton = files[0]
    return zip_archive.extract(singleton)

def validate_file(filename, archive, files_in_archive):
    """ check that file is in archive.
    """
    if not filename in files_in_archive:
        raise ValueError("File " + filename + " should be in " + archive + " acrhive.")

def unzip_nodes_names(archive):
    """ extact nodes.dmp and names.dmp
        Side effect: files are unzipped in the current directory
    """
    zip_archive = zipfile.ZipFile(archive)
    files = zip_archive.namelist()
    nodes = 'nodes.dmp'
    names = 'names.dmp'
    validate_file(nodes, archive, files)
    validate_file(names, archive, files)
    return (zip_archive.extract(nodes), zip_archive.extract(names))

def main(argv=None):
    p = optparse.OptionParser()
    p.add_option("--gi_taxid", help="Path to gzipped taxid nucleotide(or protein) file")
    p.add_option("--taxdmp", help="Path to gzipped taxdmp file")

    p.add_option("--out", help="Output filepath for taxonomy sqlite3 database")

    opts, args = p.parse_args(argv)

    if os.path.isfile(opts.out):
       p.error("Database file " + opts.out + " already exists.  Please delete first.")
    if not os.path.isfile(opts.gi_taxid):
        p.error("GI to taxid file for nucleotide sequences not found.")
    if not os.path.isfile(opts.taxdmp):
        p.error("Taxdmp file not found.")

    (nodes, names) = unzip_nodes_names(opts.taxdmp) 
    print "Nodes and Names files are unzipped."
    gi_taxid = unzip_one_file(opts.gi_taxid)
    print "GI to Taxid file is unzipped."

    tax_db = TaxDB(opts.out, open(gi_taxid), open(nodes), open(names))
    tax_db.close()

if __name__== "__main__":
    main()
