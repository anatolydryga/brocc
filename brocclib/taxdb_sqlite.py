import sqlite3

class TaxDB_sqlite:
    """ Access sqlite taxonomy db and provides API to 
        find taxonomy and lineage for sequences.

        Note:
            connection to sqlite should be explictly closed 
            by close() method.
    """

    def __init__(self, database):
        self.db_conn = sqlite3.connect(database)
        self.SKIP_NODES =  ['no rank'] 
        self.MAX_DEPTH = 30
        self.SENTINEL_NODE = 1 # NCBI denotes the root node as 1 with rank = "no_rank"

    def close(self):
        self.db_conn.close()

    def get_lineage(self, taxid):
        """extract lineage.
        
        Note:
            see get_xml.py for full description.
        """
        lineage = {}
        depth = 0
        while True:
            node = self._get_node(taxid)
            if node == None:
                return None
            (parent_id, rank, name) = node
            if not rank in self.SKIP_NODES:
                lineage[rank] = name

            taxid = parent_id

            if parent_id == self.SENTINEL_NODE:
                break
            if depth > self.MAX_DEPTH:
                # TODO: or return None for consistency with get_xml.py
                raise ValueError("Cannot extract lineage.") 
            depth += 1 
        return lineage

    def _get_node(self, taxid):
        node = None
        sql = "SELECT parent_id, rank, name FROM nodes WHERE taxid = ?"
        ans = self.db_conn.execute(sql, (taxid, ))
        row = ans.fetchone()
        if not row is None and len(row) == 3:
             node = row
        return node

    def get_taxon_id(self, gi):
        """taxonomy for a sequence id.

        Note:
            see get_xml.py for full description.
        """
        taxid = None
        sql = "SELECT taxid FROM gi_taxid WHERE gi = ?"
        ans = self.db_conn.execute(sql, (gi, ))
        row = ans.fetchone()
        if not row is None and len(row) > 0:
             taxid = row[0] 
        return taxid

    def load_cache(self):
        """kept to have consistent interface with get_xml.py
           no implementation required.
        """
        pass

    def save_cache(self):
        """kept to have consistent interface with get_xml.py
           no implementation required.
        """
        pass
