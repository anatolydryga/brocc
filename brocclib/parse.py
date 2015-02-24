from collections import defaultdict

'''
Created on Aug 29, 2011
@authors: Serena, Kyle
'''

def iter_fasta(fasta_lines):
    """Yield sequences as (name, value) pairs from a FASTA file."""
    seq = ""
    seq_name = None
    for line in fasta_lines:
        line = line.strip()
        if line.startswith(">"):
            if seq_name is not None:
                yield (seq_name, seq)
            seq_name = line[1:]
            seq = ""
        else:
            seq += line
    yield (seq_name, seq)


class BlastHit(object):
    def __init__(self, gi, pct_id, length):
        self.gi = gi
        self.pct_id = pct_id
        self.length = length

    def coverage(self, query_seq):
        return self.length / len(query_seq)


def iter_blast(blast_lines):
    full_query_id = None
    for line in blast_lines:
        if line.startswith('# Query:'):
            full_query_id = line[8:].strip()
        if not line.startswith("#"):
            vals = [x.strip() for x in line.split('\t')]
            # If this is a commented BLAST file, we'd like to use the
            # complete query ID as a convenience.  If not available,
            # we use the first word in the query ID, which is found
            # in the first column of each output row.
            if full_query_id is None:
                query_id = vals[0]
            else:
                query_id = full_query_id
            # Need to extract the GI number from the NCBI formatted
            # reference ID.
            gi_num = parse_gi_number(vals[1])
            pct_id = float(vals[2])
            length = float(vals[3])
            hit = BlastHit(gi_num, pct_id, length)
            yield query_id, hit


def read_blast(blast_lines):
    """Read a BLAST output file, return a dict() of hits."""
    res = defaultdict(list)
    for query_id, hit in iter_blast(blast_lines):
        res[query_id].append(hit)
    return res


def parse_gi_number(id_string):
        """Recover a GI number from a formatted id string in the nt database."""
        tokens = id_string.split('|')
        for t1, t2 in zip(tokens, tokens[1:]):
            if t1 == 'gi':
                return t2
        return None

