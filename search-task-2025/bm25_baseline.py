import os
import pytrec_eval
from pyserini.search.lucene import LuceneSearcher

INDEX_DIR = '/path/to/pyserini/index/'
QUERIES_PATH = '/path/to/test/queries/2024-test-queries.tsv'
QRELS_PATH = '/path/to/qrels/2024test.qrel'
NUM_HITS = 100
CUTOFFS = [10, 100]

measures_to_calculate = {
    s for cutoff in CUTOFFS for s in [f'ndcg_cut_{cutoff}', f'recall_{cutoff}']
}
measures_to_calculate.update(['infAP'])
TARGET_METRIC = f'ndcg_cut_{CUTOFFS[0]}'

# 0. Check if files exist 
if not os.path.exists(INDEX_DIR):
    raise FileNotFoundError(f"Index directory not found: {INDEX_DIR}")
if not os.path.exists(QUERIES_PATH):
    raise FileNotFoundError(f"Queries file not found: {QUERIES_PATH}")
if not os.path.exists(QRELS_PATH):
    raise FileNotFoundError(f"Qrels file not found: {QRELS_PATH}")

# 1. Load Queries
print(f"Loading queries from {QUERIES_PATH}...")
queries = {}
with open(QUERIES_PATH, 'r') as f:
    for line in f:
        line = line.strip()
        if line:
            query_id, query_text = line.split('\t', 1)
            queries[query_id] = query_text
print(f"Loaded {len(queries)} queries.")

# 2. Load Qrels
print(f"Loading qrels from {QRELS_PATH}...")
with open(QRELS_PATH, 'r') as f_qrels:
    qrels = pytrec_eval.parse_qrel(f_qrels)
print(f"Loaded qrels for {len(qrels)} queries.")

# 3. Initialize Searcher 
print(f"Initializing searcher for index: {INDEX_DIR}...")
searcher = LuceneSearcher(INDEX_DIR)
print("Searcher initialized.")


# 4. Perform Search
run_results = {}
for query_id, query_text in queries.items():
    hits = searcher.search(query_text, k=NUM_HITS)
    run_results[query_id] = {}
    for hit in hits:
        run_results[query_id][hit.docid] = float(hit.score)

    evaluator = pytrec_eval.RelevanceEvaluator(qrels, measures_to_calculate)
    results = evaluator.evaluate(run_results)

# 5. Aggregate and Store Results
aggregated_results = {}
for measure in sorted(measures_to_calculate):
    query_scores = [query_measures[measure] for query_measures in results.values()]
    if query_scores:
            aggregated_results[measure] = sum(query_scores) / len(query_scores)
    else:
            aggregated_results[measure] = 0.0


# 6. Report Results
print("-------- Results --------")
for measure, score in sorted(aggregated_results.items()):
    print(f"  {measure:<15}: {score:.4f}")
