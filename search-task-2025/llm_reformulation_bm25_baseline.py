import os
import re
import pytrec_eval
from pyserini.search.lucene import LuceneSearcher

from tqdm import tqdm
import torch
import transformers
from transformers import AutoTokenizer, AutoModelForCausalLM

def get_llama():
    model_id = "meta-llama/Meta-Llama-3-8B-Instruct"

    pipeline = transformers.pipeline(
        "text-generation",
        model=model_id,
        model_kwargs={"torch_dtype": torch.bfloat16},
        device_map='auto',
    )

    terminators = [
        pipeline.tokenizer.eos_token_id,
        pipeline.tokenizer.convert_tokens_to_ids("<|eot_id|>")
    ]
    return pipeline, terminators

def llama_expansion(pipeline, terminators, query: str):
    def extract_query_from_llm_output(text: str) -> str:
        match = re.search(r'expanded_query:\s*(.*)', text, re.IGNORECASE)

        if match:
            query = match.group(1).strip()
            return query
        else:
            return ""

    messages = [
    {"role": "system", "content": 
        """Following is a product search query. Please expand this query to improve bm25 retrieval performance. Do this by anticipating similar product names or keywords that could be relevant to the product search query. Indicate the start of the query with expanded_query: 
        example: query 'peplum top' expanded_query:  Women's Vintage Stretchy Shoulder Pad Peplum Printed Blouse Top Women's V Neck Long Sleeve Ribbed Pullover Knitted Peplum Top"""
    },
    {"role": "user", "content": f"query {query}"},
    ]
    with torch.no_grad():
        outputs = pipeline(
            messages,
            max_new_tokens=64,
            eos_token_id=terminators,
            do_sample=True,
            temperature=0.25,
            top_p=0.9,
            pad_token_id=pipeline.tokenizer.eos_token_id,
        )

    return extract_query_from_llm_output(outputs[0]["generated_text"][-1]['content'])

INDEX_DIR = '/path/to/indexes/pyserini_simple/'
QUERIES_PATH = '/path/to/trec/collection/queries/2024-test-queries.tsv'
QRELS_PATH = '/path/to/trec/collection/queries/2024test.qrel'
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

# 1.5 Reformulate Queries
reformulated_queries = {}

# Load your model here
pipeline, terminators = get_llama()
for query_id, query in tqdm(queries.items()):
    # Replace the following line with a function defining your expansion. 
    # Minimally, this should be a function that takes a string and outputs a string. 
    # A simple llama based expansion implementation is provided for reference
    expansion = llama_expansion(pipeline, terminators, query)
    expanded_query = f"{query} {expansion}"
    reformulated_queries[query_id] = expanded_query

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
for query_id, query_text in reformulated_queries.items():
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
