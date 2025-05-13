# Running the bm25 baseline with Pyserini

## Step 1: Setup

The first step is to download Pyserini and the trec-product-search data.

Pyserini can be installed with ```pip install pyserini```. See this link for more information: <https://github.com/castorini/pyserini/blob/master/docs/installation.md>

You will likely find it useful to install pyserini on a fresh conda environment since instillation can be finicky. Unless you plan on using faiss, skip it in the install as it can be especially temperamental to install.

The data can be downloaded on hugging face. There are two versions of the data: 'simple' which features title+text and 'full' which contains the full metadata as well.

See the data here: <https://huggingface.co/datasets/trec-product-search/product-search-corpus/tree/main/data>

We focus on the 'simple' dataset which can be downloaded on the huggingface web interface or via the following command
```wget https://huggingface.co/datasets/trec-product-search/product-search-corpus/resolve/main/data/pyserini-simple/collection.jsonl.gz``` In either case, you will want to unzip this file into its own directory. You will later pass this directory to pyserini and it will scan for json files.

A convenient way to evaluate the code is using pytrec_eval which can be installed with ```pip install pytrec_eval```. We use this in bm25_baseline.py. The repo can be found here: <https://github.com/cvangysel/pytrec_eval>

## Step 2: Create Index

The easiest way is to use command line. For example:

```bash
python -m pyserini.index.lucene \
  --collection JsonCollection \
  --input pyserini/pyserini_simple \
  --index indexes/pyserini_simple \
  --generator DefaultLuceneDocumentGenerator \
  --threads 1 \
  --storePositions --storeDocvectors --storeRaw
```

See this link for additional details: <https://github.com/castorini/pyserini/blob/master/docs/usage-index.md#building-a-bm25-index-direct-java-implementation>

## Step 3: Run Baseline

To run the baseline you can use the following command ```python bm25_baseline.py```. You will want to edit the INDEX_DIR, QUERIES_PATH, and QRELS_PATH variables to match your system.
