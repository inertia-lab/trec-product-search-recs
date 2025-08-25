# Search : Query Reformulation Task Instructions

Many e-commerce search systems struggle with task-oriented queries, which express broader user intents related to activities or goals rather than specific products. Examples include "home office equipment," "birthday party supplies," or "make lasagna." These queries constitute a substantial portion of e-commerce search traffic but remain difficult to address with conventional retrieval techniques.

At the heart of information retrieval lies the ideal query hypothesis, which states that for any information need, there exists an ideal keyword query that would ensure a search engine returns relevant documents at the top of results. However, users rarely possess sufficient knowledge about document content to formulate such ideal queries, creating a vocabulary gap. This disparity is particularly pronounced in task-oriented product search, where users express high-level goals rather than specific product attributes.

Traditional lexical search approaches like BM25 perform poorly on these queries because:
1. They rely heavily on term matching between query terms and product metadata
2. They fail to capture the implicit product requirements embedded in task descriptions
3. They lack the domain knowledge required to translate high-level goals into specific product needs

Despite these limitations, lexical matching approaches like BM25 offer significant computational efficiency advantages over neural search and deep learning approaches. 

The goal of this track is to develop approaches that effectively reformulate queries to bridge this semantic gap, making them more effective at retrieving relevant products when processed through standard retrieval systems.

## Task Variations
In this track, we are focusing on two variations of the query reformulation task:

1. **Automatic Reformulation**: Participants submit exactly one reformulated query for each original query.

2. **Interactive Reformulation**: Participants submit up to 4 different reformulation versions of each original query, simulating an interface where users can select the most relevant version. Performance is determined by the best-performing reformulated query among the submissions.

The goal of both variations is to successfully identify reformulated queries that improve product retrieval effectiveness for task-oriented queries.

## Training and Preparatory Data
We are providing the following data to track participants, avaliable <a href="https://huggingface.co/trec-product-search"> on HuggingFace</a>:

1. **Product Collection**:
   - A comprehensive dataset of product metadata from the e-commerce domain
   - Each product includes: ID, title, description, attributes (price, brand, color, size, etc.), and category information
   - This is available via the TREC 2024 dataset

2. **Search API**:
   - A standard BM25-based retrieval system that participants can use for testing their query reformulations
   - API access will be limited to a reasonable number of calls per day

3. **Training and Development Data**:
   - A set of task-oriented queries with original query text
   - Human-annotated reformulated versions of the query
   - Relevance judgments for products in relation to the query
   - A set of difficult queries from TREC 2024 test set for which BM25 had very low scores (<a href="https://huggingface.co/datasets/trec-product-search/product-search-2025-test-queries">link</a>)
   - TREC 2024 training dataset

## Task Definition and Query Data
The "query" data will consist of a set of previously unseen task-oriented queries representing diverse domains and complexity levels. These queries will be designed to have clear implicit product requirements not explicitly stated in the query.

For each test query, participants must submit:

1. For the **Automatic Reformulation** track:
   - The original query
   - A single reformulated query designed to improve product retrieval

2. For the **Interactive Reformulation** track:
   - The original query
   - Up to 4 different reformulated queries representing different interpretations or aspects of the task

Reformulated queries can include:
- Additional terms that make implicit requirements explicit
- More specific product categories or attributes
- Related task requirements not explicitly mentioned in the original query

## Submission Format

Output data should be a TSV file in a 6-column TREC runs format with reformulated query replacing the usual iter column. In other words, the output should be a 6-column TSV file with the following columns: qid, reformulated_query, product_id, rank, score, runID.

## Annotation and Relevance
Products retrieved based on the submitted reformulated queries will be pooled and assessed by human evaluators. Each product will be assigned a relevance score using the following criteria:

- **Essential (3)**: The product is absolutely necessary to complete the task described in the original query
- **Highly Relevant (2)**: The product is very useful for the task but might be substituted or is not strictly required
- **Somewhat Relevant (1)**: The product has some utility for the task but is secondary rather than necessary
- **Not Relevant (0)**: The product has no clear utility for the specific task described

## Evaluation Metrics
The primary evaluation metrics will be:

1. **Task Completion NDCG**: Normalized Discounted Cumulative Gain calculated with an emphasis on retrieving all essential products for task completion

2. **Essential Product Recall@K**: The proportion of essential products that appear in the top K results

3. **Product Coverage Score**: A measure of how well the retrieved products cover the different aspects or requirements of the task

4. **Average Precision**: The precision averaged over different recall points

5. **Diversity Metrics**: Measures how well the retrieved results cover different product categories needed for the task

For the Interactive Reformulation track, the best-performing reformulated query (according to the Task Completion NDCG) will be used for the final evaluation.

## Timeline
- Task Data Release: May 27, 2025
- Development Period: Summer 2025
- Test Query Release: Late August 2025
- Submission Deadline: Early September 2025

## Repos
For code see our github repo here: <a href="https://github.com/inertia-lab/trec-product-search-recs/tree/main/search-task-2025">Github Repo</a>

For datasets see our Huggingface repo here: <a href="https://huggingface.co/trec-product-search">Huggingface Repo</a>
