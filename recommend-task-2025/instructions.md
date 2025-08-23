# Recommendation Task Instructions

Many e-commerce sites provide various forms of *related product*
recommendations: products that are somehow related to a product the user is
looking at, has added to their cart, or has purchased or viewed in the past.
However, published research on related-product recommendation is scant, and very
few datasets are well-suited to evaluate such recommendations.

Product relationships also come in different types, further complicating the
evaluation but opening fresh possibilities for research. This track focuses on
*substitute* and *complementary* relationships, defined in terms of product
function and use. Specifically, given a reference item *A* and a related item
*B*:

* **Substitute** products are products that fill the same function or role as
  each other, such that *B* could be substituted for *A* (possibly with some
  restrictions). For example, two cell phone cases are substitutes for each
  other.
* **Complementary** products are products that perform related functions: *B*
  can be used with *A* to add, power, or enhance functionality. For example, a
  cell phone and a cell phone case are complementary products.

In the first version of this task, we are focusing on course-grained tasks. For
example, two camera lenses for the same camera are substitutes for each other,
even if they serve different photographic purposes (e.g., a portrait lens and a
zoom lens for the same camera platform are considered substitutes for this
version of this task).

The goal of this track is to successfully **identify** and **distinguish**
complementary and substitute products, to enable recommendation experiences that
distinguish between these relationship types and enable richer user exploration
of the product space.

Because related-product recommendation lists are often short, we will be using
short rankings of products (with separate lists of substitutes and complements)
as the primary basis for evaluating submitted runs. Teams will also submit a
third, longer list for each query for pooling.

## Training and Preparatory Data

[repo]: https://huggingface.co/datasets/trec-product-search/product-recommendation-2025/
[README]: https://huggingface.co/datasets/trec-product-search/product-recommendation-2025/blob/main/eval/README.md

We have provided the following data to track participants, available [on
HuggingFace][repo]:

* A product corpus curated from [Amazon M2][M2] and [Amazon ESCI][ESCI],
  filtered to only include items also available in the Mcauley Lab's Amazon
  reviews data.
* Training / validation queries and qrels for the Substitute and Complementary
  subtasks, synthesized from Amazon ESCI (see [README][] for details).

For your final submissions, use the **eval** directory.

All data is recorded with ASINs, so your model can be trained by cross-linking it with other public datasets:

* [Amazon M2][M2] (user purchase sessions)
* [Amazon ESCI][ESCI] (annotated search results)
* [Amazon reviews and product data][UCSD]

You are **not** limited to the product data in the corpus — feel free to enrich
with other sources, such as other data available in the original ESCI or M2 data
sets, or the UCSD Ratings & Reviews.

Our repository also contains copies of the relevant pieces of the original M2
and ESCI data sets, pursuant to their Apache licenses. The search corpus is
formed from combining the M2 and ESCI product training data sets, and filtering
as follows:

* All items must also appear on the UCSD review data set (for more detailed
  descriptions for the assessors).
* All items must be in the US locale.
* All items must have at least 50-character descriptions.
* Only items in the *Electronics*, *Home and Garden* and *Sports and Outdoors*
  categories.

[ESCI]: https://amazonkddcup.github.io/
[M2]: https://kddcup23.github.io/
[UCSD]: https://amazon-reviews-2023.github.io/

## Task Definition and Query Data

The “query” data will consist of a set of requests for related product
recommendations. Each request contains a single Amazon product ID (the
*reference item*). For each reference item, the system should produce (and teams
submit) **three** output lists:

1. A ranked list of 100 related items, with an annotation as to whether they are
   complementary or substitute. This will be used to generate deeper pools for
   evaluation.
2. A list of 10 **Top Complementary** items.
3. A list of 10 **Top Substitute** items.

Participant solutions are not restricted to the training data we provide — it is
acceptable to enrich the track data with additional data sources such as the
Amazon Review datasets for training or model operation.

### Query Format

The query data will be in a CSV file with 3 columns: query ID, product ID
(ASIN), and the product title.

### Run Format

Submitted runs should be in the 6-column TREC runs format (TSV with columns
`qid`, `iter`, `product`, `rank`, `score`, `runID`), with QIDs derived from the
input QIDs.  Specifically, for input query 3, the outputs should be recorded as
qids 3R, 3C, and 3S.

For the **100-item Pooled runs**, since standard TREC analysis ignores the
Iteration field (field 2), use it to label items: emit C for complement and S
for substitute.

## Annotation and Relevance

Recommended items from submitted runs will be pooled and assessed by NIST assessors. Each item will be labeled with one of 4 categories (2 of have graded labels):

* **Substitute** — the recommended product is a substitute for the reference product. This has three grades:
  1. **Full Substitute** — the recommended product is a full or nearly-full functional substitute for the reference product. By “nearly-full” we mean that there may be differences in features, but one product will generally work in place of the other. For example, two cameras that work with the same lens system are full substitutes for each other (even if they have different feature sets, for this first version of the task). The core idea is that the user would consider these items to be alternatives to each other, with the same broad compatibility.
  2. **Conceptual Substitute** — the recommended product serves the same core function as the reference product, but may not be compatible. For example, two cameras using different lens systems are conceptual substitutes, because they fill the same function but are not compatible with the same products.
  3. **Related but Not Substitute** — the products are meaningfully related but neither is a functional substitute for the other, nor are they complementary.
* **Complement** — the recommended product is complementary to the reference product.
  1. **Full Complement** — the products have complementary functions and appear to be likely able to work together.
  2. **Complementary but Unknown Compatibility** — the products are complementary but there is insufficient information to assess whether they are fully compatible.
  3. **Complementary but Incompatible** — the products are conceptually complementary (products of their type would complement each other), but are not directly compatible. For example, a Samsung phone and an iPhone case. This is a lower grade than Unknown Compatibility under the idea that it is worse to recommend a product that is known to be incompatible than a product where the compatibility is unknown.
* **Not Related** — the recommended product is not related to the reference product.
* **Unable to Assess** — there is insufficient information or assessor experience to assess product relatedness.

## Evaluation Metrics

The primary evaluation metric will be **NDCG** computed separately for each
top-substitute and top-complement recommendation list. This will be aggregated
in the following ways to produce submission-level metrics:

* Separate **Complement NDCG** and **Substitute NDCG**, using the relevance
  grades above (1, 2, and 3\) as the gain.
* **Average NDCG**, averaging the NDCG across all runs. This is the top-line
  metric for ordering systems in the final report.

We will compute supplementary metrics including:

* **Pool NDCG** of the longer related-product run, where the gain for an
  incorrectly-classified item is 50% of the gain it would have if it were
  correctly classified.
* Agreement of annotations in the long (pooling) run.
* **Diversity** of the substitute and complementary product lists, computed over
  fine-grained product category data from the 2023 Amazon Reviews data set.

## Timeline

* Task Data Release: **Now available**
* Development Period: Summer 2025
* Test Query Release: **Aug. 25, 2025**
* Submission Deadline: **Sep. 4, 2025**
