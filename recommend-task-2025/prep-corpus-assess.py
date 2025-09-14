"""
Prepare assessment-ready corpus entries.

Usage:
    prep-corpus-assess.py [-v] [CORPUS]

Options:
    -v, --verbose
        Turn on verbose logging.
    CORPUS
        The exported corpus file [default: product-corpus.json.zst]
"""

# pyright: basic
import logging
import sys
import time
from csv import DictReader
from pathlib import Path
from typing import TextIO

from docopt import docopt
from pydantic import BaseModel, JsonValue
from tqdm import tqdm
from xopen import xopen

log = logging.getLogger("trec-product.prep-corpus-assess")


OUT_DIR = Path("assessment-corpus")
CATS = ["Electronics", "Home_and_Kitchen", "Sports_and_Outdoors"]
NOW = time.time()


class CorpusItem(BaseModel):
    id: str
    title: str
    desc: str | None = None
    brand: str | None = None
    product_bullet_point: str | None = None


class ProductImage(BaseModel):
    thumb: str | None = None
    large: str | None = None
    variant: str | None = None
    hi_res: str | None = None


class ProductMeta(BaseModel):
    parent_asin: str
    title: str
    features: list[str] = []
    description: list[str] = []
    images: list[ProductImage] = []
    details: dict[str, JsonValue] = {}


def main(options):
    level = logging.DEBUG if options["--verbose"] else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)

    corpus = load_corpus()
    meta = load_products(corpus)

    write_queries(corpus, meta)
    write_items(corpus, meta)


def load_corpus() -> dict[str, CorpusItem]:
    log.info("loading product corpus")
    corpus = {}
    with xopen("product-corpus.json.zst", "rt") as jsonl:
        for line in jsonl:
            item = CorpusItem.model_validate_json(line)
            corpus[item.id] = line

    log.info("loaded %d items from corpus", len(corpus))
    return corpus


def load_products(corpus: dict[str, CorpusItem]) -> dict[str, ProductMeta]:
    ucsd_dir = Path("ucsd-2023")
    products = {}
    for cat in CATS:
        cpath = ucsd_dir / f"meta_{cat}.jsonl.zst"
        log.info("loading metadata from %s", cpath.name)
        with xopen(cpath, "rt") as mf:
            for line in mf:
                prod = ProductMeta.model_validate_json(line)
                if prod.parent_asin in corpus:
                    products[prod.parent_asin] = prod

    return products


def write_queries(corpus: dict[str, CorpusItem], products: dict[str, ProductMeta]):
    q_dir = OUT_DIR / "queries"
    q_dir.mkdir(exist_ok=True, parents=True)
    log.info("reading queries")
    with xopen("rec-queries.tsv", "rt") as xqf:
        queries = [
            (line["Query_id"], line["Query ASIN"])
            for line in DictReader(xqf, delimiter="\t")
        ]

    log.info("writing %d query pages", len(queries))
    for qid, qprod in tqdm(queries, desc="queries"):
        meta = products[qprod]
        with open(q_dir / f"{qid}.md", "wt") as qf:
            print(f"# Query {qid}", file=qf)
            print(file=qf)
            write_product(meta, qf)


def write_items(corpus: dict[str, CorpusItem], products: dict[str, ProductMeta]):
    i_dir = OUT_DIR / "items"
    i_dir.mkdir(exist_ok=True, parents=True)

    log.info("writing %d products", len(products))
    for pid, prod in tqdm(products.items(), desc="products"):
        with open(i_dir / f"{pid}.md", "wt") as pf:
            print("# Product {pid}\n", file=pf)
            write_product(prod, pf)


def write_product(product: ProductMeta, file: TextIO):
    print(f"**Product Title:** {product.title}\n", file=file)
    for img in product.images:
        if img.variant == "MAIN" and img.thumb:
            print(f"![Product Image]({img.thumb})\n", file=file)
            break
    print("## Description\n", file=file)
    for dl in product.description:
        print(f"{dl}\n", file=file)
    print("## Features\n", file=file)
    for f in product.features:
        print(f"-   {f}", file=file)

    print("## Details\n", file=file)
    for dname, dval in product.details.items():
        if isinstance(dval, str):
            print(dname, file=file)
            print(f":   {dval}\n", file=file)


if __name__ == "__main__":
    opts = docopt(__doc__ or "")
    main(opts)
