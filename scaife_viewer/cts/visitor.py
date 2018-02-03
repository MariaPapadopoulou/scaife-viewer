from collections import deque
from itertools import chain, islice, zip_longest
from operator import attrgetter
from typing import Iterable

from anytree.iterators import PreOrderIter

from . import api as cts


class Visitor:

    def __init__(self, executor, urn_prefix=None, chunk_size=100, limit=None, dry_run=False):
        self.executor = executor
        self.urn_prefix = urn_prefix
        self.chunk_size = chunk_size
        self.limit = limit
        self.dry_run = dry_run

    def walk(self):
        cts.TextInventory.load()
        print("Text inventory loaded")
        if self.urn_prefix:
            print(f"Applying URN prefix filter: {self.urn_prefix}")
        with self.executor as executor:
            passages = chain.from_iterable(
                executor.map(
                    self.passages_from_text,
                    self.texts(),
                    chunksize=100,
                )
            )
            if self.limit is not None:
                passages = islice(passages, self.limit)
            passages = list(passages)
            print(f"Walking {len(passages)} passages")
            consume(executor.map(self.walker, chunker(passages, self.chunk_size), chunksize=10))

    def texts(self):
        ti = cts.text_inventory()
        for text_group in ti.text_groups():
            for work in text_group.works():
                for text in work.texts():
                    if self.urn_prefix and not str(text.urn).startswith(self.urn_prefix):
                        continue
                    yield text

    def passages_from_text(self, text):
        passages = []
        try:
            toc = text.toc()
        except Exception as e:
            print(f"{text.urn} toc error: {e}")
        else:
            leaves = PreOrderIter(toc.root, filter_=attrgetter("is_leaf"))
            for i, node in enumerate(leaves):
                passages.append({
                    "urn": f"{text.urn}:{node.reference}",
                    "sort_idx": i,
                })
        return passages

    def walker(self, chunk: Iterable[str]):
        for p in chunk:
            urn = p["urn"]
            try:
                passage = cts.passage(urn)
            except cts.PassageDoesNotExist:
                print(f"Passage {urn} does not exist")
                continue
            except Exception as e:
                print(f"Error {e}")
                continue
            self.visit(passage, p["sort_idx"])

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["executor"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.executor = None


def consume(it):
    deque(it, maxlen=0)


def chunker(iterable, n):
    args = [iter(iterable)] * n
    for chunk in zip_longest(*args, fillvalue=None):
        yield [item for item in chunk if item is not None]
