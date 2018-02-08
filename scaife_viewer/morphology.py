import io
import json
from itertools import chain

from . import cts


class MorphologyBuilder(cts.Visitor):

    def visit(self, passage, sort_idx):
        lemmas = []
        for token in passage.tokenize(whitespace=False):
            lemmas.append({"w": token["w"], "l": None})
        return lemmas

    def process_results(self, results):
        self.lemma_file = io.StringIO()
        for lemma in chain.from_iterable(results):
            self.lemma_file.write(json.dumps(lemma) + "\n")
