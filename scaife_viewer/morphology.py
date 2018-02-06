from . import cts


class MorphologyBuilder(cts.Visitor):

    def visit(self, passage, sort_idx):
        passage.tokenize(whitespace=False)
