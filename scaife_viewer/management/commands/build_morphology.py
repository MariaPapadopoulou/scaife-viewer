from .corpus_walker import CorpusWalker
from ...morphology import MorphologyBuilder


class Command(CorpusWalker):

    help = "Build morphology"

    def walk(self, executor, **kwargs):
        builder = MorphologyBuilder(executor, **kwargs)
        builder.run()
