import io

from ...cloud import CloudJob
from ...morphology import MorphologyBuilder
from .corpus_walker import CorpusWalker


class Command(CloudJob, CorpusWalker):

    help = "Build morphology"

    def walk(self, executor, **kwargs):
        builder = MorphologyBuilder(executor, **kwargs)
        builder.run()
        self.artifacts["test"] = {
            "data": io.StringIO("hello world"),
        }
