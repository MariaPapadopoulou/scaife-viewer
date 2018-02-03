from . import cts


class MorphologyBuilder(cts.Visitor):

    def visit(self, passage, sort_idx):
        passage.tokenize(whitespace=False)


def run():
    # mkdir repos
    # python manage.py load_text_repos --path=repos
    # export CTS_RESOLVER=local CTS_LOCAL_DATA_PATH=$(pwd)/repos/data
    # python manage.py shell
    import concurrent.futures
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=4)
    mb = MorphologyBuilder(executor, urn_prefix="urn:cts:greekLit:tlg0012.tlg001.perseus-grc2")
    mb.walk()
