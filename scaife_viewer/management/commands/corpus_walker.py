import concurrent.futures
import time
from decimal import Decimal

from django.core.management.base import BaseCommand


class CorpusWalker(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("--max-workers", type=int, default=None)
        parser.add_argument("--dry-run", action="store_true", default=False)
        parser.add_argument("--urn-prefix")
        parser.add_argument("--chunk-size", type=int, default=100)
        parser.add_argument("--limit", type=int, default=None)

    def handle(self, *args, **options):
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=options["max_workers"])
        kwargs = {
            "urn_prefix": options["urn_prefix"],
            "chunk_size": options["chunk_size"],
            "limit": options["limit"],
            "dry_run": options["dry_run"],
        }
        with Timer() as timer:
            self.walk(executor, **kwargs)
        elapsed = timer.elapsed.quantize(Decimal("0.00"))
        print(f"Finished in {elapsed}s")

    def walk(self, executor, **kwargs):
        raise NotImplementedError()


class Timer:

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, typ, value, traceback):
        self.elapsed = Decimal.from_float(time.perf_counter() - self.start)
