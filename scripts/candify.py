import os
import time
import logging
import argparse
from pathlib import Path
from contextlib import AbstractContextManager

from candies.features import featurize
from candies.base import CandidateList

logging.basicConfig(level="INFO", datefmt="[%X]", format="%(message)s")
log = logging.getLogger("ver0")


class chdir(AbstractContextManager):
    def __init__(self, path):
        self.path = path
        self._old_cwd = []

    def __enter__(self):
        self._old_cwd.append(os.getcwd())
        os.chdir(self.path)

    def __exit__(self, *excinfo):
        os.chdir(self._old_cwd.pop())


def main():
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument("dirpath", type=Path)
    args = parser.parse_args()

    t0 = time.time()
    bcount = args.dirpath
    log.info(f"Extracting features from {str(bcount.resolve())}...")
    with chdir(bcount):
        candidates = CandidateList.from_csv("filtered_candidates.csv")
        groups = candidates.to_df().groupby("file")
        for fname, group in groups:
            featurize(
                save=True,
                zoom=True,
                fudging=512,
                verbose=False,
                progressbar=False,
                filterbank=str(fname),
                candidates=CandidateList.from_df(group),
            )
    t1 = time.time()
    dt = t1 - t0
    log.info("Feature extraction done.")
    log.info(f"Feature extraction took time = {dt:.2f} s.")


if __name__ == "__main__":
    main()
