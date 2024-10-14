import os
import time
import logging
import argparse
from pathlib import Path
from contextlib import AbstractContextManager

import numpy as np
import pandas as pd
from fetch.utils import get_model
from fetch.data_sequence import DataGenerator

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
    log.info(f"Classifying {str(bcount.resolve())}...")
    with chdir(bcount):
        model = get_model("a")
        h5files = list(Path(args.dirpath).glob("*.h5"))

        generator = DataGenerator(
            noise=False,
            batch_size=8,
            shuffle=False,
            list_IDs=h5files,
            labels=[0] * len(h5files),
        )

        probabilities = model.predict_generator(
            verbose=1,
            workers=4,
            generator=generator,
            steps=len(generator),
            use_multiprocessing=True,
        )

        pd.DataFrame(
            {
                "candidate": h5files,
                "probability": probabilities[:, 1],
                "labels": np.round(probabilities[:, 1] >= 0.5),
            }
        ).to_csv("classification.csv")
    t1 = time.time()
    dt = t1 - t0
    log.info("Classification done.")
    log.info(f"Classification took time = {dt:.2f} s.")


if __name__ == "__main__":
    main()
