import time
import logging
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from fetch.utils import get_model
from fetch.data_sequence import DataGenerator

logging.basicConfig(level="INFO", datefmt="[%X]", format="%(message)s")
log = logging.getLogger("ver0")


def main():
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument("dirpaths", nargs="*")
    args = parser.parse_args()

    t0 = time.time()
    bcounts = args.dirpaths
    log.info(f"Classifying {len(bcounts)} beams...")

    parentdir = Path(bcounts[0]).parent.resolve()
    results = parentdir / "classification.csv"

    h5files = []
    for bcount in bcounts:
        h5files.extend(list(Path(bcount).glob("*.h5")))

    model = get_model("a")

    generator = DataGenerator(
        noise=False,
        batch_size=8,
        shuffle=False,
        list_IDs=h5files,
        labels=[0] * len(h5files),
    )

    probabilities = model.predict_generator(
        verbose=1,
        workers=48,
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
    ).to_csv(
        results,
        mode="a",
        index=False,
        header=not results.exists(),
    )

    t1 = time.time()
    dt = t1 - t0
    log.info("Classification done.")
    log.info(f"Classification took time = {dt:.2f} s.")


if __name__ == "__main__":
    main()
