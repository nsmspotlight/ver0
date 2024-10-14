import os
import time
import logging
import argparse
from pathlib import Path
from contextlib import AbstractContextManager

import cudf
import numpy as np
import pandas as pd
from cuml.cluster import DBSCAN

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


def readaa(f: str | Path) -> pd.DataFrame:
    df = pd.DataFrame(np.fromfile(f, dtype=np.float32).reshape(-1, 4))
    df.columns = ["dm", "time", "snr", "wbin"]
    return df


def main():
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument("dirpath", type=Path)
    parser.add_argument("-m", "--minsamps", type=int)
    parser.add_argument("-e", "--epsilon", type=float)
    args = parser.parse_args()

    t0 = time.time()
    bcount = args.dirpath
    log.info(f"Clustering {str(bcount.resolve())}...")
    with chdir(bcount):
        filpath = Path("curfile.txt").read_text().strip()
        df = readaa("global_peaks.dat")
        df.to_csv("candidates.csv")

        log.info("Begin clustering...")

        dms = df["dm"].to_numpy()
        snrs = df["snr"].to_numpy()
        times = df["time"].to_numpy()
        wbins = df["wbin"].to_numpy()

        cudms = cudf.DataFrame(dms)
        cutimes = cudf.DataFrame(times)
        cudms.columns = ["dms_" + str(col) for col in cudms.columns]
        cutimes.columns = ["times_" + str(col) for col in cutimes.columns]

        cuX = cudf.concat([cudms, cutimes], axis=1)
        clusterer = DBSCAN(eps=args.epsilon, min_samples=args.minsamps)
        culabels = clusterer.fit_predict(cuX)

        labels = culabels.to_pandas()
        labels = labels.values.ravel()
        keys = np.unique(labels)

        maximas = []
        for key in keys:
            dmsi = dms[labels == key]
            snrsi = snrs[labels == key]
            timesi = times[labels == key]
            wbinsi = wbins[labels == key]

            ix = np.argmax(snrsi)

            maxdm = dmsi[ix]
            maxsnr = snrsi[ix]
            maxtime = timesi[ix]
            maxwbin = wbinsi[ix]

            maximas.append(
                {
                    "file": filpath,
                    "snr": maxsnr,
                    "stime": maxtime,
                    "width": maxwbin,
                    "dm": maxdm,
                    "label": 0,
                    "chan_mask_path": pd.NA,
                    "num_files": 1,
                }
            )
        pd.DataFrame(maximas).to_csv("filtered_candidates.csv")
    t1 = time.time()
    dt = t1 - t0
    log.info("Clustering done.")
    log.info(f"Clustering took time = {dt:.2f} s.")


if __name__ == "__main__":
    main()
