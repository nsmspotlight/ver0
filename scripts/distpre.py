import os
import argparse
from pathlib import Path
from natsort import natsorted


def braid(x: list):
    n = len(x)
    return [val for pair in zip(x[n // 2 :], x[: n // 2]) for val in pair]


def batched(x: list, k: int):
    n = len(x)
    return [
        x[i * (n // k) + min(i, n % k) : (i + 1) * (n // k) + min(i + 1, n % k)]
        for i in range(k)
    ]


def main():
    parser = argparse.ArgumentParser(prog=__file__)
    parser.add_argument("dirpath", type=Path)
    parser.add_argument("outpath", type=Path)
    args = parser.parse_args()

    gpuids = [0, 1]
    ver0dir = Path(os.environ["VER0_DIR"])
    nodes = (ver0dir / "assets" / "nodes.list").read_text().split()
    filfiles = natsorted(list(Path(args.dirpath).glob("*.fil")))
    batches = [batch for batch in batched(filfiles, k=len(nodes) * 2)]
    pairs = [(gpuid, node) for node in nodes for gpuid in gpuids]

    for (gpuid, node), batch in zip(pairs, batches):
        with open(args.outpath / f"aa.{node}.{gpuid}.txt", "w") as f:
            f.write("\n".join([str(_) for _ in batch]))


if __name__ == "__main__":
    main()
