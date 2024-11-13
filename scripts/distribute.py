import os
import argparse
from pathlib import Path
from natsort import natsorted


def batched(x: list, k: int):
    n = len(x)
    return [
        x[i * (n // k) + min(i, n % k) : (i + 1) * (n // k) + min(i + 1, n % k)]
        for i in range(k)
    ]


GPUIDS = [0, 1]
VER0DIR = Path(os.environ["VER0_DIR"])
NODES = (VER0DIR / "assets" / "nodes.list").read_text().split()


def pre(args):
    filfiles = natsorted(list(Path(args.dirpath).glob("*.fil")))
    batches = [batch for batch in batched(filfiles, k=len(NODES) * 2)]
    pairs = [(gpuid, node) for node in NODES for gpuid in GPUIDS]

    for (gpuid, node), batch in zip(pairs, batches):
        with open(args.outpath / f"aa.{node}.{gpuid}.txt", "w") as f:
            f.write("\n".join([str(_) for _ in batch]))


def post(args):
    beamdirs = natsorted([_ for _ in Path(args.dirpath).glob("*") if _.is_dir()])
    bcounts = [beamdir / "bcount0000" for beamdir in beamdirs]
    batches = [batch for batch in batched(bcounts, k=len(NODES) * 2)]
    pairs = [(gpuid, node) for node in NODES for gpuid in GPUIDS]

    for (gpuid, node), batch in zip(pairs, batches):
        with open(args.outpath / f"post.{node}.{gpuid}.txt", "w") as f:
            f.write("\n".join([str(_) for _ in batch]))


def main():
    parser = argparse.ArgumentParser(prog=__file__)
    subparsers = parser.add_subparsers()
    preparser = subparsers.add_parser("pre")
    postparser = subparsers.add_parser("post")

    preparser.add_argument("dirpath", type=Path)
    preparser.add_argument("outpath", type=Path)
    preparser.set_defaults(func=pre)

    postparser.add_argument("dirpath", type=Path)
    postparser.add_argument("outpath", type=Path)
    postparser.set_defaults(func=post)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
