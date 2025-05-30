import os
import argparse
from pathlib import Path
from natsort import natsorted

VER0DIR = Path(os.environ["VER0DIR"])


def batched(x: list, k: int):
    n = len(x)
    return [
        x[i * (n // k) + min(i, n % k) : (i + 1) * (n // k) + min(i + 1, n % k)]
        for i in range(k)
    ]


def pre(args):
    filfiles = natsorted(list(Path(args.dirpath).glob("BM*.fil")))
    batches = [batch for batch in batched(filfiles, k=len(args.nodes) * 2)]
    pairs = [(gpuid, node) for node in args.nodes for gpuid in [0, 1]]

    for (gpuid, node), batch in zip(pairs, batches):
        with open(args.outpath / f"aa.{node}.{gpuid}.txt", "w") as f:
            f.write("\n".join([str(_) for _ in batch]))
            f.write("\n")


def post(args):
    beamdirs = natsorted([_ for _ in Path(args.dirpath).glob("*") if _.is_dir()])
    batches = [batch for batch in batched(beamdirs, k=len(args.nodes) * 2)]
    pairs = [(gpuid, node) for node in args.nodes for gpuid in [0, 1]]

    for (gpuid, node), batch in zip(pairs, batches):
        with open(args.outpath / f"post.{node}.{gpuid}.txt", "w") as f:
            f.write("\n".join([str(_) for _ in batch]))
            f.write("\n")


def main():
    parser = argparse.ArgumentParser(prog=__file__)
    subparsers = parser.add_subparsers()
    preparser = subparsers.add_parser("pre")
    postparser = subparsers.add_parser("post")

    preparser.add_argument("dirpath", type=Path)
    preparser.add_argument("outpath", type=Path)
    preparser.add_argument("--nodes", nargs="+", type=str)
    preparser.set_defaults(func=pre)

    postparser.add_argument("dirpath", type=Path)
    postparser.add_argument("outpath", type=Path)
    postparser.add_argument("--nodes", nargs="+", type=str)
    postparser.set_defaults(func=post)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
