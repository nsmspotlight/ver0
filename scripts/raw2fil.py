import logging
import argparse
from pathlib import Path
from datetime import datetime

import pytz
import numpy as np
import astropy.units as U
from priwo import writehdr
from astropy.time import Time
from astropy.coordinates import SkyCoord

logging.basicConfig(level="INFO", datefmt="[%X]", format="%(message)s")
log = logging.getLogger("ver0")


def read_asciihdr(fn: str | Path) -> dict:
    hdr = {}
    with open(fn, "r") as lines:
        for line in lines:
            key, val = line.split("=")
            key = key.strip()
            val = val.strip()
            name, conv = {
                "Header file": ("fname", str),
                "Host ID": ("hostid", int),
                "Host name": ("hostname", str),
                "GTAC code": ("gtaccode", str),
                "Observer": ("observer", str),
                "GTAC title": ("gtactitle", str),
                "Source": ("source", str),
                "Source RA (Rad)": ("ra", float),
                "Source DEC (Rad)": ("dec", float),
                "Channels": ("nf", int),
                "Bandwidth (MHz)": ("bw", float),
                "Frequency Ch.1  (Hz)": ("f0", lambda x: float(x) * 1e6),
                "Channel width (Hz)": ("df", lambda x: float(x) * 1e6),
                "Sampling time  (uSec)": ("dt", lambda x: float(x) * 1e-6),
                "Antenna mask pol1": (
                    "maskX",
                    lambda x: [int(_) == 1 for _ in np.binary_repr(int(x, 0))],
                ),
                "Antennas pol1": ("antX", lambda x: list(x.split())),
                "Antenna mask pol2": (
                    "maskY",
                    lambda x: [int(_) == 1 for _ in np.binary_repr(int(x, 0))],
                ),
                "Antennas pol2": ("antY", lambda x: list(x.split())),
                "Beam mode": ("beammode", str),
                "No. of stokes": ("npol", int),
                "Num bits/sample": ("nbits", int),
                "De-Disperion DM": ("dm", lambda x: None if x == "NA" else float(x)),
                "PFB": ("pfb", bool),
                "PFB number of taps": (
                    "pfbtaps",
                    lambda x: None if x == "NA" else float(x),
                ),
                "WALSH": ("walsh", lambda x: False if x == "OFF" else True),
                "Broad-band RFI Filter": (
                    "rfifilter",
                    lambda x: False if x == "OFF" else True,
                ),
                "Date": ("istdate", str),
                "IST Time": ("isttime", str),
            }[key]
            hdr[name] = conv(val)

    hdr["istdatetime"] = datetime.strptime(
        " ".join(
            [
                hdr["istdate"],
                hdr["isttime"][:-3],
            ]
        ),
        "%d/%m/%Y %H:%M:%S.%f",
    )

    return hdr


def getmjd(DT: datetime):
    localtz = pytz.timezone("Asia/Kolkata")
    localdt = localtz.localize(DT, is_dst=None)
    utcdt = localdt.astimezone(pytz.utc)
    mjd = Time(utcdt).mjd
    return mjd


def raw2fil(fn: str | Path) -> None:
    fn = Path(fn)
    hdr = read_asciihdr(fn.with_suffix(".raw.ahdr"))
    data = np.fromfile(fn, dtype=np.uint8)

    df = hdr["df"]
    bw = hdr["bw"]
    dt = hdr["dt"]
    nf = hdr["nf"]
    npol = hdr["npol"]
    nbits = hdr["nbits"]
    fname = str(fn.name)
    source = hdr["source"]
    mjd = getmjd(hdr["istdatetime"])

    if df > 0.0:
        fh = hdr["f0"] + bw - (0.5 * df)
        data = np.fliplr(data)
        df = -df
    else:
        fh = hdr["f0"]

    coords = SkyCoord(hdr["ra"] * U.rad, hdr["dec"] * U.rad)  # type: ignore

    ra_sigproc = float(
        "".join(
            [
                str(int(coords.ra.hms.h)),  # type: ignore
                str(int(coords.ra.hms.m)),  # type: ignore
                str(float(coords.ra.hms.s)),  # type: ignore
            ]
        )
    )

    dec_sigproc = float(
        "".join(
            [
                str(int(coords.dec.dms.d)),  # type: ignore
                str(int(coords.dec.dms.m)),  # type: ignore
                str(float(coords.dec.dms.s)),  # type: ignore
            ]
        )
    )

    writehdr(
        {
            "rawdatafile": fname,
            "source_name": source,
            "nifs": npol,
            "nbits": nbits,
            "data_type": 1,
            "machine_id": 7,
            "telescope_id": 7,
            "barycentric": 0,
            "pulsarcentric": 0,
            "tstart": mjd,
            "foff": df,
            "fch1": fh,
            "tsamp": dt,
            "nchans": nf,
            "src_raj": ra_sigproc,
            "src_dej": dec_sigproc,
            "size": 0,
        },
        fn.with_suffix(".raw.fil"),
    )

    with open(fn.with_suffix(".raw.fil"), "ab") as f:
        data.tofile(f)


def main():
    descr = "Convert raw files to SIGPROC filterbanks for SPOTLIGHT ver0 pipeline."
    parser = argparse.ArgumentParser(prog=__file__, description=descr)
    parser.add_argument("dirpath", type=Path)
    args = parser.parse_args()

    rawfiles = list(Path(args.dirpath).glob("*.raw"))
    log.info(f"Convert {len(rawfiles)} raw files to SIGPROC filterbank...")
    for rawfile in rawfiles:
        raw2fil(rawfile)
        log.info(f"Done with file: {rawfile}.")


if __name__ == "__main__":
    main()
