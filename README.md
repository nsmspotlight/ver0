<!--Just to prettify the display in my Markdown preview window.-->
<div style="font-family: JetBrainsMono Nerd Font">
<div align="justify">

# `ver0`: [**SPOTLIGHT**][spotlight]'s `Ver0` Pipeline

This is the zeroth version of [**SPOTLIGHT**][spotlight]'s transient search pipeline. This pipeline is a temporary measure, until a real-time pipeline for the SPOTLIGHT system is developed. This is a multi-beam, multi-node, multi-GPU pipeline. Note that this pipeline is built to run on the **Param Brahmand** system at the GMRT only. To run the pipeline, follow these steps:

- Login into the **Param Brahmand** system as the `spotlight` user.
- Go to `/lustre_archive/apps/tdsoft`.
- Source the `env.sh` file.
- Enter the `ver0/` directory.
- Run `ver0.sh`.

And that's it. The pipeline will automatically distribute the jobs among the various nodes and GPUs as required. The user only needs to ensure that the filterbank files they wish to process are in the `VER0_DATA` directory, which is an environment variable set in the `env.sh` file. Currently, raw files are dumped for all beams in an observation. These are then converted into SIGPROC filterbank files using the [`raw2fil`](./scripts/raw2fil.py) script, and deposited into the `VER0_DATA` directory. Dedispersion and single pulse search is carried out using the [**AstroAccelerate**][AA] pipeline. We then cluster candidates (via [`cluster.py`](./scripts/cluster.py)), extract features from them using [**`candies`**][candies] (via [`candies.py`](./scripts/candies.py)), and classify them using [**`FETCH`**][FETCH] (via [`classify.py`](./scripts/classify.py)). All outputs are dumped into the `VER0_OUTPUTS` directory. Logs are written to `VER0_LOGS`.

</div>
</div>

[FETCH]: https://github.com/devanshkv/fetch
[spotlight]: https://spotlight.ncra.tifr.res.in
[candies]: https://github.com/astrogewgaw/candies
[AA]: https://github.com/AstroAccelerateOrg/astro-accelerate
