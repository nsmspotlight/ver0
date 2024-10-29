#!/usr/bin/env bash

#################################################################################
# ver0.sh: A shell script that runs SPOTLIGHT's Ver0 transient search pipeline. #
#################################################################################

aasinglefile() {
  NODE="${3}"
  GPUID="${4}"
  JOBNAME="${2}"
  FILEPATH="${1}"
  BEAMNAME="$(basename "${1}" ".fil")"

  export AA="${TDSOFT}/aa.frb"
  export AA_EXE="${AA}/build"
  export AA_CURFILE="${FILEPATH}"
  export AA_OUTPUTS="${VER0_OUTPUTS}"
  export AA_CONFIG="${AA}/input_files/gmrt.${NODE}.${GPUID}.txt"
  export AA_JOB_DIR="${AA_OUTPUTS}/${JOBNAME}/${BEAMNAME}/bcount0000"

  mkdir -p "${AA_JOB_DIR}"
  cd "${AA_JOB_DIR}" || exit
  echo "${AA_CURFILE}" >"curfile.txt"

  rm -f analysed*
  rm -f acc*
  rm -f global*
  rm -f fourier*
  rm -f harmonic*
  rm -f candidate*
  rm -f peak*

  read -r -d '' __CONFIG <<EOM
range    0    150  0.1  1 1
range    150  300  0.1  1 1
range    300  500  0.1  1 1
range    500  900  0.2  1 1
range    900  1200 0.4  1 1
range    1200 1500 0.8  1 1
range    1500 2000 1.0  1 1

selected_card_id ${GPUID}

sigma_cutoff    6
sigma_constant  3.0
max_boxcar_width_in_sec 0.5

zero_dm
analysis
baselinenoise
set_bandpass_average
output_DDTR_normalization

file ${AA_CURFILE}
EOM

  echo "${__CONFIG}" >"${AA_CONFIG}"

  time "${AA_EXE}/astro-accelerate" "${AA_CONFIG}"

  if ls analysed* 1>/dev/null 2>&1; then cat analysed* >global_analysed_frb.dat; fi
  if ls fourier-* 1>/dev/null 2>&1; then cat fourier-* >global_periods.dat; fi
  if ls fourier_inter* 1>/dev/null 2>&1; then cat fourier_inter* >global_interbin.dat; fi
  if ls harmo* 1>/dev/null 2>&1; then cat harmo* >global_harmonics.dat; fi
  if ls candidate* 1>/dev/null 2>&1; then cat candidate* >global_candidates.dat; fi
  if ls peak* 1>/dev/null 2>&1; then cat peak* >global_peaks.dat; fi

  echo "Finished. Output is located in ${AA_JOB_DIR}."
}

postsinglefile() {
  DIRPATH="${1}"
  which python
  python "${VER0_DIR}/scripts/cluster.py" "${DIRPATH}"
  python "${VER0_DIR}/scripts/candify.py" "${DIRPATH}"
  python "${VER0_DIR}/scripts/classify.py" "${DIRPATH}"
}

aamultifile() {
  NODE="${3}"
  GPUID="${4}"
  JOBNAME="${2}"
  FILELIST="${1}"
  while read -r FILEPATH; do
    aasinglefile "${FILEPATH}" "${JOBNAME}" "${NODE}" "${GPUID}"
  done <"${FILELIST}"
}

postmultifile() {
  DIRLIST="${1}"
  while read -r DIRPATH; do
    postsinglefile "${DIRPATH}"
  done <"${DIRLIST}"
}

aamultinode() {
  ifstop
  while read -r NODE; do
    for GPUID in 0 1; do
      ssh -n "${NODE}" "
      $(typeset -f)
      source ${TDSOFT}/env.sh;
      python ${VER0_DIR}/scripts/distpre.py ${VER0_DATA} ${VER0_DISTS};
      aamultifile ${VER0_DISTS}/aa.${NODE}.${GPUID}.txt ${AA_JOB_NAME} ${NODE} ${GPUID};
      " >"${VER0_LOGS}/VER0_${NODE}.txt" 2>&1 &
    done
  done <"${VER0_DIR}/assets/nodes.list"
  wait
}

postmultinode() {
  ifstop
  JOBDIR="${VER0_OUTPUTS}/${AA_JOB_NAME}"
  while read -r NODE; do
    for GPUID in 0 1; do
      ssh -n "${NODE}" "
      $(typeset -f)
      source ${TDSOFT}/env.sh;
      python ${VER0_DIR}/scripts/distpost.py ${JOBDIR} ${VER0_DISTS};
      CUDA_VISIBLE_DEVICES=${GPUID} postmultifile ${VER0_DISTS}/post.${NODE}.${GPUID}.txt;
      " >>"${VER0_LOGS}/VER0_${NODE}.txt" 2>&1 &
    done
  done <"${VER0_DIR}/assets/nodes.list"
  wait
}

main() {
  TIMESTAMP="$(date '+%d%h%Y_%Hh%Mm%Ss')"
  export AA_JOB_NAME="AA_${TIMESTAMP}"
  aamultinode
  postmultinode
}

ifstop() { trap 'pkill -P $$; exit' SIGINT SIGTERM; }

ifstop
main
