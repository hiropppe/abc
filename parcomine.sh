CONFIGFILE="config.yaml"
NUMJOBS=1
ONLY_NEW_HOSTS="False"
HOSTFILE=""
DELETE=""

ARGS=$(getopt -o s:c:j:h:F -- "$@")
eval set -- "${ARGS}"
for i
do
  case "$i" in
    -s )
      shift
      SNAKEFILE="$1"
      shift
      ;;
    -c )
      shift
      CONFIGFILE="$1"
      shift
      ;;
    -j )
      shift
      NUMJOBS="$1"
      shift
      ;;
    -h )
      shift
      HOSTFILE="$1"
      shift
      ;;
    -F )
      FORCE_ALL="-F"
      ;;
    --)
      shift
      break
      ;;
  esac
done

snakemake --snakefile Snakefile --configfile "${CONFIGFILE}" -j "${NUMJOBS}" --config hostsFile="${HOSTFILE}" --config onlyNewHosts="${ONLY_NEW_HOSTS}" --keep-going ${FORCE_ALL}
