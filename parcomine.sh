CONFIGFILE="config.yaml"
NUMJOBS=1
INCREMENTAL="true"
HOSTFILE=""
DELETE=""

ARGS=$(getopt -o s:c:j:h:d -- "$@")
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
    -d )
      DELETE="--delete-all-output"
      ;;
    --)
      shift
      break
      ;;
  esac
done

if [ -n "$DELETE" ]; then
    snakemake --snakefile Snakefile --configfile "${CONFIGFILE}" --config hostsFile="${HOSTFILE}" incremental="${INCREMENTAL}" --delete-all-output
fi

snakemake --snakefile Snakefile --configfile "${CONFIGFILE}" -j "${NUMJOBS}" --config hostsFile="${HOSTFILE}" incremental="${INCREMENTAL}" --keep-going
