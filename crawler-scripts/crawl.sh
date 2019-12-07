CONFIGFILE=config.yaml
NUMJOBS=1
BRAND_NEW_HOST=False
SEED=

ARGS=$(getopt -o c:j:s:NF -- $@)
eval set -- ${ARGS}
for i
do
  case $i in
    -c )
      shift
      CONFIGFILE=$1
      shift
      ;;
    -j )
      shift
      NUMJOBS=$1
      shift
      ;;
    -s )
      shift
      SEED=$1
      shift
      ;;
    -N )
      BRAND_NEW_HOST=True
      ;;
    -F )
      FORCE_ALL=--forceall
      ;;
    --)
      shift
      break
      ;;
  esac
done

snakemake --snakefile Snakefile \
          --configfile ${CONFIGFILE} \
          --config seeds_file=$SEED brand_new_host=${BRAND_NEW_HOST} \
          --jobs ${NUMJOBS} \
          --keep-going \
          ${FORCE_ALL}
