mkdir -p moses_ver
cd moses_ver

# build prob dict
cat ../GlobalVoices.en-ja.en > dictcorpus.en-ja.en
cat ../GlobalVoices.en-ja.ja > dictcorpus.en-ja.ja

/root/mosesdecoder/scripts/tokenizer/tokenizer.perl -l en < dictcorpus.en-ja.en > dictcorpus.en-ja.tok.en
mecab -Owakati < dictcorpus.en-ja.ja > dictcorpus.en-ja.tok.ja

tr '[:upper:]' '[:lower:]' < dictcorpus.en-ja.tok.en > dictcorpus.en-ja.tok.low.en
tr '[:upper:]' '[:lower:]' < dictcorpus.en-ja.tok.ja > dictcorpus.en-ja.tok.low.ja

mv dictcorpus.en-ja.tok.low.en dictcorpus.en-ja.clean.en
mv dictcorpus.en-ja.tok.low.ja dictcorpus.en-ja.clean.ja

#cp /root/mgiza/experimental/alignment-enabled/MGIZA/scripts/merge_alignment.py /root/mgiza/mgizapp/bin/
/root/mosesdecoder/scripts/training/train-model.perl --alignment grow-diag-final-and --root-dir /data/bitextor/bicleaner/moses_ver --corpus dictcorpus.en-ja.clean -e en -f ja --mgiza -mgiza-cpus=16 --parallel --first-step 1 --last-step 4 --external-bin-dir /root/mgiza/mgizapp/bin
gzip model/lex.e2f -c > dict-en.gz
gzip model/lex.f2e -c > dict-ja.gz

python /root/bicleaner/utils/dict_pruner.py model/lex.e2f dict-en.gz -n 10 -g
python /root/bicleaner/utils/dict_pruner.py model/lex.f2e dict-ja.gz -n 10 -g

# train bicleaner
cat ../GlobalVoices.en-ja.en > corpus.en-ja.en 
cat ../GlobalVoices.en-ja.ja > corpus.en-ja.ja
paste corpus.en-ja.en corpus.en-ja.ja > corpus.en-ja

python /root/bifixer/bifixer/bifixer.py --scol 1 --tcol 2 --ignore_duplicates corpus.en-ja corpus.en-ja.bifixed en ja

python /root/bicleaner/bicleaner/bicleaner_hardrules.py corpus.en-ja.bifixed corpus.en-ja.annotated -s en -t ja --scol 1 --tcol 2 --annotated_output

cat corpus.en-ja.annotated | grep "keep$" | shuf -n 100000 | cut -f1,2 > train.en-ja

python /root/bicleaner/bicleaner/bicleaner_train.py \
    train.en-ja \
    --treat_oovs --normalize_by_length \
    -s en -t ja \
    -d dict-en.gz -D dict-ja.gz \
    -b 1000 -c en-ja.classfier \
    -g 20000 -w 20000 -m en-ja.yaml \
    --classifier_type random_forest \
    --lm_training_file_sl lmtrain.en-ja.en \
    --lm_training_file_tl lmtrain.en-ja.ja \
    --lm_file_sl model.en-ja.en \
    --lm_file_tl model.en-ja.ja
