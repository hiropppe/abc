mkdir -p mgiza_ver
cd mgiza_ver

PARCO=/root/parcomine
MOSES=/root/mosesdecoder
MGIZA=/root/mgiza
MKCLS=$MGIZA/mgizapp/bin/mkcls
BICLEANER=/root/bicleaner

cat ../GlobalVoices.en-ja.en | sed "s/&apos;/'/g" | sed 's/&quot;/\"/g' | sed 's/&amp;/\&/g' | $MOSES/scripts/tokenizer/tokenizer.perl -q -b -a -l en > corpus.tok.en
cat ../GlobalVoices.en-ja.ja | sed "s/&apos;/'/g" | sed 's/&quot;/\"/g' | sed 's/&amp;/\&/g' | mecab -Owakati > corpus.tok.ja

cat corpus.tok.en | $MOSES/scripts/tokenizer/lowercase.perl > corpus.tok.low.en
cat corpus.tok.ja | $MOSES/scripts/tokenizer/lowercase.perl > corpus.tok.low.ja

perl $PARCO/utils/clean-corpus-n.perl corpus.tok.low en ja corpus.clean 1 80 corpus.lines-retained

mkdir -p mgiza
$MGIZA/mgizapp/bin/plain2snt corpus.clean.en corpus.clean.ja
mv corpus.clean.en_corpus.clean.ja.snt mgiza/corpus.ja-en-int-train.snt
mv corpus.clean.ja_corpus.clean.en.snt mgiza/corpus.en-ja-int-train.snt
cp corpus.clean.en.vcb mgiza/corpus.en.vcb
cp corpus.clean.ja.vcb mgiza/corpus.ja.vcb

$MGIZA/mgizapp/bin/mkcls -c50 -n2 -p./corpus.clean.en -Vmgiza/corpus.en.vcb.classes opt
$MGIZA/mgizapp/bin/mkcls -c50 -n2 -p./corpus.clean.ja -Vmgiza/corpus.ja.vcb.classes opt

$MGIZA/mgizapp/bin/snt2cooc mgiza/corpus.ja-en.cooc mgiza/corpus.en.vcb mgiza/corpus.ja.vcb mgiza/corpus.ja-en-int-train.snt
$MGIZA/mgizapp/bin/snt2cooc mgiza/corpus.en-ja.cooc mgiza/corpus.ja.vcb mgiza/corpus.en.vcb mgiza/corpus.en-ja-int-train.snt

$MGIZA/mgizapp/bin/mgiza -ncpus 16 -CoocurrenceFile mgiza/corpus.ja-en.cooc -c mgiza/corpus.ja-en-int-train.snt -m1 5 -m2 0 -m3 3 -m4 3 -mh 5 -m5 0 -model1dumpfrequency 1 -o mgiza/corpus.ja-en -s mgiza/corpus.en.vcb -t mgiza/corpus.ja.vcb -emprobforempty 0.0 -probsmooth 1e-7
$MGIZA/mgizapp/bin/mgiza -ncpus 16 -CoocurrenceFile mgiza/corpus.en-ja.cooc -c mgiza/corpus.en-ja-int-train.snt -m1 5 -m2 0 -m3 3 -m4 3 -mh 5 -m5 0 -model1dumpfrequency 1 -o mgiza/corpus.en-ja -s mgiza/corpus.ja.vcb -t mgiza/corpus.en.vcb -emprobforempty 0.0 -probsmooth 1e-7

cat ./mgiza/corpus.en.vcb | egrep ' [^ ][^ ]+$' > mgiza/corpus.en.filtered.vcb
cat ./mgiza/corpus.ja.vcb | egrep ' [^ ][^ ]+$' > mgiza/corpus.ja.filtered.vcb

python ../lex_dic.py mgiza/corpus.en.filtered.vcb mgiza/corpus.ja.filtered.vcb mgiza/corpus.en-ja.t3.final mgiza/corpus.ja-en.t3.final lex.e2f.gz lex.f2e.gz

cat ../GlobalVoices.en-ja.ja > corpus.en-ja.ja
cat ../GlobalVoices.en-ja.en > corpus.en-ja.en 
paste corpus.en-ja.en corpus.en-ja.ja > train.en-ja
lines=$(cat train.en-ja | wc -l)
trainlines=$(echo "$lines*4/10" | bc);
testlines=$(echo "($lines-2*$trainlines)/2" | bc)

python $BICLEANER/bicleaner/bicleaner_train.py \
    train.en-ja \
    -S "$MOSES/scripts/tokenizer/tokenizer.perl -q -b -a -l en" -T "mecab -Owakati" \
    --treat_oovs --normalize_by_length \
    -s en -t ja \
    -d lex.e2f.gz -D lex.f2e.gz \
    -c en-ja.classifier \
    -g $trainlines -w $trainlines \
    --good_test_examples $testlines --wrong_test_examples $testlines \
    -m en-ja.yaml \
    --classifier_type random_forest
