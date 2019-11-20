mkdir -p pproc
cd pproc

MOSES=/root/mosesdecoder

cat ../*.en-ja.en | sed "s/&apos;/'/g" | sed 's/&quot;/\"/g' | sed 's/&amp;/\&/g' | $MOSES/scripts/tokenizer/tokenizer.perl -q -b -a -l en > corpus.tok.en
cat ../*.en-ja.ja | sed "s/&apos;/'/g" | sed 's/&quot;/\"/g' | sed 's/&amp;/\&/g' | mecab -Owakati > corpus.tok.ja

perl $MOSES/scripts/recaser/train-truecaser.perl --model truecase-model.en --corpus corpus.tok.en 
perl $MOSES/scripts/recaser/train-truecaser.perl --model truecase-model.ja --corpus corpus.tok.ja 

perl $MOSES/scripts/recaser/truecase.perl --model truecase-model.en < corpus.tok.en > corpus.tok.case.en
perl $MOSES/scripts/recaser/truecase.perl --model truecase-model.ja < corpus.tok.ja > corpus.tok.case.ja

#cat corpus.tok.en | $MOSES/scripts/tokenizer/lowercase.perl > corpus.tok.low.en
#cat corpus.tok.ja | $MOSES/scripts/tokenizer/lowercase.perl > corpus.tok.low.ja

perl $MOSES/scripts/training/clean-corpus-n.perl corpus.tok.case en ja corpus.clean 1 80 corpus.lines-retained

paste corpus.clean.en corpus.clean.ja > corpus.clean.en-ja
python ../split_data.py corpus.clean.en-ja 0.1 0.01 corpus.en-ja.good corpus.en-ja.bad corpus.en-ja.dev

cat corpus.en-ja.good | cut -f1 > corpus.en-ja.good.en
cat corpus.en-ja.good | cut -f2 > corpus.en-ja.good.ja

cat corpus.en-ja.bad | cut -f1 > corpus.en-ja.bad.en
cat corpus.en-ja.bad | cut -f2 > corpus.en-ja.bad.ja
shuf corpus.en-ja.bad.ja > corpus.en-ja.bad.shuf
paste corpus.en-ja.bad.en corpus.en-ja.bad.shuf > corpus.en-ja.bad

cat corpus.en-ja.dev | cut -f1 > corpus.en-ja.dev.en
cat corpus.en-ja.dev | cut -f2 > corpus.en-ja.dev.ja
