level=$1

i=1
while [[ $i -le $level ]];do
    wc -l patternoutput-$i/part*
    i=`expr $i + 1`
done
