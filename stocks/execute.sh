#!/bin/sh

LIMIT=10000
SYMBOL1="A"
SYMBOL2="AA"


./averageopen.py -s $SYMBOL1 -l $LIMIT
./averageopen.py -s $SYMBOL2 -l $LIMIT

./variance.py -s $SYMBOL1 -l $LIMIT
./variance.py -s $SYMBOL2 -l $LIMIT

./correlation.py -f $SYMBOL1 -s $SYMBOL2 -l $LIMIT
