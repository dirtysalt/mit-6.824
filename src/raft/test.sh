#!/usr/bin/env bash
# Copyright (C) dirlt

rm -rf output
mkdir -p output
for ((i=0;i<50;i++))
do
    go test -run 2B | tee output/run$i.txt
done
grep "^FAIL" output/*
