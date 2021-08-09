#!/usr/bin/env bash

#
# basic map-reduce test
#

#RACE=

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

#########################################################
echo '***' Starting job count test.

rm -f mr-*

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so &
timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so
timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so &
timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -ne "8" ]
then
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
else
  echo '---' job count test: PASS
fi

wait

#########################################################