#!/bin/sh

PY=$1
SYNC_DATE=$2
CMD="$PY /home/mercy/git/US-stock-data/sync.py $SYNC_DATE"
$PY -m QPhantom.exec.guard --to luyunfei@ppmoney.com --project USHIS_DATA --label Daily_sync $CMD
