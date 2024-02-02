#/bin/bash

CSV_DIR="./data/csv"
rm -rf ${CSV_DIR}
mkdir -p ${CSV_DIR}

mdb-tables -1 data.accdb | xargs -I{} bash -c 'mdb-export data.accdb "$1" >"$1".csv' -- {}
mv *.csv ./data/csv/ -v