#!/usr/bin/env bash
set -euo pipefail

rm -f /tmp/hdfs-ready
if [[ ! -f /var/lib/hdfs/name/current/VERSION ]]; then
    hdfs namenode -format -nonInteractive
fi

hdfs namenode &
namenode=$!
hdfs datanode &
datanode=$!
trap 'kill "$namenode" "$datanode" 2>/dev/null || true; wait || true' EXIT
trap 'exit 0' TERM INT

for ((attempt = 0; attempt < 90; attempt++)); do
    kill -0 "$namenode" "$datanode"
    if hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'Safe mode is OFF'; then
        hdfs dfs -mkdir -p /data
        hdfs dfs -chown tlc /data
        touch /tmp/hdfs-ready
        wait -n "$namenode" "$datanode"
        exit $?
    fi
    sleep 2
done

echo "NameNode o DataNode HDFS non pronti entro il tempo previsto" >&2
exit 1
