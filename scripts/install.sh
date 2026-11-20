#!/usr/bin/env bash
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo 'Запуск: sudo bash scripts/install.sh' >&2; exit 1; }
version=3.5.7
name="spark-${version}-bin-hadoop3"
archive="/var/cache/${name}.tgz"
base="https://archive.apache.org/dist/spark/spark-${version}"
command -v curl >/dev/null
command -v sha512sum >/dev/null
if [[ ! -x /opt/$name/bin/spark-submit ]]; then
  curl --fail --location --retry 3 "$base/$name.tgz" -o "$archive"
  curl --fail --location --retry 3 "$base/$name.tgz.sha512" -o "$archive.sha512"
  expected=$(awk '{for(i=1;i<=NF;i++) if(length($i)==128 && $i ~ /^[0-9a-fA-F]+$/) print tolower($i)}' "$archive.sha512")
  [[ $expected =~ ^[0-9a-f]{128}$ ]] || { echo 'Не удалось прочитать SHA512 Apache.' >&2; exit 1; }
  actual=$(sha512sum "$archive" | awk '{print $1}')
  [[ $actual == "$expected" ]] || { echo 'SHA512 архива не совпадает.' >&2; exit 1; }
  tar -xzf "$archive" -C /opt
fi
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/"$name"/bin/spark-submit --version

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
command -v python3 >/dev/null
# Ubuntu 24.04: apt-get install python3-venv, if venv support is missing.
if [[ ! -x /opt/hw5-prefect/bin/python ]]; then
  python3 -m venv /opt/hw5-prefect
fi
/opt/hw5-prefect/bin/python -m pip install -r "$ROOT/requirements.txt"
/opt/hw5-prefect/bin/python -c 'import prefect; print("PREFECT_VERSION=" + prefect.__version__)'
echo INSTALL_OK
