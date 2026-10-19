#!/usr/bin/env bash
# Создаёт тестовый каталог с вложенными папками, дубликатами
# и файлами одинакового размера, но с разным содержимым.
# Использование: ./gen_testdata.sh [каталог] [число файлов]
set -euo pipefail
dir=${1:-testdata}
count=${2:-200}
rm -rf "$dir"
mkdir -p "$dir"
for i in $(seq 1 "$count"); do
    sub="$dir/d$((i % 7))/s$((i % 3))"
    mkdir -p "$sub"
    size=$(( (RANDOM % 64 + 1) * 64 ))   # KiB, многие размеры совпадут
    head -c "${size}K" /dev/urandom > "$sub/file_$i.bin"
done
# Дубликаты: копируем каждый пятый файл в другие папки.
mkdir -p "$dir/copies/deep/er"
i=0
find "$dir" -name 'file_*.bin' | sort | while read -r f; do
    i=$((i + 1))
    if (( i % 5 == 0 )); then cp "$f" "$dir/copies/$(basename "$f" .bin)_copy.bin"; fi
    if (( i % 15 == 0 )); then cp "$f" "$dir/copies/deep/er/$(basename "$f" .bin)_copy2.bin"; fi
done
echo "тестовые данные созданы в $dir"
