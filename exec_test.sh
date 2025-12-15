#!/bin/bash
zig run perf-hash.zig -O ReleaseSafe -- sample_hash_safe.csv
zig run perf-hash.zig -O ReleaseSafe -- sample_hash_safe.csv.gzip
zig run perf-hash.zig -O ReleaseFast -- sample_hash_fast.csv
zig run perf-hash.zig -O ReleaseFast -- sample_hash_fast.csv.gzip
zig run perf-hash.zig -O ReleaseSmall -- sample_hash_small.csv
zig run perf-hash.zig -O ReleaseSmall -- sample_hash_small.csv.gzip
source .venv/bin/activate
python main.py