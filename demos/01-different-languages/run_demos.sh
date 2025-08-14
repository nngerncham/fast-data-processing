#!/bin/bash

cd python/
.venv/bin/python main.py

cd ../rust/
cargo run --release
