#!/bin/bash

file_path="/app/cache/.locked"

if [ ! -f "$file_path" ]; then
   python3 main.py
fi