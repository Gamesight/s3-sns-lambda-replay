#!/bin/bash
if [ ! -f venv/bin/activate ]; then
  python3 -m venv venv
fi

. venv/bin/activate
pip install -r requirements.txt
