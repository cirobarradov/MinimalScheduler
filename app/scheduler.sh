#!/bin/bash

. /venv/bin/activate
exec python /app/MinimalScheduler.py $1 $2
