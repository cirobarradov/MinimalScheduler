#!/bin/bash

. /venv/bin/activate
exec python /app/scheduler.py $1 $2
