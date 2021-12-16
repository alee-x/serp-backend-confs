#!/bin/bash

exec gunicorn -b :5000 api:app