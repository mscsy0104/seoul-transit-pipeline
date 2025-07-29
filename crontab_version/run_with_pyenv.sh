#!/bin/bash

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv virtualenv-init -)"

pyenv activate seoul-transits-pipeline39

python /Users/sychoi/projects/seoul-trainsits-pipeline/crontab_version/test.py >> /Users/sychoi/projects/seoul-trainsits-pipeline/crontab_version/logs/test_log.txt 2>&1
