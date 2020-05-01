#!/bin/bash

# Prefix command with this to get Python profiling of preflight:
## python -m cProfile -o pipeline.profile $CTRL_MPEXEC_DIR/bin/

# Or this to get remote debugging with VSCode (for jbosch):
## python -m ptvsd --host localhost --port 25689 --wait $CTRL_MPEXEC_DIR/bin/

pipetask -i 'calibs/hsc/default','raw/hsc','refcats/ps1_pv3','skymaps','masks/hsc' \
"$@" \
--register-dataset-types \
-p "$PIPE_TASKS_DIR"/pipelines/DataReleaseProduction.yaml
