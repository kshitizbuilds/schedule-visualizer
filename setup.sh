#!/bin/bash

# The directory containing your Oracle Instant Client files in the deployed environment
INSTANTCLIENT_DIR="/app/instantclient_23_9"

# Add the Instant Client directory to the dynamic linker search path (LD_LIBRARY_PATH)
# This is the fix for the DPI-1047 error.
echo "Setting LD_LIBRARY_PATH for Oracle Instant Client..."
export LD_LIBRARY_PATH="$INSTANTCLIENT_DIR:$LD_LIBRARY_PATH"
echo "LD_LIBRARY_PATH is set to: $LD_LIBRARY_PATH"