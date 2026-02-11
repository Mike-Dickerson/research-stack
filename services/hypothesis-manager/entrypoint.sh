#!/bin/bash

# Ensure config directory exists and is writable
mkdir -p /app/config

# If precepts.json exists but isn't writable, fix permissions
if [ -f /app/config/precepts.json ]; then
    chmod 666 /app/config/precepts.json 2>/dev/null || true
fi

# Ensure the directory itself is writable
chmod 777 /app/config 2>/dev/null || true

# Start the application
exec python -u app.py
