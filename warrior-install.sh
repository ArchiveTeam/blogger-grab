#!/bin/bash

echo "Installing lua-date..."
sudo luarocks install date || exit 1

exit 0
