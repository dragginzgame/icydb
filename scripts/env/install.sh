#!/bin/bash

#
# This script is designed to be run on a fresh Ubuntu install
#

#
# ERROR HANDLING
#

set -e

print_error() {
    echo -e "\033[1;31mERROR: Command '$2' failed with exit code $1\033[0m"
}

error_trap() {
    local last_exit_code=$?
    local last_command=$BASH_COMMAND
    print_error "$last_exit_code" "$last_command"
    exit $last_exit_code
}

# Set the trap for any command that exits with non-zero status
trap 'error_trap' ERR

#
# START
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$ROOT"

# apt
sudo apt -y update && sudo apt -y upgrade
# os
sudo apt -y install build-essential ntp ntpdate cmake curl wget libssl-dev pkg-config
# misc helpers
sudo apt -y install speedtest-cli fdupes tree cloc
# profiling
sudo apt -y install valgrind
# wasm
sudo apt -y install binaryen wabt
# orm
sudo apt -y install jq
# perf
sudo apt -y install linux-tools-common linux-tools-generic linux-headers-generic
sudo apt -y autoremove

# system
sudo ntpdate ntp.ubuntu.com || true

# rust
echo "" | curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.profile

# rust cfg
rustup toolchain install beta
rustup toolchain install nightly
rustup target add wasm32-unknown-unknown

# dfx
sh -ci "$(curl -fsSL https://smartcontracts.org/install.sh)"
source ~/.profile

# bin
mkdir -p $HOME/bin

# didc
# https://github.com/dfinity/candid/releases/
wget https://github.com/dfinity/candid/releases/download/2024-05-14/didc-linux64
mv -f ./didc-linux64 $HOME/bin/didc
chmod +x $HOME/bin/didc

# idl2json
# https://github.com/dfinity/idl2json/releases
wget https://github.com/dfinity/idl2json/releases/download/v0.10.1/idl2json_cli-x86_64-unknown-linux-musl.tar.gz
tar -xzf idl2json_cli-x86_64-unknown-linux-musl.tar.gz
rm -rf idl2json_cli-x86_64-unknown-linux-musl.tar.gz
mv -f ./idl2json $HOME/bin/idl2json
chmod +x $HOME/bin/idl2json
mv -f ./yaml2candid $HOME/bin/yaml2candid
chmod +x $HOME/bin/yaml2candid
source ~/.profile

# quill
# https://github.com/dfinity/quill/releases/
wget https://github.com/dfinity/quill/releases/download/v0.5.4/quill-linux-x86_64
mv -f ./quill-linux-x86_64 $HOME/bin/quill
chmod +x $HOME/bin/quill
source ~/.profile

# run update script
"$SCRIPT_DIR"/update.sh
