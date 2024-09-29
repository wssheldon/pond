#!/bin/bash
set -e

# Build Pond Planner
cd ../../pond/pond-planner
cargo lambda build --release
cd -

# Build Pond Duckling
cd ../../pond/pond-duckling
cargo lambda build --release
cd -

# Run CDK commands
cdk synth
