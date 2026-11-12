#!/bin/sh
# Regenerate the Python classes in sapient_msg/ from the vendored proto files.
# The proto files are BSI Flex 335 v2.0 from https://github.com/dstl/SAPIENT-Proto-Files
# (Apache 2.0, see sapient_msg/LICENCE.txt), commit 244a741 (2024-06-04).
#
# Uses protoc from grpcio-tools 1.62.x (protobuf 4.25 generated code): the result runs on
# protobuf 4.25 up to 5.x. Newer protoc versions add a runtime version check that refuses
# older protobuf runtimes.
#     pip install grpcio-tools==1.62.3
#     integrations/sapient/tools/generate_proto.sh
set -e
cd "$(dirname "$0")/.."
python3 -m grpc_tools.protoc -I . --python_out=. --pyi_out=. \
    sapient_msg/proto_options.proto sapient_msg/bsi_flex_335_v2_0/*.proto
echo "generated: sapient_msg/*_pb2.py"
