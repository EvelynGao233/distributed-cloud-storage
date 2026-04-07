# Dev image with gRPC + Protobuf CMake CONFIG packages under /usr/local.
# (Stock Ubuntu/Debian apt packages often lack ProtobufConfig.cmake required by this repo.)
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build and install gRPC (bundles protobuf); first build is slow but one-time per image.
RUN git clone -b v1.62.0 --depth 1 --recurse-submodules --shallow-submodules \
      https://github.com/grpc/grpc /tmp/grpc \
    && cmake -S /tmp/grpc -B /tmp/grpc/cmake/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build /tmp/grpc/cmake/build -j"$(nproc)" \
    && cmake --install /tmp/grpc/cmake/build \
    && ldconfig \
    && rm -rf /tmp/grpc

WORKDIR /workspace
CMD ["/bin/bash"]
