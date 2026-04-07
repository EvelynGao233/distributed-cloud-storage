# Optional: institutional gRPC toolchain image (replace IMAGE with your class image if applicable).
# Public alternative: ./run_docker_local.sh
docker run -it --rm --name cis5050 \
  -w /home/cis5050/workspace \
  -v "$(pwd)":/home/cis5050/workspace \
  -p 2500:2500 \
  -p 11000:11000 \
  -p 5050-5100:5050-5100 \
  -p 8080-8100:8080-8100 \
  cis5050/docker-env:gRPC
