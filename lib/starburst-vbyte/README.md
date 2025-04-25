# Java wrapper around streamvbyte library


## Building native libs

We store prebuilt native streamvbyte libraries in the repo for following platforms
 * Macos ARM64
 * Linux ARM64
 * Linux AMD64

To rebuild with newer versions gain shell access on machine with relevant architecture.
* For linux builds we used ubuntu AWS machines.
* For macos we used ARM based macbook.

Install toolchain.
On linux:
```
sudo apt update
sudo apt -y install cmake make gcc g++
```
On macos:
```
brew install cmake
```

and the follow the steps:
 
```
git clone https://github.com/fast-pack/streamvbyte
cd streamvbyte

cmake -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX:PATH=`pwd`/install \
  -DSTREAMVBYTE_ENABLE_EXAMPLES=ON \
  -DSTREAMVBYTE_ENABLE_TESTS=ON \
  -DBUILD_SHARED_LIBS=true \
  -B \
  build

cmake --build build
ctest --test-dir build

cmake --install build
```

Target artifacts are available as `install/lib/libstreamvbyte.so` and `install/lib/libstreamvbyte.dylib`
