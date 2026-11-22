# CMake package

Use CMake 3.20 or newer. The core library installs as `miniraft::miniraft`
with its public headers and C++20 requirement. A build with
`MINIRAFT_BUILD_NETWORK=ON` also installs the `Network` component as
`miniraft::net`.

Build and install it to a prefix:

```sh
cmake -S . -B build-package \
  -DMINIRAFT_BUILD_NETWORK=OFF \
  -DBUILD_TESTING=OFF \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-package --target miniraft
cmake --install build-package --prefix "$HOME/.local"
```

A downstream project can use the package with:

```cmake
find_package(miniraft 0.1 CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE miniraft::miniraft)
```

Set `CMAKE_PREFIX_PATH` to the install prefix when it is not in CMake's default
search paths. The exported target carries the include directory and C++20
compile feature requirement.
