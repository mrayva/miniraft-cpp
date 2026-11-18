# CMake package

The core `miniraft` library installs as the imported target
`miniraft::miniraft`, with its public headers and C++20 requirement. The package
does not include the optional built-in network server.

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
