cmake_minimum_required(VERSION 3.10)
project(miniraft VERSION 0.1.0 LANGUAGES CXX)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if (EXISTS ${CMAKE_BINARY_DIR}/local.cmake)
include(${CMAKE_BINARY_DIR}/local.cmake)
endif ()

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
option(MINIRAFT_BUILD_NETWORK "Build coroio networking and examples" ON)

add_subdirectory(miniraft)
add_subdirectory(tools)
if(MINIRAFT_BUILD_NETWORK)
  add_subdirectory(coroio/coroio)
  add_subdirectory(miniraft/net)
  add_subdirectory(examples)
endif()
if(BUILD_TESTING)
  add_subdirectory(test)
endif()

set(MINIRAFT_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/miniraft")
configure_package_config_file(
  cmake/miniraftConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/miniraftConfig.cmake"
  INSTALL_DESTINATION "${MINIRAFT_INSTALL_CMAKEDIR}"
)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/miniraftConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion
)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/miniraftConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/miniraftConfigVersion.cmake"
  DESTINATION "${MINIRAFT_INSTALL_CMAKEDIR}"
)
