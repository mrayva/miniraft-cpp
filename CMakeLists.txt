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
  # The coroio submodule exposes its source include path directly. Replace that
  # one path with build/install interfaces so the target can be exported.
  get_target_property(_coroio_includes coroio INTERFACE_INCLUDE_DIRECTORIES)
  get_filename_component(_coroio_source_include "${PROJECT_SOURCE_DIR}/coroio" ABSOLUTE)
  list(REMOVE_ITEM _coroio_includes
    "${PROJECT_SOURCE_DIR}/coroio/coroio/.."
    "${_coroio_source_include}")
  list(APPEND _coroio_includes
    "$<BUILD_INTERFACE:${_coroio_source_include}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
  set_property(TARGET coroio PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${_coroio_includes}")
  set_target_properties(coroio PROPERTIES EXPORT_NAME coroio)
  install(TARGETS coroio EXPORT miniraftTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  install(DIRECTORY "${PROJECT_SOURCE_DIR}/coroio/coroio/"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/coroio
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp")
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
