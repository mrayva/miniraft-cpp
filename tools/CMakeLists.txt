if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(miniraft-migrate-disk-state migrate_disk_state.cpp)
  target_link_libraries(miniraft-migrate-disk-state PRIVATE miniraft)
endif()
