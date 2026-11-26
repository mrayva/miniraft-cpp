add_library(miniraft
    messages.cpp
    idempotency.cpp
    idempotent_client.cpp
    durable_idempotent_client.cpp
    raft.cpp
    persist.cpp
    durable.cpp
    snapshot.cpp
    configuration.cpp
)

target_include_directories(miniraft PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_compile_features(miniraft PUBLIC cxx_std_20)

set_target_properties(miniraft PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

install(TARGETS miniraft EXPORT miniraftTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(FILES
    messages.h
    persist.h
    durable.h
    state.h
    timesource.h
    quorum.h
    memory_budget.h
    snapshot.h
    snapshot_codec.h
    idempotency.h
    idempotent_client.h
    configuration.h
    raft.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/miniraft
)
install(EXPORT miniraftTargets
    FILE miniraftTargets.cmake
    NAMESPACE miniraft::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/miniraft
)
