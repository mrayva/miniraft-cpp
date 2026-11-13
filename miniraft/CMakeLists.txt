add_library(miniraft
    messages.cpp
    raft.cpp
    persist.cpp
    durable.cpp
    snapshot.cpp
    configuration.cpp
)

target_include_directories(miniraft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_compile_features(miniraft PUBLIC cxx_std_20)
