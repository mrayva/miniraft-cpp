add_library(miniraft.net
    server.cpp
)

target_link_libraries(miniraft.net PUBLIC coroio miniraft)
target_compile_features(miniraft.net PUBLIC cxx_std_20)

set_target_properties(miniraft.net PROPERTIES EXPORT_NAME net)
install(TARGETS miniraft.net EXPORT miniraftTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(FILES
    server.h
    node_impl.h
    retirable_ssl_socket.h
    idempotent_client_transport.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/miniraft/net
)
