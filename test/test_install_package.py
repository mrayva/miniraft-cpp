"""Build a small consumer against the installed MiniRaft network package."""
import pathlib
import subprocess
import sys
import tempfile


def run(command):
    result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError("command failed:\n{}\n{}{}".format(
            " ".join(map(str, command)), result.stdout, result.stderr))


def main():
    build = pathlib.Path(sys.argv[1]).resolve()
    cmake = sys.argv[2]
    minimum_version = sys.argv[3]
    compile_flags = sys.argv[4]
    linker_flags = sys.argv[5]
    source_root = pathlib.Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(prefix="miniraft-package-") as directory:
        root = pathlib.Path(directory)
        prefix = root / "install"
        source = root / "consumer"
        source.mkdir()
        (source / "CMakeLists.txt").write_text(
            f"cmake_minimum_required(VERSION {minimum_version})\n"
            "project(miniraft_package_consumer LANGUAGES CXX)\n"
            "find_package(miniraft CONFIG REQUIRED COMPONENTS Network)\n"
            "add_executable(package_consumer main.cpp)\n"
            "target_link_libraries(package_consumer PRIVATE miniraft::net)\n")
        (source / "main.cpp").write_text(
            "#include <coroio/select.hpp>\n"
            "#include <miniraft/net/idempotent_client_transport.h>\n"
            "int main() {\n"
            "  NNet::TIdempotentClientRetryOptions options;\n"
            "  NNet::TSelect poller;\n"
            "  NNet::TAddress address{\"127.0.0.1\", 1};\n"
            "  NNet::TSocket socket(address, poller);\n"
            "  return options.MaxAttempts == 4 && socket.Fd() >= 0 ? 0 : 1;\n"
            "}\n")

        run([cmake, "--install", str(build), "--prefix", str(prefix)])
        export_file = prefix / "lib/cmake/miniraft/miniraftTargets.cmake"
        if str(source_root) in export_file.read_text():
            raise RuntimeError("Installed targets retain a build-machine source path")
        consumer_build = source / "build"
        consumer_configure = [cmake, "-S", str(source), "-B", str(consumer_build),
                              "-DCMAKE_PREFIX_PATH={}".format(prefix)]
        if compile_flags:
            consumer_configure.append("-DCMAKE_CXX_FLAGS={}".format(compile_flags))
        if linker_flags:
            consumer_configure.append("-DCMAKE_EXE_LINKER_FLAGS={}".format(linker_flags))
        run(consumer_configure)
        run([cmake, "--build", str(consumer_build)])

        core_build = root / "core-build"
        run([cmake, "-S", str(source_root), "-B", str(core_build),
             "-DMINIRAFT_BUILD_NETWORK=OFF", "-DBUILD_TESTING=OFF"])
        run([cmake, "--build", str(core_build), "--target", "miniraft"])
        core_prefix = root / "core-install"
        run([cmake, "--install", str(core_build), "--prefix", str(core_prefix)])

        unavailable_network = subprocess.run(
            [cmake, "-S", str(source), "-B", str(root / "network-unavailable"),
             "-DCMAKE_PREFIX_PATH={}".format(core_prefix)],
            text=True, capture_output=True)
        if unavailable_network.returncode == 0 or "miniraft_FOUND to FALSE" not in (
                unavailable_network.stdout + unavailable_network.stderr):
            raise RuntimeError("Core-only package did not reject its unavailable Network component:\n{}{}".format(
                unavailable_network.stdout, unavailable_network.stderr))

        core_source = root / "core-consumer"
        core_source.mkdir()
        (core_source / "CMakeLists.txt").write_text(
            f"cmake_minimum_required(VERSION {minimum_version})\n"
            "project(miniraft_core_consumer LANGUAGES CXX)\n"
            "find_package(miniraft CONFIG REQUIRED)\n"
            "add_executable(core_consumer main.cpp)\n"
            "target_link_libraries(core_consumer PRIVATE miniraft::miniraft)\n")
        (core_source / "main.cpp").write_text(
            "#include <miniraft/raft.h>\n"
            "int main() { return 0; }\n")
        core_consumer_build = core_source / "build"
        run([cmake, "-S", str(core_source), "-B", str(core_consumer_build),
             "-DCMAKE_PREFIX_PATH={}".format(core_prefix)])
        run([cmake, "--build", str(core_consumer_build)])
    print("Installed MiniRaft network and core package consumer builds passed")


if __name__ == "__main__":
    main()
