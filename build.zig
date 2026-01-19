const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // Parse build.zig.zon to extract version information
    const zon_content = @embedFile("build.zig.zon");
    const BuildZon = struct {
        version: []const u8,
        name: ?[]const u8 = null,
    };

    // Parse the ZON content at compile time, or use fallback values if parsing fails
    var parsed_zon: BuildZon = undefined;
    var should_free = false;
    if (std.zon.parse.fromSlice(BuildZon, b.allocator, zon_content, null, .{})) |result| {
        parsed_zon = result;
        should_free = true;
    } else |_| {
        // Fallback values if parsing fails
        parsed_zon = BuildZon{ .version = "0.0.0", .name = "nats.zig" };
    }
    defer if (should_free) std.zon.parse.free(b.allocator, parsed_zon);

    const zio = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });

    // Create build options
    const options = b.addOptions();
    options.addOption([]const u8, "version", parsed_zon.version);
    options.addOption([]const u8, "name", parsed_zon.name orelse "nats.zig");
    options.addOption([]const u8, "lang", "zig");

    // This creates a "module", which represents a collection of source files alongside
    // some compilation options, such as optimization mode and linked system libraries.
    // Every executable or library we compile will be based on one or more modules.
    const lib_mod = b.addModule("nats", .{
        // `root_source_file` is the Zig "entry point" of the module. If a module
        // only contains e.g. external object files, you can make this `null`.
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_mod.addImport("zio", zio.module("zio"));

    // Add build options to the module
    lib_mod.addOptions("build_options", options);

    // Now, we will create a static library based on the module we created above.
    // This creates a `std.Build.Step.Compile`, which is the build step responsible
    // for actually invoking the compiler.
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "nats_zig",
        .root_module = lib_mod,
    });

    // This declares intent for the library to be installed into the standard
    // location when the user invokes the "install" step (the default step when
    // running `zig build`).
    b.installArtifact(lib);

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    // Similar to creating the run step earlier, this exposes a `test-unit` step to
    // the `zig build --help` menu, providing a way for the user to request
    // running the unit tests.
    const unit_tests_step = b.step("test-unit", "Run unit tests");
    unit_tests_step.dependOn(&run_lib_unit_tests.step);

    // Integration tests (require Docker and NATS server)
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/all_tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });
    integration_tests.root_module.addImport("nats", lib_mod);
    integration_tests.root_module.addImport("zio", zio.module("zio"));

    const run_integration_tests = b.addRunArtifact(integration_tests);
    run_integration_tests.has_side_effects = true; // Allow repeated runs with Docker interactions

    const e2e_tests_step = b.step("test-e2e", "Run end-to-end tests (requires Docker)");
    e2e_tests_step.dependOn(&run_integration_tests.step);

    // All tests - this is now the main test target
    const test_step = b.step("test", "Run all tests (unit and e2e)");
    test_step.dependOn(&run_lib_unit_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    // Create all example executables
    const example_files = [_]struct { name: []const u8, file: []const u8 }{
        .{ .name = "pub", .file = "examples/pub.zig" },
        .{ .name = "sub", .file = "examples/sub.zig" },
        .{ .name = "requestor", .file = "examples/requestor.zig" },
        .{ .name = "replier", .file = "examples/replier.zig" },
    };

    const examples_step = b.step("examples", "Build all examples");

    for (example_files) |example_info| {
        const exe = b.addExecutable(.{
            .name = example_info.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(example_info.file),
                .target = target,
                .optimize = optimize,
            }),
        });
        exe.root_module.addImport("nats", lib_mod);

        // Only install examples when explicitly building examples step
        const install_exe = b.addInstallArtifact(exe, .{});
        examples_step.dependOn(&install_exe.step);
    }

    // Create benchmark executables
    const benchmark_files = [_]struct { name: []const u8, file: []const u8 }{
        .{ .name = "echo_server", .file = "benchmarks/echo_server.zig" },
        .{ .name = "echo_client", .file = "benchmarks/echo_client.zig" },
        .{ .name = "publisher", .file = "benchmarks/publisher.zig" },
        .{ .name = "subscriber", .file = "benchmarks/subscriber.zig" },
    };

    const benchmarks_step = b.step("benchmarks", "Build all benchmarks");

    for (benchmark_files) |benchmark_info| {
        const exe = b.addExecutable(.{
            .name = benchmark_info.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(benchmark_info.file),
                .target = target,
                .optimize = optimize,
            }),
        });
        exe.root_module.addImport("nats", lib_mod);

        // Only install benchmarks when explicitly building benchmarks step
        const install_exe = b.addInstallArtifact(exe, .{});
        benchmarks_step.dependOn(&install_exe.step);
    }

    // C benchmarks (require libnats)
    const c_echo_server = b.addExecutable(.{
        .name = "echo_server_c",
        .root_module = b.createModule(.{
            .root_source_file = null,
            .target = target,
            .optimize = optimize,
        }),
    });
    c_echo_server.addCSourceFile(.{ .file = b.path("benchmarks/echo_server.c"), .flags = &.{} });
    c_echo_server.addCSourceFile(.{ .file = b.path("benchmarks/bench_util.c"), .flags = &.{} });
    c_echo_server.linkLibC();
    c_echo_server.linkSystemLibrary("nats");

    const c_echo_client = b.addExecutable(.{
        .name = "echo_client_c",
        .root_module = b.createModule(.{
            .root_source_file = null,
            .target = target,
            .optimize = optimize,
        }),
    });
    c_echo_client.addCSourceFile(.{ .file = b.path("benchmarks/echo_client.c"), .flags = &.{} });
    c_echo_client.addCSourceFile(.{ .file = b.path("benchmarks/bench_util.c"), .flags = &.{} });
    c_echo_client.linkLibC();
    c_echo_client.linkSystemLibrary("nats");

    const c_publisher = b.addExecutable(.{
        .name = "publisher_c",
        .root_module = b.createModule(.{
            .root_source_file = null,
            .target = target,
            .optimize = optimize,
        }),
    });
    c_publisher.addCSourceFile(.{ .file = b.path("benchmarks/publisher.c"), .flags = &.{} });
    c_publisher.addCSourceFile(.{ .file = b.path("benchmarks/bench_util.c"), .flags = &.{} });
    c_publisher.linkLibC();
    c_publisher.linkSystemLibrary("nats");

    const c_subscriber = b.addExecutable(.{
        .name = "subscriber_c",
        .root_module = b.createModule(.{
            .root_source_file = null,
            .target = target,
            .optimize = optimize,
        }),
    });
    c_subscriber.addCSourceFile(.{ .file = b.path("benchmarks/subscriber.c"), .flags = &.{} });
    c_subscriber.addCSourceFile(.{ .file = b.path("benchmarks/bench_util.c"), .flags = &.{} });
    c_subscriber.linkLibC();
    c_subscriber.linkSystemLibrary("nats");

    const install_c_echo_server = b.addInstallArtifact(c_echo_server, .{});
    const install_c_echo_client = b.addInstallArtifact(c_echo_client, .{});
    const install_c_publisher = b.addInstallArtifact(c_publisher, .{});
    const install_c_subscriber = b.addInstallArtifact(c_subscriber, .{});

    benchmarks_step.dependOn(&install_c_echo_server.step);
    benchmarks_step.dependOn(&install_c_echo_client.step);
    benchmarks_step.dependOn(&install_c_publisher.step);
    benchmarks_step.dependOn(&install_c_subscriber.step);
}
