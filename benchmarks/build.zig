const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast });

    const nats = b.dependency("nats", .{
        .target = target,
        .optimize = optimize,
    });

    const zio = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });

    const benchmarks = [_][]const u8{
        "echo_server",
        "echo_client",
        "publisher",
        "subscriber",
    };

    for (benchmarks) |name| {
        const exe = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("{s}.zig", .{name})),
                .target = target,
                .optimize = optimize,
            }),
        });
        exe.root_module.addImport("nats", nats.module("nats"));
        exe.root_module.addImport("zio", zio.module("zio"));
        b.installArtifact(exe);
    }

    // C equivalents built on the official client (require system libnats)
    for (benchmarks) |name| {
        const exe = b.addExecutable(.{
            .name = b.fmt("{s}_c", .{name}),
            .root_module = b.createModule(.{
                .root_source_file = null,
                .target = target,
                .optimize = optimize,
                .link_libc = true,
            }),
        });
        exe.root_module.addCSourceFile(.{ .file = b.path(b.fmt("{s}.c", .{name})), .flags = &.{} });
        exe.root_module.addCSourceFile(.{ .file = b.path("bench_util.c"), .flags = &.{} });
        exe.root_module.linkSystemLibrary("nats", .{});
        b.installArtifact(exe);
    }
}
