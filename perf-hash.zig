const std = @import("std");

pub fn generateIPv4(random: std.Random) [4]u8 {
    var ip: [4]u8 = undefined;
    random.bytes(&ip);
    return ip;
}

pub fn generateIPv6(random: std.Random) [16]u8 {
    var ip: [16]u8 = undefined;
    random.bytes(&ip);
    return ip;
}

pub inline fn generateIP(random: std.Random, mode: enum { ip4, ip6 }) [16]u8 {
    switch (mode) {
        inline .ip4 => return [_]u8{0} ** 12 ++ generateIPv4(random),
        inline .ip6 => return generateIPv6(random),
    }
}

const AddressMap = std.AutoHashMapUnmanaged([16]u8, *allowzero anyopaque);

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    var iterArgs = try std.process.argsWithAllocator(allocator);
    defer iterArgs.deinit();
    _ = iterArgs.skip(); // skip program name
    const outputPath = iterArgs.next() orelse @panic("expected output path argument");

    var thread = std.Io.Threaded.init(allocator);
    defer thread.deinit();
    const io = thread.ioBasic();

    var rng = std.Random.Sfc64.init(0);
    const r = rng.random();
    var hashTable: std.AutoHashMap([16]u8, *allowzero anyopaque) = .init(allocator);
    defer hashTable.deinit();

    const COUNT = 40_000_000;

    var start: std.Io.Timestamp = undefined;
    var duration: std.Io.Duration = undefined;

    start = try io.vtable.now(io.userdata, .real);
    for (0..COUNT) |i| {
        const ip = generateIP(r, if (i & 1 == 0) .ip4 else .ip6);
        _ = try hashTable.put(ip, @ptrFromInt(0));
    }
    duration = start.durationTo(io.vtable.now(io.userdata, .real) catch unreachable);
    std.debug.print("Inserted {d} entries in {D} ({d} ns)\n", .{
        COUNT,
        @as(i64, @truncate(duration.nanoseconds)),
        @as(f64, @floatFromInt(duration.nanoseconds)) / @as(f64, @floatFromInt(COUNT)),
    });

    var match: u32 = 0;

    var rng2 = std.Random.Sfc64.init(0);
    const sample = rng2.random();

    const queueBuffer = allocator.alloc(?CSVRow, 1024) catch unreachable;
    defer allocator.free(queueBuffer);
    var queue = std.Io.Queue(?CSVRow).init(queueBuffer);

    var f = try io.concurrent(writeCSV, .{ io, allocator, &queue, outputPath });
    defer f.cancel(io);

    start = try io.vtable.now(io.userdata, .real);

    for (0..COUNT) |i| {
        switch (sample.int(u8)) {
            inline 0 => {
                var timer = try std.time.Timer.start();
                const ip = generateIP(r, if (i & 1 == 0) .ip4 else .ip6);
                if (hashTable.get(ip) != null) match += 1;
                const duration2 = timer.read();
                queue.putOneUncancelable(io, .{
                    .type = (if (i & 1 == 0) "ip4" else "ip6").*,
                    .ip = ip,
                    .index = i,
                    .duration_ns = duration2,
                });
            },
            else => {
                const ip = generateIP(r, if (i & 1 == 0) .ip4 else .ip6);
                if (hashTable.get(ip) != null) match += 1;
            },
        }
    }

    duration = start.durationTo(io.vtable.now(io.userdata, .real) catch unreachable);
    std.debug.print("Found {d} matches\n", .{match});
    std.debug.print("Looked up {d} entries in {D} ({d} ns)\n", .{
        COUNT,
        @as(i64, @truncate(duration.nanoseconds)),
        @as(f64, @floatFromInt(duration.nanoseconds)) / @as(f64, @floatFromInt(COUNT)),
    });
}

const CSVRow = struct {
    type: [3]u8,
    ip: [16]u8,
    index: usize,
    duration_ns: u64,
};

fn writeCSV(io: std.Io, allocator: std.mem.Allocator, queue: *std.Io.Queue(?CSVRow), path: []const u8) void {
    var csv = std.fs.cwd().createFile(path, .{ .truncate = true }) catch unreachable;
    defer csv.close();

    const writerBuffer = allocator.alloc(u8, std.heap.pageSize()) catch unreachable;
    defer allocator.free(writerBuffer);
    var fileWriter = csv.writer(writerBuffer);
    defer fileWriter.interface.flush() catch unreachable;
    var writer = &fileWriter.interface;

    var writerCompressBuffer: ?[]u8 = null;
    defer if (writerCompressBuffer) |wcb| allocator.free(wcb);
    var compressWriter: ?std.compress.flate.Compress = null;
    defer if (compressWriter) |_| {
        writer.flush() catch unreachable;
        writer = &fileWriter.interface;
    };

    if (std.mem.endsWith(u8, path, ".gzip")) {
        writerCompressBuffer = allocator.alloc(u8, std.heap.pageSize() * 64) catch unreachable;
        compressWriter = std.compress.flate.Compress.init(&fileWriter.interface, writerCompressBuffer.?, .gzip, .default) catch unreachable;
        writer = &compressWriter.?.writer;
    }

    writer.print("type,ip,index,duration_ns\n", .{}) catch unreachable;
    while (!io.cancelRequested()) {
        const row = (queue.getOne(io) catch break) orelse break;
        _ = writer.print("{s},{x},{d},{d}\n", .{
            row.type,
            if (row.type[2] == '4') row.ip[12..16] else row.ip[0..],
            row.index,
            row.duration_ns,
        }) catch unreachable;
    }
}
