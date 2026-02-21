const std = @import("std");
const Io = std.Io;

const dlight = @import("dlight");

const MetaCommandError = error{UnrecognizedCommand};
const PrepareError = error{UnrecognizedStatement};

const StatementType = enum { insert, select };
const Statement = struct { type: StatementType };

fn printPrompt(writer: *Io.Writer) Io.Writer.Error!void {
    try writer.print("db > ", .{});
}

fn executeStatement(writer: *Io.Writer, statement: *Statement) !void {
    switch (statement.type) {
        .insert => {
            try writer.print("This is where we would do an insert.\n", .{});
        },
        .select => {
            try writer.print("This is where we would do a select.\n", .{});
        },
    }
}

fn prepareStatement(input: []const u8, statement: *Statement) PrepareError!void {
    if (std.mem.eql(u8, input, "insert")) {
        statement.type = StatementType.insert;
        return;
    }

    if (std.mem.eql(u8, input, "select")) {
        statement.type = StatementType.select;
        return;
    }

    return PrepareError.UnrecognizedStatement;
}

fn doMetaCommand(input: []const u8) MetaCommandError!void {
    if (std.mem.eql(u8, input, ".exit")) {
        std.process.exit(0);
    } else {
        return MetaCommandError.UnrecognizedCommand;
    }
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    var stdout_buffer: [1024]u8 = undefined;
    var stdin_buffer: [1024]u8 = undefined;

    var stdout_file_writer: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const stdout = &stdout_file_writer.interface;

    while (true) {
        try printPrompt(stdout);
        try stdout.flush();

        var stdin_file: Io.File.Reader = .init(.stdin(), io, &stdin_buffer);
        const stdin = &stdin_file.interface;
        const line = try stdin.takeDelimiterExclusive('\n');

        // If command is meta command
        if (line[0] == '.') {
            doMetaCommand(line) catch |err| switch (err) {
                MetaCommandError.UnrecognizedCommand => {
                    try stdout.print("Unrecognized command '{s}'\n", .{line});
                    try stdout.flush();
                    continue;
                },
            };
            continue;
        }

        var statement: Statement = .{ .type = .insert };

        prepareStatement(line, &statement) catch |err| switch (err) {
            PrepareError.UnrecognizedStatement => {
                try stdout.print("Unrecognized keyword at start of '{s}'\n", .{line});
                try stdout.flush();
                continue;
            },
        };

        try executeStatement(stdout, &statement);
        try stdout.print("Executed.\n", .{});
        try stdout.flush();
    }
}
