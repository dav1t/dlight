const std = @import("std");
const Io = std.Io;

const dlight = @import("dlight");

const MetaCommandResult = enum { meta_command_success, meta_command_unrecognized_command };
const PrepareResult = enum { perpare_success, pepare_unrecognized_statement };

const StatementType = enum { statement_insert, statement_select };

const Statement = struct { type: StatementType };

fn printPrompt(writer: *Io.Writer) Io.Writer.Error!void {
    try writer.print("db > ", .{});
}

fn executeStatement(writer: *Io.Writer, statement: *Statement) !void {
    switch (statement.type) {
        .statement_insert => {
            try writer.print("This is where we would do an insert. \n", .{});
        },
        .statement_select => {
            try writer.print("This is where would do a select \n", .{});
        },
    }
}

fn prepareStatement(input: *const []u8, statement: *Statement) PrepareResult {
    if (std.mem.eql(u8, input.*, "insert")) {
        statement.type = StatementType.statement_insert;
        return PrepareResult.perpare_success;
    }

    if (std.mem.eql(u8, input.*, "select")) {
        statement.type = StatementType.statement_select;
        return PrepareResult.perpare_success;
    }

    return PrepareResult.pepare_unrecognized_statement;
}

fn doMetaCommand(input: *const []u8) MetaCommandResult {
    if (std.mem.eql(u8, input.*, ".exit")) {
        std.process.exit(0);
    } else {
        return MetaCommandResult.meta_command_unrecognized_command;
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
            switch (doMetaCommand(&line)) {
                MetaCommandResult.meta_command_success => continue,
                MetaCommandResult.meta_command_unrecognized_command => {
                    try stdout.print("Unrecognized command '{s}' \n", .{line});
                    try stdout.flush();
                    continue;
                },
            }
        }

        var statement: Statement = Statement{ .type = .statement_insert };

        switch (prepareStatement(&line, &statement)) {
            .pepare_unrecognized_statement => {
                try stdout.print("Unrecognized keyword at start of '{s}' \n", .{line});
                try stdout.flush();
                continue;
            },
            .perpare_success => {},
        }

        try executeStatement(stdout, &statement);
        try stdout.print("Executed. \n", .{});
        try stdout.flush();
    }
}
