const std = @import("std");
const Io = std.Io;

const dlight = @import("dlight");

const COLUMN_USERNAME_SIZE = 32;
const COLUMN_EMAIL_SIZE = 255;

const MetaCommandError = error{UnrecognizedCommand};
const PrepareError = error{UnrecognizedStatement};
const SyntaxError = error{CantParseArgument};
const ExecuteError = error{ ExecuteTableFull, SerializationError };

const Row = struct {
    id: i32,
    username: [COLUMN_USERNAME_SIZE]u8,
    email: [COLUMN_EMAIL_SIZE]u8,
};

const StatementType = enum { none, insert, select };
const Statement = struct { type: StatementType, row_to_insert: ?Row };

const ID_SIZE = @sizeOf(@FieldType(Row, "id"));
const USERNAME_SIZE = @sizeOf(@FieldType(Row, "username"));
const EMAIL_SIZE = @sizeOf(@FieldType(Row, "email"));

const ID_OFFSET = @offsetOf(Row, "id");
const USERNAME_OFFSET = @offsetOf(Row, "username");
const EMAIL_OFFSET = @offsetOf(Row, "email");

const ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE = 4096;
const TABLE_MAX_PAGES = 100;
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

const Table = struct {
    num_rows: u32,
    pages: [TABLE_MAX_PAGES]?[]u8,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) Table {
        return Table{ .num_rows = 0, .pages = [_]?[]u8{null} ** TABLE_MAX_PAGES, .allocator = allocator };
    }

    fn deinit(self: *Table) void {
        for (self.pages) |page| {
            if (page) |p| {
                self.allocator.free(p);
            }
        }
    }

    fn rowSlot(self: *Table, row_num: u32) ![]u8 {
        const pageNum = row_num / ROWS_PER_PAGE;

        if (self.pages[pageNum] == null) {
            self.pages[pageNum] = try self.allocator.alloc(u8, PAGE_SIZE);
        }

        const page = self.pages[pageNum].?;
        const rowOffset = row_num % ROWS_PER_PAGE;
        const byteOffset = rowOffset * ROW_SIZE;

        return page[byteOffset .. byteOffset + ROW_SIZE];
    }
};

fn serializeRow(row: *const Row, dest: []u8) void {
    std.mem.copyForwards(u8, dest[ID_OFFSET .. ID_OFFSET + ID_SIZE], std.mem.asBytes(&row.id));
    std.mem.copyForwards(u8, dest[USERNAME_OFFSET .. USERNAME_OFFSET + USERNAME_SIZE], &row.username);
    std.mem.copyForwards(u8, dest[EMAIL_OFFSET .. EMAIL_OFFSET + EMAIL_SIZE], &row.email);
}

fn deserializeRow(src: []const u8, dest: *Row) void {
    dest.id = std.mem.bytesAsValue(i32, src[ID_OFFSET .. ID_OFFSET + ID_SIZE]).*;
    @memcpy(&dest.username, src[USERNAME_OFFSET .. USERNAME_OFFSET + USERNAME_SIZE]);
    @memcpy(&dest.email, src[EMAIL_OFFSET .. EMAIL_OFFSET + EMAIL_SIZE]);
}

fn printPrompt(writer: *Io.Writer) Io.Writer.Error!void {
    try writer.print("db > ", .{});
}

fn print_row(row: *Row) void {
    std.debug.print("({d}, {s}, {s})\n", .{ row.id, row.email, row.username });
}

fn executeInsert(statement: *Statement, table: *Table) ExecuteError!void {
    if (table.num_rows >= TABLE_MAX_ROWS) {
        return ExecuteError.ExecuteTableFull;
    }

    const row: *Row = &statement.row_to_insert.?;
    const slot = table.rowSlot(table.num_rows) catch {
        return ExecuteError.SerializationError;
    };
    serializeRow(row, slot);
    table.num_rows += 1;
}

fn executeSelect(statement: *Statement, table: *Table) ExecuteError!void {
    var row: Row = undefined;

    if (statement.type == .none) {
        // PLACEHOLDER
        return ExecuteError.ExecuteTableFull;
    }

    for (0..table.num_rows) |i| {
        const slot = table.rowSlot(@intCast(i)) catch {
            return ExecuteError.SerializationError;
        };
        deserializeRow(slot, &row);
        print_row(&row);
    }
}

fn executeStatement(writer: *Io.Writer, statement: *Statement, table: *Table) !void {
    switch (statement.type) {
        .insert => {
            try executeInsert(statement, table);
            return;
        },
        .select => {
            try executeSelect(statement, table);
            return;
        },
        .none => {
            try writer.print("Nothing happens. \n", .{});
        },
    }
}

fn prepareStatement(input: []const u8, statement: *Statement) PrepareError!void {
    if (std.mem.startsWith(u8, input, "insert")) {
        statement.type = StatementType.insert;
        statement.row_to_insert = parseArguments(input) catch |err| switch (err) {
            SyntaxError.CantParseArgument => null,
        };
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

fn parseArguments(input: []const u8) SyntaxError!Row {
    var it = std.mem.tokenizeScalar(u8, input, ' ');

    _ = it.next() orelse return SyntaxError.CantParseArgument;

    const id_str = it.next() orelse return SyntaxError.CantParseArgument;
    const username_str = it.next() orelse return SyntaxError.CantParseArgument;
    const email_str = it.next() orelse return SyntaxError.CantParseArgument;

    const id = std.fmt.parseInt(i32, id_str, 10) catch {
        return SyntaxError.CantParseArgument;
    };

    if (username_str.len > COLUMN_USERNAME_SIZE or email_str.len > COLUMN_EMAIL_SIZE) {
        return SyntaxError.CantParseArgument;
    }

    var username: [COLUMN_USERNAME_SIZE]u8 = [_]u8{0} ** COLUMN_USERNAME_SIZE;
    var email: [COLUMN_EMAIL_SIZE]u8 = [_]u8{0} ** COLUMN_EMAIL_SIZE;

    std.mem.copyForwards(u8, username[0..username_str.len], username_str);
    std.mem.copyForwards(u8, email[0..email_str.len], email_str);

    return Row{ .id = id, .email = email, .username = username };
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    const allocator = init.gpa;
    var table = Table.init(allocator);
    defer table.deinit();

    var stdout_buffer: [1024]u8 = undefined;
    var stdin_buffer: [1024]u8 = undefined;

    var stdout_file_writer: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const stdout = &stdout_file_writer.interface;

    const manualSize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
    try stdout.print("builtin Row size is {} \n manually calculated size is {} \n", .{ ROW_SIZE, manualSize });
    try stdout.flush();

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

        var statement: Statement = .{ .type = .none, .row_to_insert = null };

        prepareStatement(line, &statement) catch |err| switch (err) {
            PrepareError.UnrecognizedStatement => {
                try stdout.print("Unrecognized keyword at start of '{s}'\n", .{line});
                try stdout.flush();
                continue;
            },
        };

        try executeStatement(stdout, &statement, &table);
        try stdout.print("Executed.\n", .{});
        try stdout.flush();
    }
}
