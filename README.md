Names: <Your Name>, <Partner 1 Name>, <Partner 2 Name>

# CS4065 Project 2 - A Simple Bulletin Board Using Socket Programming

A client-server bulletin board written entirely in Python using plain TCP
sockets (the `socket` module only - no other networking libraries). It
implements both parts of the assignment in one program:

* **Part 1** - one public message board.
* **Part 2** - five private message boards that a user can join simultaneously.
  A user can use the public board and any number of private groups in the same
  session without reconnecting.

It includes a command-line client (the `%command` interface from the
assignment) and a tkinter GUI client (extra credit), both in `client.py`. Both talk to the same server.

## Files

| File | Purpose |
|---|---|
| `server.py` | The server: protocol helpers, threading, and all bulletin board logic |
| `client.py` | The client: command-line interface (default) and tkinter GUI (`--gui`) in one file |
| `Makefile` | Shortcuts (`make server`, `make client`, `make gui`) |

## Requirements

* Python 3.8 or newer (developed and tested on 3.12 and 3.13). Only the standard library is used.
* The GUI (`--gui`) needs tkinter; the command-line client does not. tkinter ships with Python on Windows and macOS
  (python.org installers). On Debian/Ubuntu install it with `sudo apt install python3-tk`.
* Nothing needs compiling.

## How to run

Start the server (default port 5050, any port above 1023 works):

```
python3 server.py                # or: make server
python3 server.py --port 6000    # custom port (optionally --host <address>)
```

Start one or more clients (each in its own terminal / window):

```
python3 client.py                      # then type: %connect 127.0.0.1 5050
python3 client.py 127.0.0.1 5050       # connects immediately
python3 client.py --gui [host port]   # GUI (or: make gui)
```

To use the client from another machine, run the server there and `%connect` to
that machine's IP address and port (make sure the firewall allows the port).

## Using the command-line client

Commands follow the assignment's suggested set. Type `%help` in the client for
the same list.

| Command | What it does |
|---|---|
| `%connect <host> <port>` | Connect to a server |
| `%join [username]` | Join the public board (prompts for a username if you have none yet) |
| `%post <subject> <body>` | Post to the public board |
| `%users` | List users on the public board |
| `%message <id>` | Show the full content of a public-board message |
| `%leave` | Leave the public board |
| `%groups` | List the 5 private groups (with current member counts) |
| `%groupjoin <group> [username]` | Join a private group by **id or name** (can join several) |
| `%grouppost <group> <subject> <body>` | Post to a group |
| `%groupusers <group>` | List users in a group |
| `%groupmessage <group> <id>` | Show a message from a group |
| `%groupleave <group>` | Leave a group |
| `%exit` | Disconnect and quit |

Private groups are: `1 general`, `2 sports`, `3 music`, `4 tech`, `5 games`
(`%groupjoin 3 sam` and `%groupjoin music sam` are equivalent).

**Writing subjects and bodies.** The subject is the first word by default.
For a multi-word subject, put it in quotes or separate it from the body with ` | `:

```
%post Hello everyone, welcome to the board
%post "Lunch plans" Pizza at noon?
%post Lunch plans | Pizza at noon?
%grouppost music "Gig tonight" Doors open at 8
```

**What you see.** When you join you get the user list and the last 2 messages.
Afterwards the server pushes events as they happen, even while you are typing:

```
[music] * bob joined the group
[music] New message: [3] bob | 2026-11-10 16:42:00 | Gig tonight
[music] * bob left the group
```

Messages are listed as `[Message ID] Sender | Post Date | Subject`. Use
`%message` / `%groupmessage` with the ID to fetch the body.

## Using the GUI

1. Enter host/port and click **Connect**.
2. Type a **Username**, select a board on the left (`public` or a private group) and click **Join**.
   Boards you have joined are marked with `*`. You can join several; click a board to switch views.
3. The table shows that board's messages (ID, Sender, Post Date, Subject). **Double-click** a row
   to read the message body. The right-hand list shows the users in the board.
4. Type a subject and body at the bottom and click **Post**. **Leave** leaves the selected board.
   The Activity log shows joins, leaves, and new messages as they arrive.

## Protocol specification

TCP delivers an ordered byte stream with no message boundaries, so the
application protocol is **newline-delimited JSON**: each message is one JSON
object, UTF-8 encoded, terminated by `\n`. (`json.dumps` escapes newlines inside
strings, so a raw `\n` only ever means "end of message".) Receivers read one line
at a time, so messages split across TCP segments or glued together are handled.
Lines over 256 KB are rejected.

### Client -> server requests

| `type` | Fields | Success response fields |
|---|---|---|
| `join` | `group`, `user` | `group`, `group_id`, `user`, `users` (list), `history` (last 2 message headers) |
| `leave` | `group` | `group`, `user` |
| `post` | `group`, `subject`, `body` | `group`, `message` (header) |
| `users` | `group` | `group`, `users` |
| `message` | `group`, `id` | `group`, `message` (header + `body`) |
| `groups` | - | `groups` (list of `{id, name, members}`) |
| `exit` | - | (server then closes the connection) |

`group` is a name or numeric id; `"public"` / `0` is the Part 1 board. If omitted, it defaults to `public`.

### Server -> client messages

* `{"type":"welcome","public_group":"public","groups":[{id,name,members}, ...]}`
  sent once when a client connects (the list of 5 groups).
* `{"type":"response","cmd":<request type>,"status":"ok", ...fields above}`
* `{"type":"response","cmd":<request type>,"status":"error","reason":"..."}`
* `{"type":"notify","event":"user_joined","group":g,"user":u}`
* `{"type":"notify","event":"user_left","group":g,"user":u}`
* `{"type":"notify","event":"new_message","group":g,"message":{id,sender,date,subject}}`

Example exchange:

```
C: {"type":"join","group":"music","user":"alice"}
S: {"type":"response","cmd":"join","status":"ok","group":"music","group_id":3,"user":"alice","users":["alice"],"history":[]}
S: {"type":"notify","event":"user_joined","group":"music","user":"bob"}
C: {"type":"post","group":"music","subject":"Gig","body":"Tonight at 8"}
S: {"type":"response","cmd":"post","status":"ok","group":"music","message":{"id":1,"sender":"alice","date":"2026-11-10 16:42:00","subject":"Gig"}}
```

## Design decisions

* **Boards.** The server has 6 independent boards: the public board (Part 1) and 5 private groups
  (Part 2). Each has its own members, messages and message-ID counter (IDs start at 1 per board),
  so nothing leaks between boards. Part 1 is just "the board called `public`", which is why both parts work
  in the same process and session.
* **Usernames** must be unique *per board* (case-insensitive) and may use letters, digits, `_`, `-`, `.`
  (max 32). The same connection may use different names in different groups. No authentication, as specified.
* **Notifications** (join/leave/new message) go to every *other* member of that board; the person who
  acted gets a direct response instead. A client that disconnects without leaving is removed from every board
  and the others are notified.
* **Membership is required** to post, list users, read messages, or leave a board; errors are returned
  for non-members.
* **History on join** is the last 2 messages of that board (headers only; fetch bodies with `%message`).
  A member can read only those 2 messages plus everything posted after they joined. Requesting an
  older message returns an error ("posted before you joined"). The window is recalculated if the user leaves and rejoins.
* **Limits:** subject <= 100 chars, body <= 8000 chars.
* **Threading.** One reader thread per client plus one writer thread per client. All sends are
  `queue.put` calls, so a slow client can't stall a broadcast, and two threads never write to the same socket
  at once. A single lock guards the shared state, and replies are queued while it is held so each client
  sees events in the order the server applied them.

## Major issues and how we handled them

* **TCP has no message boundaries.** A single `recv` can return half a message or two messages. We framed
  every message with a newline and read line-by-line through a buffered reader; tests send messages split in
  two and two in one segment.
* **Concurrent writes to a socket.** A broadcast thread and a reply thread could interleave bytes. Fixed with
  the per-client outbound queue and single writer thread described above.
* **Dead clients during broadcasts.** A write to a vanished client must not abort the broadcast to everyone
  else. Because each client has its own writer thread, a failed send only affects that client, whose reader
  thread then cleans it up.
* **Clients leaving without `%leave`.** Reader threads treat EOF/reset as a disconnect and run the same cleanup
  as `%exit`, notifying every board the user was in.
* **Receiving while typing (CLI).** The CLI uses a receive thread so notifications print immediately; the prompt
  is redrawn after each async message.
* **tkinter is not thread-safe.** The GUI's receive thread only enqueues messages; the Tk main loop polls the queue
  every 100 ms and updates widgets.

## Testing

During development the server and clients were tested with automated scripts that start a real server on a free port
and talk to it over raw sockets. They covered both parts: join/duplicate usernames, last-2 history (and refusing older
messages), broadcast and retrieval, leave and `exit`, abrupt disconnects, private-group isolation, joining multiple groups
on one connection, malformed input, split and coalesced TCP segments, Unicode/multi-line bodies, and 8 concurrent clients
posting at once. The command-line client was run against a live server with two clients, and the GUI was driven
end-to-end under a virtual display. (The test scripts are not part of this submission.)

## Known limitations

* No persistence: messages and users are kept in memory and are lost when the server stops.
* Message listings carry no pagination; boards are assumed small.
* The `%` command line is parsed simply (see the subject/body rules above); there is no escaping of ` | ` inside a subject.
