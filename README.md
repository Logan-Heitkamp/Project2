Group: Will Horvath, Logan Heitkamp, Rohit Ramesh

# CS4065 Project 2 - Simple Bulletin Board Using Socket Programming

A bulletin board with a server and a client, written entirely in Python using
plain TCP sockets (no networking libraries). Part 1 is a single public message
board. Part 2 adds five private groups. A user can use the public board and any
number of private groups in the same session without reconnecting.

The client has a command-line interface (the `%` commands) and a tkinter GUI. Both are in `client.py`.

## Files

| File | Purpose |
|---|---|
| `server.py` | The server |
| `client.py` | The client: command-line interface (default) and GUI (`--gui`) |
| `Makefile` | Shortcuts for running the server and client |

## Requirements

* Python 3 (standard library only). Nothing needs to be compiled.
* The GUI needs tkinter, which comes with Python on Windows and macOS. On
  Debian/Ubuntu, install it with `sudo apt install python3-tk`. The
  command-line client does not need tkinter.

## How to Run

Start the server first (default port 5050):

```
python3 server.py                 # or: make server
python3 server.py --port 5050     # use a different port
```

Then start one or more clients, each in its own terminal:

```
python3 client.py                    # command-line client, then type: %connect 127.0.0.1 5050
python3 client.py 127.0.0.1 5050     # command-line client, connects right away
python3 client.py --gui 127.0.0.1 5050   # GUI client
```

The Makefile has the same shortcuts: `make server`, `make client`, `make gui`.
Change the defaults with `make client HOST=<address> PORT=<port>`.

To connect from another computer, run the server on one machine and use that
machine's IP address and port in `%connect` (or the GUI's Host/Port boxes).

## Commands

| Command | What it does |
|---|---|
| `%connect <host> <port>` | Connect to the server |
| `%join [username]` | Join the public board |
| `%post <subject> <body>` | Post a message to the public board |
| `%users` | List the users on the public board |
| `%message <id>` | Show the full content of a message |
| `%leave` | Leave the public board |
| `%groups` | List the private groups |
| `%groupjoin <group> [username]` | Join a private group (by id or name) |
| `%grouppost <group> <subject> <body>` | Post a message to a group |
| `%groupusers <group>` | List the users in a group |
| `%groupmessage <group> <id>` | Show the full content of a group message |
| `%groupleave <group>` | Leave a group |
| `%exit` | Disconnect and quit |
| `%help` | Show the command list |

The private groups are `1 school`, `2 sports`, `3 music`, `4 entertainment` and
`5 gaming`. `<group>` can be either the number or the name.

The subject is the first word of what you type after the command. For a
subject with several words, put it in quotes:

```
%post Hello! Hello everyone, welcome to the board
%post "Lunch plans" Pizza at noon?
%grouppost music "Concert tonight" Doors open at 8
```

Messages are listed as `[Message ID] Sender | Post Date | Subject`. Use
`%message` or `%groupmessage` with the ID to read the full message.

## Using the GUI

1. Enter the host and port, then click **Connect**.
2. Type a username, select a board on the left, and click **Join**. Boards you
   have joined are marked with `*`, and you can join more than one.
3. The table shows the selected board's messages. Double-click a row to read the
   full message. The list on the right shows who is in the board.
4. Type a subject and a body at the bottom and click **Post**. **Leave** leaves
   the selected board.

## How It Works

* **Messages.** TCP delivers a stream of bytes with no message boundaries, so
  every message between the client and server is one JSON object on its own
  line, ended by a newline.
* **Boards.** The server keeps six boards: the public board and the five private
  groups. Each board has its own users, messages and message IDs, so nothing
  from one board shows up in another.
* **Usernames** must be unique within a board (not case sensitive) and can use
  letters, digits, `_`, `-` and `.` (up to 32 characters). Joining a group does
  not disconnect you from the others.
* **Joining a board.** The user gets the list of current users and the last 2
  messages. They can read only those 2 messages and anything posted after they
  joined, not older ones.
* **Notifications.** Everyone else in a board is told when a user joins or
  leaves and when a new message is posted. A client that disconnects without
  using `%leave` or `%exit` is removed from all of its boards, and the others
  are notified.
* **Threads.** The server uses one thread per connected client to read
  requests, and each client also has a thread that sends its replies and
  notifications. The client has a background thread that receives messages, so
  notifications appear while the user is typing.

## Major Issues We Ran Into

* **TCP has no message boundaries.** One `recv` can return half a message or
  several messages at once. We ended every message with a newline and read one
  line at a time.
* **Two threads writing to one socket.** A reply and a broadcast could mix
  their bytes. Each client now has a queue, and one thread per client does all of
  its sending, so a slow or dead client cannot hold up a broadcast to everyone
  else.
* **Clients that leave without saying so.** A closed window or lost connection
  must still remove the user from every board and notify the others. The server
  treats a closed or reset connection the same as `%exit`.
* **Reading messages from before joining.** At first, a user could request any
  message on a board by ID, including ones posted before they joined. The
  server now remembers the first message each member is allowed to read on each
  board and rejects requests for older ones.
* **tkinter is not thread-safe.** The GUI's receiving thread cannot update
  widgets directly. It puts messages on a queue, and the GUI checks the queue
  every 100 ms and updates the screen from the main thread.
* **Receiving while typing.** The command-line client prints incoming
  notifications from its receiving thread and redraws the prompt, so they show up
  right away.