"""
Group: Will Horvath, Logan Heitkamp, Rohit Ramesh

Client side we decided to create using python.

This file will handle all client side processes including receiving packet from server, sending messages to sever,
and handle the GUI.

"""

import json
import queue
import socket
import sys
import threading

# Protocol helpers
DEFAULT_PORT = 5050
ENCODING = "utf-8"
MAX_LINE_BYTES = 256 * 1024
PUBLIC_GROUP = "public" # the single public board used by Part 1
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
HISTORY_ON_JOIN = 2

MAX_USERNAME_LEN = 32
MAX_SUBJECT_LEN = 100
MAX_BODY_LEN = 8000

DEFAULT_HOST = "127.0.0.1"


class ProtocolError(Exception):
    """
    Raised when incoming bytes cannot be turned into a protocol message.

    fatal means the stream can no longer be trusted (e.g. a line longer
    than MAX_LINE_BYTES) and the connection should be closed.
    """

    def __init__(self, reason, fatal=False):
        super().__init__(reason)
        self.reason = reason
        self.fatal = fatal


# serialize a dict into a string
def encode(message):
    return (json.dumps(message, separators=(",", ":")) + "\n").encode(ENCODING)


# Parse string into a dict
def decode(line):
    try:
        obj = json.loads(line.decode(ENCODING))
    except (UnicodeDecodeError, ValueError):
        raise ProtocolError("malformed JSON")
    if not isinstance(obj, dict):
        raise ProtocolError("message must be a JSON object")
    return obj


# Read the next message from the buffer
def read_message(rfile):
    while True:
        line = rfile.readline(MAX_LINE_BYTES + 1)
        if not line:
            return None
        if len(line) > MAX_LINE_BYTES and not line.endswith(b"\n"):
            raise ProtocolError("message too long", fatal=True)
        if line.strip():
            return decode(line)


class Client:
    """
    TCP connection to the server and a background receive thread.

    on_message(msg) is called (from the receive thread) for every message
    the server sends. on_disconnect() is called if the connection is lost
    (not when the application itself calls close).
    """

    def __init__(self, on_message, on_disconnect=None):
        self.on_message = on_message
        self.on_disconnect = on_disconnect
        self.socket = None
        self.send_lock = threading.Lock()

    # Returns connection state
    def is_connected(self):
        return self.socket is not None

    # create connection with server
    def connect(self, host, port, timeout=5.0):
        if self.socket is not None:
            raise ConnectionError("already connected")

        # setup socket
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(None)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.socket = sock

        # start connection thread
        threading.Thread(target=self._reader, args=(sock,), daemon=True).start()

    # Continuously reads messages from server as they are sent to client
    def _reader(self, sock):
        rfile = sock.makefile("rb")
        # Try to read all available messages
        try:
            while True:
                # try to read next message
                try:
                    message = read_message(rfile)
                except ProtocolError as exc:
                    if exc.fatal:
                        break
                    continue                 # skip one garbled line, keep going
                if message is None:
                    break
                # send message notification to user
                self.on_message(message)
        except OSError:
            pass

        # Close socket and disconnect
        finally:
            try:
                rfile.close()
            except OSError:
                pass
            if self.socket is sock:           # connection lost (not closed by us)
                self.socket = None
                try:
                    sock.close()
                except OSError:
                    pass
                if self.on_disconnect:
                    self.on_disconnect()

    # Build request packet to send to server
    def request(self, request_type, **fields):
        self.send({"type": request_type, **fields})

    # send packet to server
    def send(self, message):
        sock = self.socket

        if sock is None:
            raise ConnectionError("Not connected. Use %connect <host> <port>.")

        data = encode(message)

        with self.send_lock:                # one writer at a time per socket
            try:
                sock.sendall(data)
            except OSError as exc:
                raise ConnectionError(f"Send failed: {exc}") from exc

    #  close connection with server
    def close(self):
        sock = self.socket

        if sock is None:
            return
        try:
            self.send({"type": "exit"})
        except ConnectionError:
            pass

        self.socket = None

        for action in (lambda: sock.shutdown(socket.SHUT_RDWR), sock.close):
            try:
                action()
            except OSError:
                pass


def split_subject_body(text):
    """
    Split "subject body" typed after %post into (subject, body).

    Accepted forms (first match wins):
    - "Quoted subject" body text     (single or double quotes)
    - Subject body text              (subject is the first word)
    Returns None if either part is missing.
    """
    text = text.strip()

    if not text:
        return None

    subject = body = None

    # "quoted subject" body text
    if text[0] in "\"'":
        end = text.find(text[0], 1)
        if end > 1:
            subject, body = text[1:end], text[end + 1:]

    # subject body text
    if subject is None:
        subject, _, body = text.partition(" ")

    subject, body = subject.strip(), body.strip()
    return (subject, body) if subject and body else None


# Format header into str
def format_header(message):
    return (f"[{message.get('id')}] {message.get('sender')} | "
            f"{message.get('date')} | {message.get('subject')}")

# Format server responses to display in terminal
def format_event(msg, hints=True):
    kind = msg.get("type")

    if kind == "welcome":
        if not hints:
            return ["Connected to the bulletin board."]
        lines = ["Connected to the bulletin board.",
                 "Public board: use %join <username>.",
                 "Private groups (use %groupjoin <id|name> <username>):"]
        lines += [f"  {g['id']}. {g['name']}" for g in msg.get("groups", [])]
        lines.append("Type %help for all commands.")
        return lines

    if kind == "response":
        if msg.get("status") != "ok":
            return [f"Error ({msg.get('cmd')}): {msg.get('reason')}"]
        return _format_ok(msg)

    if kind == "notify":
        return _format_notify(msg, hints)
    return [f"Unrecognized message from server: {msg!r}"]


# Format "ok" responses from the server
def _format_ok(msg):
    cmd, group = msg.get("cmd"), msg.get("group")

    if cmd == "join":
        users = ", ".join(msg.get("users", []))
        lines = [f"Joined '{group}' as {msg.get('user')}.",
                 f"Users in '{group}': {users}"]
        history = msg.get("history", [])
        if history:
            lines.append(f"Last {len(history)} message(s):")
            lines += ["  " + format_header(m) for m in history]
        else:
            lines.append("No messages posted yet.")
        return lines

    if cmd == "leave":
        return [f"You left '{group}'."]

    if cmd == "post":
        return [f"Posted to '{group}': {format_header(msg['message'])}"]

    if cmd == "users":
        users = ", ".join(msg.get("users", []))
        return [f"Users in '{group}': {users}"]

    if cmd == "message":
        m = msg["message"]
        return [f"--- Message {m.get('id')} in '{group}' ---",
                f"From:    {m.get('sender')}",
                f"Date:    {m.get('date')}",
                f"Subject: {m.get('subject')}",
                "", m.get("body", ""), "---"]

    if cmd == "groups":
        lines = ["Private groups (public board: %join):"]
        lines += [f"  {g['id']}. {g['name']} ({g['members']} user{'' if g['members'] == 1 else 's'})"
                  for g in msg.get("groups", [])]
        return lines

    if cmd == "exit":
        return ["Goodbye."]
    return [f"OK ({cmd})"]


# Format notifications from server including new messages and users joining and leaving
def _format_notify(msg, hints=True):
    tag = f"[{msg.get('group')}] "
    event = msg.get("event")

    if event == "user_joined":
        return [f"{tag}* {msg.get('user')} joined the group"]

    if event == "user_left":
        return [f"{tag}* {msg.get('user')} left the group"]

    if event == "new_message":
        lines = [f"{tag}New message: {format_header(msg['message'])}"]
        if hints:
            lines.append(f"      (read it with {_read_hint(msg)})")
        return lines

    return [f"{tag}Event: {event}"]


# Format the read hint message
def _read_hint(msg):
    mid = msg["message"].get("id")

    if msg.get("group") == PUBLIC_GROUP:
        return f"%message {mid}"

    return f"%groupmessage {msg.get('group')} {mid}"


# ---------- Client CLI ----------

HELP = """\
Connection
  %connect <host> <port>        connect to a bulletin board server
  %exit                         disconnect and quit

Public board (Part 1)
  %join [username]              join the public board
  %post <subject> <body>        post a message   (subject: first word, "in quotes",
                                or text before a ' | ' separator)
  %users                        list users on the public board
  %message <id>                 show the full text of a message
  %leave                        leave the public board

Private groups (Part 2)  -  <group> is a group id or name
  %groups                       list the available groups
  %groupjoin <group> [username] join a group (you can join several)
  %grouppost <group> <subject> <body>
  %groupusers <group>
  %groupmessage <group> <id>
  %groupleave <group>

  %help                         show this help
"""


class BulletinCLI:
    """
    Handles all commands for the client in a custom terminal that only handles commands for this project.
    """

    def __init__(self):
        self.client = Client(self._on_message, self._on_disconnect)
        self.username = None                 # last name successfully joined with
        self._print_lock = threading.Lock()
        self.commands = {
            "%connect": self.cmd_connect, "%exit": self.cmd_exit,
            "%join": self.cmd_join, "%post": self.cmd_post,
            "%users": self.cmd_users, "%message": self.cmd_message,
            "%leave": self.cmd_leave, "%groups": self.cmd_groups,
            "%groupjoin": self.cmd_groupjoin, "%grouppost": self.cmd_grouppost,
            "%groupusers": self.cmd_groupusers, "%groupmessage": self.cmd_groupmessage,
            "%groupleave": self.cmd_groupleave, "%help": self.cmd_help,
        }

    # ---- output from the receive thread -----------------------------------

    def _show(self, lines):
        """Print server output, then redraw the prompt."""
        with self._print_lock:
            sys.stdout.write("\r  \r" + "\n".join(lines) + "\n> ")
            sys.stdout.flush()

    def _on_message(self, msg):
        if (msg.get("type") == "response" and msg.get("status") == "ok"
                and msg.get("cmd") == "join"):
            self.username = msg.get("user")

        self._show(format_event(msg))

    def _on_disconnect(self):
        self._show(["Connection to the server was lost."])

    # ---- helpers ------------------------------------------------------------

    def _send(self, request_type, **fields):
        """Used for sending anything to the server from the CLI"""
        try:
            self.client.request(request_type, **fields)
        except ConnectionError as exc:
            print(exc)

    def _need_username(self, given):
        if given:
            return given
        if self.username:
            return self.username
        return input("Choose a username: ").strip()

    def _parse_id(self, text):
        try:
            return int(text)
        except ValueError:
            print("Message id must be a number.")
            return None

    # ---- commands -----------------------------------------------------------

    def cmd_help(self, rest):
        print(HELP)

    def cmd_connect(self, rest):
        parts = rest.split()

        if len(parts) != 2 or not parts[1].isdigit():
            print("Usage: %connect <host> <port>")
            return
        try:
            self.client.connect(parts[0], int(parts[1]))
        except (OSError, ConnectionError) as exc:
            print(f"Could not connect: {exc}")

    def cmd_exit(self, rest):
        self.client.close()
        return False

    def cmd_join(self, rest):
        parts = rest.split()

        if len(parts) > 1:
            print("Usage: %join [username]   (usernames cannot contain spaces)")
            return

        self._send("join", group=PUBLIC_GROUP, user=self._need_username(rest.strip()))

    def cmd_post(self, rest):
        parsed = split_subject_body(rest)

        if parsed is None:
            print("Usage: %post <subject> <body>   e.g. %post \"Lunch plans\" Pizza at noon?")
            return

        self._send("post", group=PUBLIC_GROUP, subject=parsed[0], body=parsed[1])

    def cmd_users(self, rest):
        self._send("users", group=PUBLIC_GROUP)

    def cmd_message(self, rest):
        message_id = self._parse_id(rest.strip())

        if message_id is None or not rest.strip():
            print("Usage: %message <id>")
            return

        self._send("message", group=PUBLIC_GROUP, id=message_id)

    def cmd_leave(self, rest):
        self._send("leave", group=PUBLIC_GROUP)

    def cmd_groups(self, rest):
        self._send("groups")

    def cmd_groupjoin(self, rest):
        parts = rest.split()

        if not 1 <= len(parts) <= 2:
            print("Usage: %groupjoin <group id|name> [username]")
            return

        user = self._need_username(parts[1] if len(parts) == 2 else "")
        self._send("join", group=parts[0], user=user)

    def cmd_grouppost(self, rest):
        group, _, tail = rest.strip().partition(" ")
        parsed = split_subject_body(tail)

        if not group or parsed is None:
            print("Usage: %grouppost <group> <subject> <body>")
            return

        self._send("post", group=group, subject=parsed[0], body=parsed[1])

    def cmd_groupusers(self, rest):
        if len(rest.split()) != 1:
            print("Usage: %groupusers <group id|name>")
            return

        self._send("users", group=rest.strip())

    def cmd_groupmessage(self, rest):
        parts = rest.split()

        if len(parts) != 2:
            print("Usage: %groupmessage <group id|name> <message id>")
            return

        message_id = self._parse_id(parts[1])

        if message_id is not None:
            self._send("message", group=parts[0], id=message_id)

    def cmd_groupleave(self, rest):
        if len(rest.split()) != 1:
            print("Usage: %groupleave <group id|name>")
            return

        self._send("leave", group=rest.strip())

    # ---- main loop ----------------------------------------------------------

    def handle(self, line):
        """Run one input line.  Returns False when the client should quit."""
        line = line.strip()

        if not line:
            return True
        if not line.startswith("%"):
            print("Commands start with '%'. Type %help for a list.")
            return True

        name, _, rest = line.partition(" ")
        command = self.commands.get(name.lower())

        if command is None:
            print(f"Unknown command '{name}'. Type %help for a list.")
            return True

        return command(rest) is not False

    def run(self):
        """Starts the terminal"""
        print("Bulletin Board client. Type %help for commands.")
        try:
            while True:
                try:
                    line = input("> ")
                except EOFError:
                    break
                if not self.handle(line):
                    break
        except KeyboardInterrupt:
            print()
        finally:
            self.client.close()


# ---------- GUI CODE ----------

class BoardState:
    """What this client currently knows: boards, memberships, users, messages."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.groups = [PUBLIC_GROUP]   # board names, public first
        self.joined = {}               # group -> username used there
        self.users = {}                # group -> list of usernames
        self.messages = {}             # group -> list of message headers

    def _add_message(self, group, header):
        if group in self.joined:
            known = self.messages.setdefault(group, [])
            if all(m["id"] != header["id"] for m in known):
                known.append(header)

    def apply(self, msg):
        """
        Update the model from a server message.

        Returns the full message dict if this was the answer to a message
        retrieval request (so the UI can display its body), otherwise None.
        """
        kind = msg.get("type")

        if kind == "welcome":
            self.groups = [PUBLIC_GROUP] + [g["name"] for g in msg.get("groups", [])]

        elif kind == "response" and msg.get("status") == "ok":
            cmd, group = msg.get("cmd"), msg.get("group")
            if cmd == "join":
                self.joined[group] = msg["user"]
                self.users[group] = list(msg.get("users", []))
                self.messages[group] = list(msg.get("history", []))
            elif cmd == "leave":
                self.joined.pop(group, None)
                self.users.pop(group, None)
                self.messages.pop(group, None)
            elif cmd == "post":
                self._add_message(group, msg["message"])
            elif cmd == "users":
                self.users[group] = list(msg.get("users", []))
            elif cmd == "groups":
                names = [g["name"] for g in msg.get("groups", [])]
                self.groups = [PUBLIC_GROUP] + names
            elif cmd == "message":
                return msg["message"]

        elif kind == "notify":
            group, event = msg.get("group"), msg.get("event")
            if event == "user_joined":
                users = self.users.setdefault(group, [])
                if msg["user"] not in users:
                    users.append(msg["user"])
            elif event == "user_left":
                users = self.users.get(group, [])
                if msg["user"] in users:
                    users.remove(msg["user"])
            elif event == "new_message":
                self._add_message(group, msg["message"])

        return None


class BulletinGUI:
    """
    GUI built in Tkinter and uses same functions the CLI uses for sending packets to the server.
    """

    POLL_MS = 100

    def __init__(self, root, host="127.0.0.1", port=DEFAULT_PORT):
        self.root = root
        root.title("Bulletin Board")
        root.geometry("980x680")
        root.minsize(820, 560)

        self.events = queue.Queue() # network thread -> GUI thread
        self.state = BoardState()
        self.client = Client(
            on_message=lambda msg: self.events.put(("msg", msg)),
            on_disconnect=lambda: self.events.put(("lost", None)))
        self.current_group = None   # board shown in the center

        self.host_var = tk.StringVar(value=host)
        self.port_var = tk.StringVar(value=str(port))
        self.user_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Not connected")
        self.subject_var = tk.StringVar()

        self._build_widgets()
        self._set_controls_enabled(False)
        root.protocol("WM_DELETE_WINDOW", self.on_close)
        root.after(self.POLL_MS, self._poll)

    # ---- layout ---------------------------------------------------------------

    def _build_widgets(self):
        top = ttk.Frame(self.root, padding=(8, 8, 8, 4))
        top.pack(fill="x")
        ttk.Label(top, text="Host").pack(side="left")
        ttk.Entry(top, textvariable=self.host_var, width=16).pack(side="left", padx=(4, 10))
        ttk.Label(top, text="Port").pack(side="left")
        ttk.Entry(top, textvariable=self.port_var, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(top, text="Username").pack(side="left")
        ttk.Entry(top, textvariable=self.user_var, width=14).pack(side="left", padx=(4, 10))
        self.connect_button = ttk.Button(top, text="Connect", command=self.toggle_connection)
        self.connect_button.pack(side="left")
        ttk.Label(top, textvariable=self.status_var, foreground="#555").pack(side="right")

        body = ttk.Frame(self.root, padding=(8, 4))
        body.pack(fill="both", expand=True)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        # Left: board list + join / leave
        left = ttk.Frame(body)
        left.grid(row=0, column=0, sticky="ns", padx=(0, 8))
        ttk.Label(left, text="Boards  (* = joined)").pack(anchor="w")
        self.group_list = tk.Listbox(left, width=20, exportselection=False, activestyle="none")
        self.group_list.pack(fill="y", expand=True, pady=4)
        self.group_list.bind("<<ListboxSelect>>", self.on_group_select)
        self.join_button = ttk.Button(left, text="Join", command=self.join_selected)
        self.join_button.pack(fill="x", pady=1)
        self.leave_button = ttk.Button(left, text="Leave", command=self.leave_selected)
        self.leave_button.pack(fill="x", pady=1)
        self.refresh_button = ttk.Button(left, text="Refresh users",
                                         command=self.refresh_users)
        self.refresh_button.pack(fill="x", pady=1)

        # Center: message table + message body viewer
        center = ttk.Frame(body)
        center.grid(row=0, column=1, sticky="nsew")
        center.rowconfigure(1, weight=1)
        center.rowconfigure(3, weight=1)
        center.columnconfigure(0, weight=1)
        self.board_label = ttk.Label(center, text="Select a board", font=("TkDefaultFont", 11, "bold"))
        self.board_label.grid(row=0, column=0, sticky="w")

        columns = ("id", "sender", "date", "subject")
        self.tree = ttk.Treeview(center, columns=columns, show="headings", height=8,
                                 selectmode="browse")
        for name, title, width in (("id", "ID", 50), ("sender", "Sender", 120),
                                   ("date", "Post Date", 150), ("subject", "Subject", 300)):
            self.tree.heading(name, text=title)
            self.tree.column(name, width=width, anchor="w", stretch=(name == "subject"))
        self.tree.grid(row=1, column=0, sticky="nsew", pady=4)
        tree_scroll = ttk.Scrollbar(center, orient="vertical", command=self.tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=tree_scroll.set)
        self.tree.bind("<Double-1>", self.open_selected_message)

        ttk.Label(center, text="Message (double-click a row above to open it)").grid(
            row=2, column=0, sticky="w")
        self.message_text = self._readonly_text(center, height=6)
        self.message_text[0].grid(row=3, column=0, sticky="nsew", pady=4)
        self.message_text[1].grid(row=3, column=1, sticky="ns")

        # Right: users in the selected board
        right = ttk.Frame(body)
        right.grid(row=0, column=2, sticky="ns", padx=(8, 0))
        ttk.Label(right, text="Users").pack(anchor="w")
        self.user_list = tk.Listbox(right, width=18, activestyle="none", exportselection=False)
        self.user_list.pack(fill="y", expand=True, pady=4)

        # Compose
        compose = ttk.LabelFrame(self.root, text="New message (posts to the selected board)",
                                 padding=6)
        compose.pack(fill="x", padx=8, pady=4)
        compose.columnconfigure(1, weight=1)
        ttk.Label(compose, text="Subject").grid(row=0, column=0, sticky="w")
        self.subject_entry = ttk.Entry(compose, textvariable=self.subject_var)
        self.subject_entry.grid(row=0, column=1, sticky="ew", padx=6, pady=2)
        ttk.Label(compose, text="Body").grid(row=1, column=0, sticky="nw")
        self.body_text = tk.Text(compose, height=3, wrap="word")
        self.body_text.grid(row=1, column=1, sticky="ew", padx=6, pady=2)
        self.post_button = ttk.Button(compose, text="Post", command=self.post_message)
        self.post_button.grid(row=0, column=2, rowspan=2, sticky="ns")

        # Activity log
        log_frame = ttk.LabelFrame(self.root, text="Activity", padding=4)
        log_frame.pack(fill="both", padx=8, pady=(0, 8))
        self.log_text = self._readonly_text(log_frame, height=6)
        self.log_text[0].pack(side="left", fill="both", expand=True)
        self.log_text[1].pack(side="right", fill="y")

    def _readonly_text(self, parent, height):
        text = tk.Text(parent, height=height, wrap="word", state="disabled")
        scroll = ttk.Scrollbar(parent, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=scroll.set)
        return text, scroll

    def _set_controls_enabled(self, enabled):
        state = "normal" if enabled else "disabled"

        for widget in (self.join_button, self.leave_button, self.refresh_button,
                       self.post_button):
            widget.configure(state=state)

    # ---- helpers ----------------------------------------------------------

    def _write(self, text_pair, content, replace=False):
        widget = text_pair[0]
        widget.configure(state="normal")

        if replace:
            widget.delete("1.0", "end")

        widget.insert("end", content)
        widget.see("end" if not replace else "1.0")
        widget.configure(state="disabled")

    def log(self, lines):
        self._write(self.log_text, "\n".join(lines) + "\n")

    def _send(self, request_type, **fields):
        try:
            self.client.request(request_type, **fields)
            return True
        except ConnectionError as exc:
            messagebox.showerror("Bulletin Board", str(exc))
            return False

    def _selected_group(self):
        if self.current_group is None:
            messagebox.showinfo("Bulletin Board", "Select a board from the list first.")

        return self.current_group

    # ---- connection -------------------------------------------------------------

    def toggle_connection(self):
        if self.client.is_connected():
            self.client.close()
            self._after_disconnect("Disconnected")
            return

        try:
            port = int(self.port_var.get())
        except ValueError:
            messagebox.showerror("Bulletin Board", "Port must be a number.")
            return

        try:
            self.client.connect(self.host_var.get().strip(), port)
        except (OSError, ConnectionError) as exc:
            messagebox.showerror("Bulletin Board", f"Could not connect: {exc}")
            return

        self.status_var.set(f"Connected to {self.host_var.get().strip()}:{port}")
        self.connect_button.configure(text="Disconnect")
        self._set_controls_enabled(True)

    def _after_disconnect(self, status):
        self.state.reset()
        self.current_group = None
        self.status_var.set(status)
        self.connect_button.configure(text="Connect")
        self._set_controls_enabled(False)
        self.refresh_all()

    # ---- user actions -----------------------------------------------------------

    def join_selected(self):
        group = self._selected_group()

        if group is None:
            return

        username = self.user_var.get().strip()

        if not username:
            messagebox.showinfo("Bulletin Board", "Enter a username first.")
            return

        self._send("join", group=group, user=username)

    def leave_selected(self):
        group = self._selected_group()

        if group is not None:
            self._send("leave", group=group)

    def refresh_users(self):
        group = self._selected_group()

        if group is not None:
            self._send("users", group=group)

    def post_message(self):
        group = self._selected_group()

        if group is None:
            return

        subject = self.subject_var.get().strip()
        body = self.body_text.get("1.0", "end").strip()

        if not subject or not body:
            messagebox.showinfo("Bulletin Board", "Both a subject and a body are required.")
            return

        self._send("post", group=group, subject=subject, body=body)

    def open_selected_message(self, _event=None):
        group, selection = self.current_group, self.tree.selection()

        if group is None or not selection:
            return

        message_id = int(self.tree.item(selection[0], "values")[0])

        self._send("message", group=group, id=message_id)

    def on_group_select(self, _event=None):
        selection = self.group_list.curselection()

        if selection:
            self.current_group = self.state.groups[selection[0]]
            self.refresh_group_view()

    def on_close(self):
        self.client.close()
        self.root.destroy()

    # ---- handling server messages (GUI thread only) ------------------------------

    def _poll(self):
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "msg":
                    self.handle_message(payload)
                else:
                    self.log(["Connection to the server was lost."])
                    self._after_disconnect("Connection lost")
        except queue.Empty:
            pass

        self.root.after(self.POLL_MS, self._poll)

    def handle_message(self, msg):
        opened = self.state.apply(msg)
        is_response = msg.get("type") == "response"

        if is_response and msg.get("status") != "ok":
            self.log(format_event(msg, hints=False))
            messagebox.showerror("Bulletin Board", msg.get("reason", "Unknown error"))
        elif opened is not None:
            self.show_message(opened)
        else:
            self.log(format_event(msg, hints=False))
            if is_response and msg.get("cmd") == "join":
                self.current_group = msg["group"]   # jump to the board just joined
            elif is_response and msg.get("cmd") == "post":
                self.subject_var.set("")
                self.body_text.delete("1.0", "end")

        self.refresh_all()

    def show_message(self, message):
        text = (f"From: {message['sender']}\nDate: {message['date']}\n"
                f"Subject: {message['subject']}\n\n{message['body']}")
        self._write(self.message_text, text, replace=True)

    # ---- redrawing ----------------------------------------------------------------

    def refresh_all(self):
        self.group_list.delete(0, "end")

        for name in self.state.groups:
            marker = "* " if name in self.state.joined else "   "
            self.group_list.insert("end", marker + name)

        if self.current_group in self.state.groups:
            index = self.state.groups.index(self.current_group)
            self.group_list.selection_set(index)
            self.group_list.see(index)
        else:
            self.current_group = None

        self.refresh_group_view()

    def refresh_group_view(self):
        group = self.current_group
        self.tree.delete(*self.tree.get_children())
        self.user_list.delete(0, "end")

        if group is None:
            self.board_label.configure(text="Select a board")
            return

        joined_as = self.state.joined.get(group)
        status = f"joined as {joined_as}" if joined_as else "not joined"
        self.board_label.configure(text=f"{group}  -  {status}")

        for m in self.state.messages.get(group, []):
            self.tree.insert("", "end", values=(m["id"], m["sender"], m["date"], m["subject"]))

        self.tree.yview_moveto(1.0)

        for user in self.state.users.get(group, []):
            self.user_list.insert("end", user)

def run_gui(host=DEFAULT_HOST, port=DEFAULT_PORT):
    """Start the tkinter GUI (tkinter is imported here so the CLI never needs it)."""
    global tk, ttk, messagebox

    try:
        import tkinter as tk
        from tkinter import ttk, messagebox
    except ImportError:
        print("tkinter is not installed, so the GUI is unavailable.\n")
        return

    root = tk.Tk()
    BulletinGUI(root, host, port)
    root.mainloop()


def main():
    args = sys.argv[1:]
    use_gui = bool(args) and args[0] == "--gui"

    if use_gui:
        args = args[1:]

    if len(args) not in (0, 2) or (args and not args[1].isdigit()):
        print("Usage: python3 client.py [--gui] [host port]")
        return 1

    if use_gui:
        run_gui(*([args[0], int(args[1])] if args else []))
        return 0

    cli = BulletinCLI()

    if args:
        cli.cmd_connect(f"{args[0]} {args[1]}")

    cli.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())