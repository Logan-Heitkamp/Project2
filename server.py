"""Bulletin board server (Parts 1 and 2) - a single self-contained file.

Usage:  python3 server.py [--host ADDRESS] [--port PORT]      (default port 5050)

Wire format
-----------
TCP only gives us an ordered stream of bytes, so every protocol message is one
JSON object encoded as UTF-8 and terminated by a newline.  ``json.dumps`` always
escapes newlines inside strings, so a raw newline only ever means "end of
message".  See README.md for the full field-by-field protocol specification.

Concurrency design
------------------
* One *reader* thread per client reads requests from that client's socket.
* One *writer* thread per client drains an outbound queue onto the socket.
  Every send to a client (replies and broadcasts) is just ``queue.put``, so a
  slow or dead client can never block a broadcast, and two threads can never
  interleave bytes on the same socket.
* All shared state (groups, members, messages) is guarded by one lock.
  Replies are queued while that lock is held, which guarantees a client sees
  events in the same order the server applied them.

Boards
------
Group 0 is the public board used by Part 1 (name "public").  Groups 1-5 are the
private boards offered to Part 2 clients.  Every board has its own member list,
message list and message ID counter, so boards are fully isolated.
"""

import argparse
import datetime
import json
import logging
import queue
import re
import socket
import threading
from collections import OrderedDict

# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

DEFAULT_PORT = 5050            # non-system port (>1023)
ENCODING = "utf-8"
MAX_LINE_BYTES = 256 * 1024    # reject absurdly long lines instead of buffering forever
PUBLIC_GROUP = "public"        # the single public board used by Part 1
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
HISTORY_ON_JOIN = 2            # how many recent messages a new member sees

MAX_USERNAME_LEN = 32
MAX_SUBJECT_LEN = 100
MAX_BODY_LEN = 8000


class ProtocolError(Exception):
    """Raised when incoming bytes cannot be turned into a protocol message.

    ``fatal`` means the stream can no longer be trusted (e.g. a line longer
    than MAX_LINE_BYTES) and the connection should be closed.
    """

    def __init__(self, reason, fatal=False):
        super().__init__(reason)
        self.reason = reason
        self.fatal = fatal


def encode(message):
    """Serialize a dict to one newline-terminated UTF-8 JSON line."""
    return (json.dumps(message, separators=(",", ":")) + "\n").encode(ENCODING)


def decode(line):
    """Parse one received line (bytes) into a dict."""
    try:
        obj = json.loads(line.decode(ENCODING))
    except (UnicodeDecodeError, ValueError):
        raise ProtocolError("malformed JSON") from None
    if not isinstance(obj, dict):
        raise ProtocolError("message must be a JSON object")
    return obj


def read_message(rfile):
    """Read the next message from a buffered binary file object.

    Returns ``None`` when the peer has closed the connection.  Blank lines are
    skipped.  Raises ProtocolError for undecodable or oversized lines.
    """
    while True:
        line = rfile.readline(MAX_LINE_BYTES + 1)
        if not line:
            return None
        if len(line) > MAX_LINE_BYTES and not line.endswith(b"\n"):
            raise ProtocolError("message too long", fatal=True)
        if line.strip():
            return decode(line)


def ok_response(cmd, **fields):
    """Build a successful response to request type ``cmd``."""
    return {"type": "response", "cmd": cmd, "status": "ok", **fields}


def error_response(cmd, reason, **fields):
    """Build an error response to request type ``cmd``."""
    return {"type": "response", "cmd": cmd, "status": "error",
            "reason": reason, **fields}


def notification(event, **fields):
    """Build an unsolicited server notification."""
    return {"type": "notify", "event": event, **fields}


# ---- server ----

log = logging.getLogger("bulletin.server")

PRIVATE_GROUP_NAMES = ["general", "sports", "music", "tech", "games"]  # ids 1..5
USERNAME_RE = re.compile(r"[A-Za-z0-9_.\-]{1,%d}" % MAX_USERNAME_LEN)


class Message:
    """A single posted message."""

    def __init__(self, message_id, sender, date, subject, body):
        self.id = message_id
        self.sender = sender
        self.date = date
        self.subject = subject
        self.body = body

    def header(self):
        """Fields shown in listings: ID, sender, post date, subject."""
        return {"id": self.id, "sender": self.sender,
                "date": self.date, "subject": self.subject}

    def full(self):
        """Header plus the message body (returned when a message is requested)."""
        return {**self.header(), "body": self.body}


class Group:
    """One message board with its own members and messages."""

    def __init__(self, group_id, name):
        self.id = group_id
        self.name = name
        self.members = OrderedDict()   # ClientHandler -> username (join order)
        self.messages = []             # Message objects; message id == index + 1

    def username_taken(self, username):
        wanted = username.lower()
        return any(name.lower() == wanted for name in self.members.values())

    def info(self):
        return {"id": self.id, "name": self.name, "members": len(self.members)}


class ClientHandler(threading.Thread):
    """Serves one connected client (reader thread + a helper writer thread)."""

    def __init__(self, server, sock, address):
        super().__init__(daemon=True, name="client-%s:%s" % address[:2])
        self.server = server
        self.sock = sock
        self.label = "%s:%s" % address[:2]
        self.outbox = queue.Queue()
        self.memberships = {}        # group id -> username (guarded by server.lock)
        self.first_visible = {}      # group id -> lowest message id this member may read
        self.disconnected = False    # guarded by server.lock

    def send(self, message):
        """Queue a message for this client (never blocks).  None closes it."""
        self.outbox.put(message)

    def _writer(self):
        try:
            while True:
                message = self.outbox.get()
                if message is None:
                    break
                self.sock.sendall(encode(message))
        except OSError:
            pass          # peer vanished; the reader thread will notice and clean up
        finally:
            for action in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
                try:
                    action()
                except OSError:
                    pass

    def run(self):
        threading.Thread(target=self._writer, daemon=True).start()
        rfile = self.sock.makefile("rb")
        try:
            self.send(self.server.welcome_message())
            while True:
                try:
                    request = read_message(rfile)
                except ProtocolError as exc:
                    self.send(error_response("error", exc.reason))
                    if exc.fatal:
                        break
                    continue
                if request is None:       # client closed the connection
                    break
                if self.server.handle_request(self, request):
                    break                 # client sent "exit"
        except OSError:
            pass                          # connection reset etc.
        finally:
            self.server.disconnect(self)
            try:
                rfile.close()
            except OSError:
                pass


class BulletinServer:
    """Listens for clients and implements all bulletin board commands."""

    def __init__(self, host="0.0.0.0", port=DEFAULT_PORT):
        self.host = host
        self.port = port
        self.lock = threading.RLock()
        self.groups = OrderedDict()       # id -> Group
        self.groups_by_name = {}          # lowercase name -> Group
        self._add_group(0, PUBLIC_GROUP)
        for number, name in enumerate(PRIVATE_GROUP_NAMES, start=1):
            self._add_group(number, name)
        self.handlers = set()
        self._listener = None
        self._accept_thread = None
        self._stopping = threading.Event()
        self._commands = {
            "join": self._cmd_join,
            "leave": self._cmd_leave,
            "post": self._cmd_post,
            "users": self._cmd_users,
            "message": self._cmd_message,
            "groups": self._cmd_groups,
            "exit": self._cmd_exit,
        }

    # ---- setup / lifecycle -------------------------------------------------

    def _add_group(self, group_id, name):
        group = Group(group_id, name)
        self.groups[group_id] = group
        self.groups_by_name[name.lower()] = group

    def start(self):
        """Bind, listen and start accepting clients in a background thread."""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen(50)
        listener.settimeout(0.5)          # lets the accept loop notice stop()
        self.port = listener.getsockname()[1]   # resolves port 0 for tests
        self._listener = listener
        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="accept")
        self._accept_thread.start()
        log.info("Bulletin board server listening on %s:%d", self.host, self.port)

    def serve_forever(self):
        self.start()
        try:
            while self._accept_thread.is_alive():
                self._accept_thread.join(0.5)
        except KeyboardInterrupt:
            log.info("Shutting down")
        finally:
            self.stop()

    def stop(self):
        self._stopping.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        for handler in list(self.handlers):
            self.disconnect(handler)

    def _accept_loop(self):
        while not self._stopping.is_set():
            try:
                conn, address = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(None)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            handler = ClientHandler(self, conn, address)
            with self.lock:
                self.handlers.add(handler)
            log.info("Client connected: %s", handler.label)
            handler.start()

    # ---- helpers (call with self.lock held unless noted) -------------------

    def welcome_message(self):
        with self.lock:
            return {"type": "welcome", "public_group": PUBLIC_GROUP,
                    "groups": [g.info() for g in self.groups.values() if g.id != 0]}

    def resolve_group(self, ref):
        """Find a group by numeric id or (case-insensitive) name."""
        if isinstance(ref, bool):
            return None
        if isinstance(ref, int):
            return self.groups.get(ref)
        if isinstance(ref, str):
            ref = ref.strip().lower()
            if ref.isdigit():
                return self.groups.get(int(ref))
            return self.groups_by_name.get(ref)
        return None

    def _notify(self, group, notification, exclude=None):
        for member in list(group.members):
            if member is not exclude:
                member.send(notification)

    def _member_context(self, handler, request, cmd):
        """Resolve the group and verify the sender joined it.

        Returns (group, username, error_reply); error_reply is None on success.
        """
        group = self.resolve_group(request.get("group", PUBLIC_GROUP))
        if group is None:
            return None, None, error_response(cmd, "unknown group")
        username = handler.memberships.get(group.id)
        if username is None:
            return group, None, error_response(
                cmd, "you have not joined group '%s'" % group.name, group=group.name)
        return group, username, None

    def disconnect(self, handler):
        """Remove a client from every group and close its connection (idempotent)."""
        with self.lock:
            if handler.disconnected:
                return
            handler.disconnected = True
            self.handlers.discard(handler)
            for group_id, username in list(handler.memberships.items()):
                group = self.groups[group_id]
                group.members.pop(handler, None)
                self._notify(group, notification(
                    "user_left", group=group.name, user=username), exclude=handler)
            handler.memberships.clear()
            handler.first_visible.clear()
            handler.send(None)           # writer thread flushes, then closes the socket
        log.info("Client disconnected: %s", handler.label)

    # ---- request dispatch --------------------------------------------------

    def handle_request(self, handler, request):
        """Process one request.  Returns True if the connection should close."""
        cmd = request.get("type")
        if not isinstance(cmd, str) or cmd not in self._commands:
            handler.send(error_response(str(cmd)[:40], "unknown command"))
            return False
        with self.lock:
            try:
                reply = self._commands[cmd](handler, request)
            except Exception:                      # keep the server alive no matter what
                log.exception("Error handling %r from %s", cmd, handler.label)
                reply = error_response(cmd, "internal server error")
            handler.send(reply)
        return cmd == "exit"

    # ---- commands ----------------------------------------------------------

    def _cmd_join(self, handler, request):
        group = self.resolve_group(request.get("group", PUBLIC_GROUP))
        if group is None:
            return error_response("join", "unknown group")
        username = request.get("user")
        if not isinstance(username, str) or not USERNAME_RE.fullmatch(username):
            return error_response(
                "join", "invalid username (1-%d letters, digits, '_', '-' or '.')"
                % MAX_USERNAME_LEN, group=group.name)
        if group.id in handler.memberships:
            return error_response(
                "join", "you are already in group '%s'" % group.name, group=group.name)
        if group.username_taken(username):
            return error_response(
                "join", "username '%s' is already taken in group '%s'"
                % (username, group.name), group=group.name)
        group.members[handler] = username
        handler.memberships[group.id] = username
        self._notify(group, notification(
            "user_joined", group=group.name, user=username), exclude=handler)
        # A new member may read only the last HISTORY_ON_JOIN messages that already
        # existed (the history sent below) plus everything posted after joining.
        first_visible = max(1, len(group.messages) - HISTORY_ON_JOIN + 1)
        handler.first_visible[group.id] = first_visible
        history = [m.header() for m in group.messages[first_visible - 1:]]
        log.info("%s joined '%s' as %s", handler.label, group.name, username)
        return ok_response("join", group=group.name, group_id=group.id,
                             user=username, users=list(group.members.values()),
                             history=history)

    def _cmd_leave(self, handler, request):
        group, username, error = self._member_context(handler, request, "leave")
        if error:
            return error
        del group.members[handler]
        del handler.memberships[group.id]
        del handler.first_visible[group.id]
        self._notify(group, notification(
            "user_left", group=group.name, user=username))
        log.info("%s left '%s'", handler.label, group.name)
        return ok_response("leave", group=group.name, user=username)

    def _cmd_users(self, handler, request):
        group, _, error = self._member_context(handler, request, "users")
        if error:
            return error
        return ok_response("users", group=group.name,
                             users=list(group.members.values()))

    def _cmd_post(self, handler, request):
        group, username, error = self._member_context(handler, request, "post")
        if error:
            return error
        subject, body = request.get("subject"), request.get("body")
        if not isinstance(subject, str) or not subject.strip():
            return error_response("post", "a subject is required", group=group.name)
        if not isinstance(body, str) or not body.strip():
            return error_response("post", "a message body is required", group=group.name)
        if len(subject) > MAX_SUBJECT_LEN:
            return error_response(
                "post", "subject too long (max %d characters)" % MAX_SUBJECT_LEN,
                group=group.name)
        if len(body) > MAX_BODY_LEN:
            return error_response(
                "post", "body too long (max %d characters)" % MAX_BODY_LEN,
                group=group.name)
        message = Message(len(group.messages) + 1, username,
                          datetime.datetime.now().strftime(DATE_FORMAT),
                          subject.strip(), body)
        group.messages.append(message)
        self._notify(group, notification(
            "new_message", group=group.name, message=message.header()), exclude=handler)
        log.info("%s posted message %d in '%s'", username, message.id, group.name)
        return ok_response("post", group=group.name, message=message.header())

    def _cmd_message(self, handler, request):
        group, _, error = self._member_context(handler, request, "message")
        if error:
            return error
        message_id = request.get("id")
        if isinstance(message_id, str) and message_id.strip().isdigit():
            message_id = int(message_id)
        if isinstance(message_id, bool) or not isinstance(message_id, int):
            return error_response("message", "message id must be a number",
                                    group=group.name)
        if not 1 <= message_id <= len(group.messages):
            return error_response(
                "message", "no message with id %d in group '%s'"
                % (message_id, group.name), group=group.name)
        if message_id < handler.first_visible[group.id]:
            return error_response(
                "message", "message %d was posted before you joined '%s' and cannot be read"
                % (message_id, group.name), group=group.name)
        return ok_response("message", group=group.name,
                             message=group.messages[message_id - 1].full())

    def _cmd_groups(self, handler, request):
        return ok_response("groups", groups=[
            g.info() for g in self.groups.values() if g.id != 0])

    def _cmd_exit(self, handler, request):
        return ok_response("exit")


def main():
    parser = argparse.ArgumentParser(description="Bulletin board server")
    parser.add_argument("--host", default="0.0.0.0", help="address to bind (default: all)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="port to listen on (default: %d)" % DEFAULT_PORT)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    BulletinServer(args.host, args.port).serve_forever()


if __name__ == "__main__":
    main()
