# Pure Python - there is nothing to compile.  These targets are shortcuts.
#   make server      start the server on PORT (default 5050)
#   make client      start the command-line client
#   make gui         start the tkinter GUI client
#   make clean       remove __pycache__ folders
# Override defaults with e.g.:  make client HOST=192.168.1.20 PORT=6000

PYTHON ?= python3
HOST   ?= 127.0.0.1
PORT   ?= 5050

.PHONY: server client gui clean

server:
	$(PYTHON) server.py --port $(PORT)

client:
	$(PYTHON) client.py $(HOST) $(PORT)

gui:
	$(PYTHON) client.py --gui $(HOST) $(PORT)

clean:
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
