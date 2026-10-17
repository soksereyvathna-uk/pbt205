"""
Task 1 - Chat GUI with room selection and chat history (PBT205)

Run:  python chat_gui.py
Needs: pip install pika   (Tkinter is built into Python)

Design notes
- One topic exchange "chat" (group contract); the routing key is "room.<name>".
- Each room has a RabbitMQ *stream* called "history.<room>" bound to that routing key.
  Streams are non-destructive logs, so every user who joins reads the room's
  messages from the start (history) and then keeps receiving new ones live.
"""
import re
import json
import time
import queue
import threading
import tkinter as tk
from tkinter import messagebox

import pika

from mq_common import declare_topic, publish  

EXCHANGE = "chat"
HOST = "localhost"
ROOMS = ["general", "study", "random"]   # quick-pick rooms; users can type any room name

# ---------- Theme ----------
BG = "#14151a"
PANEL = "#1c1d25"
INPUT = "#2a2c38"
ACCENT = "#6366f1"
ACCENT_HOVER = "#4f52d9"
TEXT = "#e8e8f0"
MUTED = "#8b8da0"
GREEN = "#34d399"
BUBBLE_OTHER = "#2a2c38"
BUBBLE_ME = "#5457e0"
NAME_COLORS = ["#f87171", "#fbbf24", "#34d399", "#38bdf8", "#c084fc", "#fb7185", "#2dd4bf"]
FONT = "Segoe UI"


def clean_room(name):
    """Lower-case and remove spaces/special characters so 'PBT 205' and 'pbt205' match."""
    return re.sub(r"[^a-z0-9_-]", "", name.lower())


def hover(button, normal, over):
    button.bind("<Enter>", lambda e: button.config(bg=over))
    button.bind("<Leave>", lambda e: button.config(bg=normal))


class ChatApp:
    def __init__(self, root):
        self.root = root
        root.title("PBT205 Chat")
        root.geometry("460x680")
        root.minsize(380, 520)
        root.configure(bg=BG)
        self.incoming = queue.Queue()    # messages from the listener thread -> GUI
        self.username = None
        self.room = None
        self.pub_conn = None
        self.pub_ch = None
        self.frame = None
        self.last_day = None
        self.last_user = None
        self.last_ts = 0
        self.user_tags = set()
        self.build_login()
        root.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- Login screen ----------
    def build_login(self):
        self.frame = tk.Frame(self.root, bg=BG)
        self.frame.pack(fill="both", expand=True)

        card = tk.Frame(self.frame, bg=PANEL, padx=30, pady=28)
        card.place(relx=0.5, rely=0.5, anchor="center", width=360)

        logo = tk.Canvas(card, width=56, height=56, bg=PANEL, highlightthickness=0)
        logo.create_oval(2, 2, 54, 54, fill=ACCENT, outline="")
        logo.create_text(28, 28, text="#", fill="white", font=(FONT, 22, "bold"))
        logo.pack()

        tk.Label(card, text="PBT205 Chat", bg=PANEL, fg=TEXT,
                 font=(FONT, 18, "bold")).pack(pady=(10, 0))
        tk.Label(card, text="Sign in and pick a room", bg=PANEL, fg=MUTED,
                 font=(FONT, 10)).pack(pady=(0, 16))

        self.user_var = tk.StringVar()
        self.port_var = tk.StringVar(value="5672")
        self.room_var = tk.StringVar(value=ROOMS[0])

        user_entry = self.field(card, "Username", self.user_var)
        self.field(card, "Port", self.port_var)
        self.field(card, "Room", self.room_var)

        # Quick-pick room chips
        chips = tk.Frame(card, bg=PANEL)
        chips.pack(fill="x", pady=(2, 14))
        self.chip_buttons = {}
        for r in ROOMS:
            b = tk.Button(chips, text=f"# {r}", relief="flat", bd=0, cursor="hand2",
                          font=(FONT, 9), padx=10, pady=3,
                          command=lambda r=r: self.pick_room(r))
            b.pack(side="left", padx=(0, 6))
            self.chip_buttons[r] = b
        self.room_var.trace_add("write", lambda *a: self.refresh_chips())
        self.refresh_chips()

        join = tk.Button(card, text="Join room", command=self.join, bg=ACCENT, fg="white",
                         activebackground=ACCENT_HOVER, activeforeground="white",
                         relief="flat", bd=0, cursor="hand2", font=(FONT, 11, "bold"),
                         pady=8)
        join.pack(fill="x")
        hover(join, ACCENT, ACCENT_HOVER)

        user_entry.focus()
        self.root.bind("<Return>", lambda e: self.join() if self.username is None else None)

    def field(self, parent, label, var):
        tk.Label(parent, text=label, bg=PANEL, fg=MUTED, font=(FONT, 9),
                 anchor="w").pack(fill="x")
        e = tk.Entry(parent, textvariable=var, bg=INPUT, fg=TEXT, insertbackground=TEXT,
                     relief="flat", font=(FONT, 11), highlightthickness=1,
                     highlightbackground=INPUT, highlightcolor=ACCENT)
        e.pack(fill="x", pady=(2, 10), ipady=6)
        return e

    def pick_room(self, room):
        self.room_var.set(room)

    def refresh_chips(self):
        current = clean_room(self.room_var.get())
        for r, b in self.chip_buttons.items():
            if r == current:
                b.config(bg=ACCENT, fg="white", activebackground=ACCENT_HOVER)
            else:
                b.config(bg=INPUT, fg=MUTED, activebackground=INPUT)

    # ---------- Join: connect to RabbitMQ ----------
    def join(self):
        user = self.user_var.get().strip()
        raw_room = self.room_var.get().strip()
        room = clean_room(raw_room)
        if not user or not room:
            messagebox.showerror("Missing info",
            "Enter a username and a room (letters, numbers, - or _).")
            return
        try:
            port = int(self.port_var.get())
        except ValueError:
            messagebox.showerror("Bad port", "Port must be a number, e.g. 5672.")
            return
        if room != raw_room.lower():
            messagebox.showinfo("Room name adjusted",
                                f"Room name cleaned to '{room}' "
                                "(spaces and special characters are removed).")

        # Start subscriber thread and wait until it is ready (or failed)
        ready = threading.Event()
        status = {}
        threading.Thread(target=self.listen, args=(port, room, ready, status),
        daemon=True).start()
        ready.wait(10)
        if "error" in status or not ready.is_set():
            messagebox.showerror("Connection failed",
            f"Could not connect to RabbitMQ on port {port}.\n"
            f"Is Docker running?\n\n{status.get('error', 'Timed out')}")
            return

        # Separate connection for publishing (heartbeat=0: it sits idle between messages)
        try:
            self.pub_conn = pika.BlockingConnection(
                pika.ConnectionParameters(HOST, port, heartbeat=0))
            self.pub_ch = self.pub_conn.channel()
            declare_topic(self.pub_ch, EXCHANGE)
        except Exception as e:
            messagebox.showerror("Connection failed", str(e))
            return

        self.username = user
        self.room = room
        self.frame.destroy()
        self.build_chat()
        self.root.after(100, self.poll)

    # ---------- Listener thread ----------
    def listen(self, port, room, ready, status):
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(HOST, port, heartbeat=0))
            ch = conn.channel()
            declare_topic(ch, EXCHANGE)

            # One persistent stream per room = the room's message history
            stream = f"history.{room}"
            ch.queue_declare(queue=stream, durable=True,
            arguments={"x-queue-type": "stream", "x-max-age": "7D"})
            ch.queue_bind(exchange=EXCHANGE, queue=stream, routing_key=f"room.{room}")
            ch.basic_qos(prefetch_count=100)   # required for streams
        except Exception as e:
            status["error"] = str(e)
            ready.set()
            return

        def on_message(channel, method, props, body):
            self.incoming.put(json.loads(body))
            channel.basic_ack(method.delivery_tag)   # streams require acks

        # offset "first" = start from the oldest stored message, then continue live
        ch.basic_consume(queue=stream, on_message_callback=on_message, auto_ack=False,
        arguments={"x-stream-offset": "first"})
        ready.set()
        try:
            ch.start_consuming()
        except Exception as e:
            self.incoming.put({"system": f"Connection to RabbitMQ lost ({type(e).__name__}). "
            "Restart the app to rejoin."})

    # ---------- Chat screen ----------
    def build_chat(self):
        self.root.title(f"PBT205 Chat - {self.username} in #{self.room}")
        self.frame = tk.Frame(self.root, bg=BG)
        self.frame.pack(fill="both", expand=True)

        # Header bar
        header = tk.Frame(self.frame, bg=PANEL, padx=14, pady=10)
        header.pack(fill="x")
        avatar = tk.Canvas(header, width=40, height=40, bg=PANEL, highlightthickness=0)
        avatar.create_oval(1, 1, 39, 39, fill=ACCENT, outline="")
        avatar.create_text(20, 20, text=self.username[0].upper(), fill="white",
        font=(FONT, 14, "bold"))
        avatar.pack(side="left")
        info = tk.Frame(header, bg=PANEL)
        info.pack(side="left", padx=10)
        tk.Label(info, text=f"# {self.room}", bg=PANEL, fg=TEXT,
        font=(FONT, 14, "bold")).pack(anchor="w")
        tk.Label(info, text=f"Signed in as {self.username}", bg=PANEL, fg=MUTED,
        font=(FONT, 9)).pack(anchor="w")
        tk.Label(header, text="\u25CF Connected", bg=PANEL, fg=GREEN,
        font=(FONT, 9)).pack(side="right")

        # Message area
        self.text = tk.Text(self.frame, state="disabled", wrap="word", bg=BG, fg=TEXT,
                            relief="flat", highlightthickness=0, padx=4, pady=8,
                            cursor="arrow", font=(FONT, 11), selectbackground=ACCENT)
        self.text.pack(fill="both", expand=True)
        self.setup_tags()

        # Input bar
        bar = tk.Frame(self.frame, bg=PANEL, padx=12, pady=12)
        bar.pack(fill="x")
        self.msg_var = tk.StringVar()
        entry = tk.Entry(bar, textvariable=self.msg_var, bg=INPUT, fg=TEXT,
        insertbackground=TEXT, relief="flat", font=(FONT, 11),
        highlightthickness=1, highlightbackground=INPUT,
        highlightcolor=ACCENT)
        entry.pack(side="left", fill="x", expand=True, ipady=8)
        entry.bind("<Return>", lambda e: self.send())
        send = tk.Button(bar, text="Send", command=self.send, bg=ACCENT, fg="white",
        activebackground=ACCENT_HOVER, activeforeground="white",
        relief="flat", bd=0, cursor="hand2", font=(FONT, 10, "bold"),
        padx=18, pady=7)
        send.pack(side="left", padx=(8, 0))
        hover(send, ACCENT, ACCENT_HOVER)
        entry.focus()
        self.entry = entry

        self.add_system(f"Welcome to #{self.room}")

    def setup_tags(self):
        t = self.text
        # Layout tags
        t.tag_configure("left", justify="left", lmargin1=14, lmargin2=14, rmargin=70,
                        spacing1=1, spacing3=4)
        t.tag_configure("right", justify="right", lmargin1=70, lmargin2=70, rmargin=14,
                        spacing1=1, spacing3=4)
        t.tag_configure("center", justify="center", lmargin1=10, lmargin2=10, rmargin=10,
                        spacing1=8, spacing3=8)
        t.tag_configure("gstart", spacing1=12)
        # Style tags
        t.tag_configure("me_name", foreground="#a5b4fc", font=(FONT, 9, "bold"))
        t.tag_configure("time", foreground=MUTED, font=(FONT, 8))
        t.tag_configure("bubble_me", background=BUBBLE_ME, foreground="white",
                        font=(FONT, 11))
        t.tag_configure("bubble_other", background=BUBBLE_OTHER, foreground=TEXT,
                        font=(FONT, 11))
        t.tag_configure("system", foreground=MUTED, font=(FONT, 9, "italic"))
        t.tag_configure("day", foreground=MUTED, font=(FONT, 9, "bold"))

    def user_tag(self, user):
        tag = f"u_{user}"
        if tag not in self.user_tags:
            color = NAME_COLORS[sum(ord(c) for c in user) % len(NAME_COLORS)]
            self.text.tag_configure(tag, foreground=color, font=(FONT, 9, "bold"))
            self.user_tags.add(tag)
        return tag

    def write(self, segments):
        self.text.config(state="normal")
        for content, tags in segments:
            self.text.insert("end", content, tags)
        self.text.config(state="disabled")
        self.text.see("end")

    def add_system(self, line):
        self.write([(line + "\n", ("center", "system"))])

    def add_message(self, msg):
        t = msg.get("timestamp", time.time())
        lt = time.localtime(t)
        day = time.strftime("%Y-%m-%d", lt)

        if day != self.last_day:
            label = "Today" if day == time.strftime("%Y-%m-%d") else time.strftime("%a %d %b", lt)
            self.write([(f"\u2014 {label} \u2014\n", ("center", "day"))])
            self.last_day = day
            self.last_user = None

        mine = msg["user"] == self.username
        side = "right" if mine else "left"
        new_group = not (msg["user"] == self.last_user and 0 <= t - self.last_ts < 120)

        if new_group:
            name = "You" if mine else msg["user"]
            name_tag = "me_name" if mine else self.user_tag(msg["user"])
            self.write([
                (name, (side, "gstart", name_tag)),
                ("   " + time.strftime("%H:%M", lt), (side, "gstart", "time")),
                ("\n", (side, "gstart")),
            ])

        bubble = "bubble_me" if mine else "bubble_other"
        self.write([(f"  {msg['text']}  ", (side, bubble)), ("\n", (side,))])
        self.last_user = msg["user"]
        self.last_ts = t

    def send(self):
        text = self.msg_var.get().strip()
        if not text:
            return
        message = {
            "user": self.username,
            "room": self.room,
            "text": text,
            "timestamp": time.time(),
        }
        try:
            publish(self.pub_ch, EXCHANGE, f"room.{self.room}", message)
        except Exception as e:
            messagebox.showerror("Send failed", f"Connection lost.\n\n{e}")
            return
        # No local echo: our own message comes back through the room's stream,
        # so history and live messages always appear in the same order.
        self.msg_var.set("")
        self.entry.focus()

    # Check for new messages every 100 ms (Tkinter is not thread-safe, so the
    # listener thread only puts messages in a queue and the GUI reads them here)
    def poll(self):
        while not self.incoming.empty():
            msg = self.incoming.get()
            if "system" in msg:
                self.add_system(f"* {msg['system']}")
            else:
                self.add_message(msg)
        self.root.after(100, self.poll)

    def on_close(self):
        try:
            if self.pub_conn and self.pub_conn.is_open:
                self.pub_conn.close()
        except Exception:
            pass
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    ChatApp(root)
    root.mainloop()
