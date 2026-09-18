"""Rate-limit incident notifications without hiding errors from runtime logs."""
import time


class IncidentAlerts:
    def __init__(self, send, interval=300, clock=time.monotonic):
        self.send = send
        self.interval = max(1, interval)
        self.clock = clock
        self.last_sent = {}

    def notify(self, key, message):
        now = self.clock()
        previous = self.last_sent.get(key)
        if previous is not None and now - previous < self.interval:
            return False
        # Limit attempts too, so an unavailable Telegram API cannot flood retries.
        self.last_sent[key] = now
        return self.send(message)

    def clear(self, key):
        self.last_sent.pop(key, None)
