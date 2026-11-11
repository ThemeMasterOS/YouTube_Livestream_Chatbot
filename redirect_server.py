import os
from http.server import BaseHTTPRequestHandler, HTTPServer

# Render requires binding to 0.0.0.0 and the PORT env var it provides
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 10000))
TARGET = os.environ.get("TARGET", "http://217.154.36.84:8011")


class RedirectHandler(BaseHTTPRequestHandler):
    def _redirect(self, status):
        # Preserve the path and query string (e.g. /foo?x=1 -> example.com/foo?x=1)
        location = TARGET.rstrip("/") + self.path
        self.send_response(status)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._redirect(302)

    def do_HEAD(self):
        self._redirect(302)

    def do_POST(self):
        # 307 keeps the request method as POST after the redirect
        self._redirect(307)


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), RedirectHandler)
    print(f"Redirecting port {PORT} -> {TARGET} (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()
