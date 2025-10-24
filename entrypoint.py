#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import threading
import logging
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if self.path in ('/', '/healthz', '/_ah/health'):
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(b'OK')
            else:
                self.send_response(404)
                self.end_headers()
        except Exception:
            # 保持容器存活，不抛出
            pass
    def log_message(self, format, *args):
        # 静默日志
        return


def start_health_server():
    port = int(os.getenv('PORT', '8080'))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=False).start()
    logger.info(f"Health server listening on 0.0.0.0:{port}")


def run_bot():
    try:
        import asyncio
        import importlib
        bot = importlib.import_module('bot')
        asyncio.run(bot.main())
    except Exception as e:
        logger.error(f"启动机器人失败: {e}", exc_info=True)
        # 不中止进程，保持健康服务存活
        while True:
            time.sleep(3600)


if __name__ == '__main__':
    start_health_server()
    run_bot()
