from http.server import BaseHTTPRequestHandler
from api.index import handler as BaseHandler

class handler(BaseHTTPRequestHandler):
    route_override = "/api/overview"
    do_GET = BaseHandler.do_GET
    handle_get = BaseHandler.handle_get
    json_response = BaseHandler.json_response
    log_message = BaseHandler.log_message
