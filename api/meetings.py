from api.index import handler as BaseHandler

class handler(BaseHandler):
    route_override = "/api/meetings"
