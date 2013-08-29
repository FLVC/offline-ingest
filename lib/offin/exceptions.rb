# Package errors will abort processing a particular package; in
# general, they are meant for the end user, and won't require a traceback.
#
# System errors should abort all activity entirely, (e.g., missing
# config file, can't connect to an essential service).  Their
# associated messages are likely to be sensitive and are meant to be
# kept for internal operations (and, in the case of the web services,
# logged to apache error logs only, not sent to browser).


class PackageError < StandardError; end
class SystemError  < StandardError; end
