# Package errors will abort processing a particular package; in
# general, they are meant for the end user, and won't require a traceback.
#
# System errors should abort all activity entirely, (e.g., missing
# config file, can't connect to an essential service).  They are
# meant to be kept for internal operations.

class PackageError < StandardError; end
class SystemError  < StandardError; end
