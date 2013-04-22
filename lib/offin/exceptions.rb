
# Package errors will abort processing a particular package; in
# general, they are meant for the end user.
#
# System errors will abort all activity entirely, (e.g., missing
# config file, can't connect to an essential service).  They are
# meant to be kept to operations.

class PackageError < StandardError; end
class CollectionError < PackageError; end



class SystemError  < StandardError; end
