import sys

# pylint: disable=undefined-variable

if sys.version_info[0] > 2:
    def is_string(obj):
        """
        Python 3 wrapper for determining if an object is a "string" (unicode).

        :param obj: The object
        :return: True if the object is a unicode string, False if not.
        :rtype: bool
        """
        return isinstance(obj, str)
else:
    def is_string(obj):
        """
        Python 2 wrapper for determining if an object is a "string" (unicode
        or byte-string).

        :param obj: The object
        :return: True if the object is a "string", False if not.
        :rtype: bool
        """
        return isinstance(obj, basestring)
