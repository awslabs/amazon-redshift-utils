from .core import (
    Warning, Bytea, DataError, DatabaseError, InterfaceError, ProgrammingError,
    Error, OperationalError, IntegrityError, InternalError, NotSupportedError,
    ArrayContentNotHomogenousError, ArrayContentEmptyError,
    ArrayDimensionsNotConsistentError, ArrayContentNotSupportedError, utc,
    Connection, Cursor, Binary, Date, DateFromTicks, Time, TimeFromTicks,
    Timestamp, TimestampFromTicks, BINARY, Interval)
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

# Copyright (c) 2007-2009, Mathieu Fenniak
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"


def connect(
        user=None, host='localhost', unix_sock=None, port=5432, database=None,
        password=None, ssl=False, timeout=None, **kwargs):
    """Creates a connection to a PostgreSQL database.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_; however, the arguments of the
    function are not defined by the specification.

    :param user:
        The username to connect to the PostgreSQL server with. If this is not
        provided, pg8000 looks first for the PGUSER then the USER environment
        variables.

        If your server character encoding is not ``ascii`` or ``utf8``, then
        you need to provide ``user`` as bytes, eg.
        ``"my_name".encode('EUC-JP')``.

    :keyword host:
        The hostname of the PostgreSQL server to connect with.  Providing this
        parameter is necessary for TCP/IP connections.  One of either ``host``
        or ``unix_sock`` must be provided. The default is ``localhost``.

    :keyword unix_sock:
        The path to the UNIX socket to access the database through, for
        example, ``'/tmp/.s.PGSQL.5432'``.  One of either ``host`` or
        ``unix_sock`` must be provided.

    :keyword port:
        The TCP/IP port of the PostgreSQL server instance.  This parameter
        defaults to ``5432``, the registered common port of PostgreSQL TCP/IP
        servers.

    :keyword database:
        The name of the database instance to connect with.  This parameter is
        optional; if omitted, the PostgreSQL server will assume the database
        name is the same as the username.

        If your server character encoding is not ``ascii`` or ``utf8``, then
        you need to provide ``database`` as bytes, eg.
        ``"my_db".encode('EUC-JP')``.

    :keyword password:
        The user password to connect to the server with.  This parameter is
        optional; if omitted and the database server requests password-based
        authentication, the connection will fail to open.  If this parameter
        is provided but not requested by the server, no error will occur.

    :keyword ssl:
        Use SSL encryption for TCP/IP sockets if ``True``.  Defaults to
        ``False``.

    :keyword timeout:
        Only used with Python 3, this is the time in seconds before the
        connection to the database will time out. The default is ``None`` which
        means no timeout.

    :rtype:
        A :class:`Connection` object.
    """
    return Connection(
        user, host, unix_sock, port, database, password, ssl, timeout)

apilevel = "2.0"
"""The DBAPI level supported, currently "2.0".

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

threadsafety = 3
"""Integer constant stating the level of thread safety the DBAPI interface
supports.  This DBAPI module supports sharing the module, connections, and
cursors, resulting in a threadsafety value of 3.

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

paramstyle = 'format'
"""String property stating the type of parameter marker formatting expected by
the interface.  This value defaults to "format", in which parameters are
marked in this format: "WHERE name=%s".

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.

As an extension to the DBAPI specification, this value is not constant; it
can be changed to any of the following values:

    qmark
        Question mark style, eg. ``WHERE name=?``
    numeric
        Numeric positional style, eg. ``WHERE name=:1``
    named
        Named style, eg. ``WHERE name=:paramname``
    format
        printf format codes, eg. ``WHERE name=%s``
    pyformat
        Python format codes, eg. ``WHERE name=%(paramname)s``
"""

# I have no idea what this would be used for by a client app.  Should it be
# TEXT, VARCHAR, CHAR?  It will only compare against row_description's
# type_code if it is this one type.  It is the varchar type oid for now, this
# appears to match expectations in the DB API 2.0 compliance test suite.

STRING = 1043
"""String type oid."""


NUMBER = 1700
"""Numeric type oid"""

DATETIME = 1114
"""Timestamp type oid"""

ROWID = 26
"""ROWID type oid"""

__all__ = [
    Warning, Bytea, DataError, DatabaseError, connect, InterfaceError,
    ProgrammingError, Error, OperationalError, IntegrityError, InternalError,
    NotSupportedError, ArrayContentNotHomogenousError, ArrayContentEmptyError,
    ArrayDimensionsNotConsistentError, ArrayContentNotSupportedError, utc,
    Connection, Cursor, Binary, Date, DateFromTicks, Time, TimeFromTicks,
    Timestamp, TimestampFromTicks, BINARY, Interval]

"""Version string for pg8000.

    .. versionadded:: 1.9.11
"""
