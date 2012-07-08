# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
"""
"""
# Copyright © 2010  Philipp Kern <pkern@debian.org>
# Copyright © 2010, 2011  Fabian Knittel <fabian.knittel@lettink.de>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA.

def build_description_url(dbhost, dbport, dbname):
    tmpl = None
    if dbhost is not None and len(dbhost) > 0:
        if dbport is not None and dbport > 0:
            if dbname is not None and len(dbname) > 0:
                tmpl = '%(host)s:%(port)d/%(name)s'
            else:
                tmpl = '%(host)s:%(port)d/'
        else:
            if dbname is not None and len(dbname) > 0:
                tmpl = '%(host)s/%(name)s'
            else:
                tmpl = '%(host)s/'
    else:
        # Without dbhost, we ignore dbport
        if dbname is not None:
            tmpl = '/%(name)s'
        else:
            tmpl = '/'

    if tmpl is None:
        return None

    return tmpl % {'host':dbhost, 'port':dbport, 'name':dbname}

def table_exists(sess, table_name):
    """Returns True in case table `table_name` exists in the database.
    """
    conn = sess.connection()
    dialect = conn.engine.dialect
    return dialect.has_table(conn, table_name)
