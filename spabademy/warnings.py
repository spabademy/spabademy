# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:

# spabademy.warnings -- provides a Python 2.6 compat-layer for Python 2.5 users
#
# Copyright © 2010  Fabian Knittel <fabian.knittel@avona.com>
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

from __future__ import absolute_import
import warnings as _warnings # pylint: disable=W0406

if not hasattr(_warnings, 'catch_warnings'):
    class catch_warnings(object):
        def __init__(self):
            self.stored_filters = None
        def __enter__(self):
            self.stored_filters = _warnings.filters
            _warnings.filters = self.stored_filters[:]

        def __exit__(self, *exc_info):
            _warnings.filters = self.stored_filters
else:
    from warnings import catch_warnings #@UnusedImport

from warnings import filterwarnings #@UnusedImport pylint: disable=W0611
