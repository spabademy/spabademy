# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
'''
``spabademy.database.migrations`` contains the flexible PostgreSQL database schema
migration support.
'''
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

from spabademy.database.migrations.db import AppliedPatch

class SqlMigrationException(Exception):
    pass

def check_repository_has_patches(sess, repository_name, patch_names):
    not_applied = []
    for patch_name in patch_names:
        if not AppliedPatch.is_applied(sess, repository_name, patch_name):
            not_applied.append(patch_name)
    if len(not_applied) > 0:
        raise SqlMigrationException('The repository has an outdated schema '
                'state and misses the patches %s' % (not_applied))
