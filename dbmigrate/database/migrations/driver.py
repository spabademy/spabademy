# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
'''
'''
# Copyright © 2011  Fabian Knittel <fabian.knittel@avona.com>
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

from __future__ import with_statement

from dbmigrate.database.migrations.db import create_tables
from dbmigrate.database.migrations.db import drop_tables
from dbmigrate.database.migrations.db import Repository
from dbmigrate.database.migrations.db import AppliedPatch
from dbmigrate.database.migrations.db import execute_script
from dbmigrate.database.migrations.patch import Patch
from dbmigrate.database.migrations.patch import generate_upgrade_plan
from dbmigrate.database.migrations.patch import generate_downgrade_plan
from sqlalchemy.exc import DatabaseError

class PatchFailedException(Exception):
    """Is raised when an SQL snippet fails to apply.
    """
    def __init__(self, operation, details):
        Exception.__init__(self, operation)
        self.details = details

class Driver(object):
    """Drives the upgrade and downgrade of a repository by applying or
    downgrading patches.
    """
    def __init__(self, sess, patch_repo):
        self.sess = sess
        self.patch_repo = patch_repo
        self.repo_name = self.patch_repo.repo_name

    def init_repo(self, patches=None):
        create_tables(self.sess.connection())

        # Check whether repo already set-up
        existing_repo = self._get_repo()
        if existing_repo is not None:
            raise RuntimeError('repository "%s" already exists' % (
                    self.repo_name))

        dbrepo = Repository(repository_name=self.repo_name)
        self.sess.add(dbrepo)
        self.sess.flush()

        if patches is not None:
            for patch in patches:
                self.sess.add(AppliedPatch(dbrepo.repository_id, patch.name))

    def uninit_repo(self):
        # Delete this repository
        dbrepo = self._get_repo()
        if dbrepo is None:
            raise RuntimeError('repository "%s" does not exist' % (
                    self.repo_name))
        self.sess.delete(dbrepo)

        # Check whether any repositories remain - if not, delete the tables too.
        num_remaining_repos = self.sess.query(Repository).count()
        if num_remaining_repos == 0:
            drop_tables(self.sess.connection())

    def _get_repo(self):
        existing_repo = self.sess.query(Repository)\
                .filter_by(repository_name=self.repo_name)\
                .first()
        return existing_repo

    @property
    def applied_patches(self):
        db_patches = AppliedPatch.get_all(self.sess, self.repo_name)
        patches = []
        for dbpatch in db_patches:
            if dbpatch.patch_name in self.patch_repo.patches:
                patches.append(self.patch_repo.patches[dbpatch.patch_name])
            else:
                patches.append(Patch(dbpatch.patch_name))
        return patches

    @property
    def unapplied_patches(self):
        applied = set(self.applied_patches)
        unapplied = [patch for patch in self.patch_repo.patches.itervalues() \
                if patch not in applied]
        return unapplied

    def upgrade_patches(self, patches, execute_sql=True):
        dbrepo = self._get_repo()
        applied_patches = self.applied_patches
        plan = generate_upgrade_plan(applied_patches=applied_patches,
                to_be_applied_patches=patches)
        for patch in plan:
            print "applying patch '%s'" % patch.name
            self.sess.add(AppliedPatch(dbrepo.repository_id, patch.name))
            if patch.upgrade_sql is not None and execute_sql:
                for patch_name in patch.missing_deps:
                    print " (ignoring optional missing patch '%s')" % patch_name
                with _TranslateErrors("patch upgrade failed '%s'" % (
                        patch.name)):
                    execute_script(self.sess, patch.upgrade_sql)
        return plan

    def calculate_minimal_deps(self, patches):
        """Returns the minimal set of patches that is equivalent to `patches`.
        The returned list will be equal to or smaller than `patches`, due to
        potentially existing dependencies between the patches.
        """
        # Note: This is a primitive algorithm and slow implementation. It isn't
        # intended for large sets of patches.
        patches = set(patches)
        minimal_patches = patches.copy()
        for patch in patches:
            # Generate recursive list of dependencies introduced by this patch.
            patch_deps = set(generate_upgrade_plan(applied_patches=[],
                    to_be_applied_patches=[patch]))
            # See whether any of the other patches are mentioned in the full
            # dependency list.
            for other_patch in patches:
                if other_patch == patch:
                    continue
                if other_patch in patch_deps and other_patch in minimal_patches:
                    # `other_patch` is part of the dependency list of `patch`, so
                    # (assuming an acyclic graph) it is not part the minimal
                    # patch set.
                    minimal_patches.remove(other_patch)
        return minimal_patches

    def upgrade(self, execute_sql=True):
        return self.upgrade_patches(self.patch_repo.patches.values(),
                execute_sql=execute_sql)

    def test_upgrade_patches(self, patches):
        """Performs upgrade and downgrade on specific patches more than once to
        help detect incomplete downgrade scripts.
        """
        for _ in range(2):
            up_plan = self.upgrade_patches(patches)
            self.downgrade_patches(up_plan)

    def test_upgrade(self):
        """Performs upgrade and downgrade more than once to help detect
        incomplete downgrade scripts.
        """
        for _ in range(2):
            up_plan = self.upgrade()
            self.downgrade_patches(up_plan)

    def downgrade_patches(self, patches, execute_sql=True):
        dbrepo = self._get_repo()
        applied_patches = self.applied_patches
        plan = generate_downgrade_plan(applied_patches=applied_patches,
                to_be_removed_patches=patches)
        for patch in plan:
            print "removing patch '%s'" % patch.name
            dbpatch = self.sess.query(AppliedPatch)\
                    .filter_by(repository_id=dbrepo.repository_id,
                            patch_name=patch.name)\
                    .one()
            self.sess.delete(dbpatch)
            if patch.downgrade_sql is not None and execute_sql:
                for patch_name in patch.missing_deps:
                    print " (ignoring optional missing patch '%s')" % patch_name
                with _TranslateErrors("patch downgrade failed '%s'" % (
                        patch.name)):
                    execute_script(self.sess, patch.downgrade_sql)
        return plan

    def downgrade(self, execute_sql=True):
        applied_patches = self.applied_patches
        self.downgrade_patches(applied_patches, execute_sql=execute_sql)

    def renew_patches(self, patches):
        """Performs downgrade of a set of patches and then upgrades
        all patches again. Useful if one or more patches were upgraded and need
        to be refreshed.
        """
        down_plan = self.downgrade_patches(patches)
        self.upgrade_patches(down_plan)


class _TranslateErrors(object):
    def __init__(self, operation):
        self.operation = operation

    def __enter__(self):
        return self

    def __exit__(self, _type, exc, _traceback):
        if exc is not None:
            if isinstance(exc, DatabaseError):
                raise PatchFailedException(self.operation,
                        exc.args[0].strip().splitlines())
            # Otherwise, do not suppress the exception.
            return False
