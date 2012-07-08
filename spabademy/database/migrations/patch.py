# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
'''
Provides support for parsing SQL patch files (snippets).
'''
# Copyright © 2011  Fabian Knittel <fabian.knittel@lettink.de>
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

import os.path
import codecs

class Patch(object):
    '''
    A Patch object represents a single SQL patch. Such a patch contains SQL
    directives to transform the database's current schema to a new schema and
    possibly directives to revert such a step.

    The patch may contain one or more dependencies, which means patches that
    need to be applied before this patch can be applied itsself. Dependencies
    may be optional, in which case it is acceptable for the patch to not exist.
    In case optional, depended-upon patches exist and aren't applied, they need
    to be applied before this patch.
    '''

    def __init__(self, name, depends_on_names=None, upgrade_sql=None,
                 downgrade_sql=None, origin=None):
        self.name = name
        self.depends_on_names = depends_on_names \
                if depends_on_names is not None else []
        self.depends_on = []
        self.upgrade_sql = upgrade_sql
        self.downgrade_sql = downgrade_sql
        self.origin = origin
        self.missing_deps = []

    def __repr__(self):
        return "<Patch('%s')>" % (self.name)

    def resolve_dependencies(self, patch_repo):
        '''
        Search for the depended-on patches and populate
        ``depends_on`` with references to the patch instances.

        *patch_repo*
          repository of patches in which the dependencies are searched for.

        Throws ``PatchNotFound`` in case a dependency can't be resolved.
        '''
        self.depends_on = []
        for dep_name, is_optional in self.depends_on_names:
            if dep_name in patch_repo.patches:
                self.depends_on.append(patch_repo.patches[dep_name])
            elif not is_optional:
                raise PatchNotFound(
                        'patch %s: could not find depended on patch "%s"' % (
                                self.name, dep_name))
            else:
                self.missing_deps.append(dep_name)

class PatchNotFound(Exception):
    pass

class PatchNotAccessible(Exception):
    pass

class PatchRepository(object):
    '''
    Holds all patches for a certain repository that could potentially be
    applied.
    '''

    def __init__(self):
        self.patches = {}

    def add_patch(self, patch):
        self.patches[patch.name] = patch

    def add_patches(self, *patches):
        for patch in patches:
            self.add_patch(patch)

    def resolve_dependencies(self):
        for patch in self.patches.itervalues():
            patch.resolve_dependencies(self)

    def lookup_patch_name(self, patch_name):
        return self.patches[patch_name]

    def lookup_patch_names(self, patch_names):
        return [self.patches[patch_name] for patch_name in patch_names]

class DirPatchLoader(object):
    '''
    Loads SQL patches from a directory-based structure.

    A patch is represented by a directory, where the directory's name is equal
    to the patch's name. Within the directory, there are up to three files:
    ``depends_on``, ``upgrade_sql`` and
    ``downgrade_sql``. The ``depends_on`` file contains a
    single patch name per line. Each patch name represents a dependency. The
    ``upgrade_sql`` and ``downgrade_sql`` files contain SQL
    code for upgrading to the patch or downgrading from the patch
    (respectively).

    Any of the files can be ommitted and any additional files within the
    directory will be ignored.
    '''
    def is_patch(self, patch_path):
        """Returns true in case ``patch_path`` is a valid path to a patch."""
        return os.path.exists(patch_path) and os.path.isdir(patch_path)

    def load_patch(self, patch_path):
        """Returns the ``Patch`` loaded from the path ``patch_path``."""
        if not self.is_patch(patch_path):
            raise PatchNotFound('patch dir "%s"' % (patch_path))
        patch_name = os.path.basename(patch_path)
        if not os.access(os.path.join(patch_path, '.'), os.F_OK):
            raise PatchNotAccessible(
                '%s is not accessible for the current user.' \
                    % os.path.join(patch_path))
        depends_on_names = self._parse_dependencies(patch_path)
        upgrade_sql = self._read_contents(os.path.join(patch_path,
                'upgrade.sql'))
        downgrade_sql = self._read_contents(os.path.join(patch_path,
                'downgrade.sql'))

        return Patch(patch_name, depends_on_names, upgrade_sql, downgrade_sql,
                origin=patch_path)

    def _read_contents(self, fn):
        """Return contents of ``fn`` or None if it doesn't exists.
        """
        if not os.path.exists(fn):
            return None
        with codecs.open(fn, 'rb', 'utf-8') as fp:
            return fp.read()

    def _parse_dependencies(self, patch_path):
        lines = self._read_lines_as_list(os.path.join(patch_path,
                'depends_on'))
        if lines is None:
            return None

        deps = []
        for l in lines:
            if l.endswith('?'):
                is_optional = True
                patch_name = l[:-1]
            else:
                is_optional = False
                patch_name = l
            deps.append((patch_name, is_optional))
        return deps

    def _read_lines_as_list(self, fn):
        """Return the lines of ``fn`` as array or None if the file doesn't
        exist. Lines starting with ``#`` are ignored.
        """
        if not os.path.exists(fn):
            return None
        l = []
        with codecs.open(fn, 'rb', 'utf-8') as fp:
            for line in fp:
                line = line.strip()
                if line.startswith('#'):
                    continue
                l.append(line)
        return l

class DirPatchRepositoryLoader(object):
    """Loads patches from directory of patches.
    """
    def __init__(self, patch_loader):
        self._patch_loader = patch_loader

    def load_repo(self, repo_dir):
        """Returns a new repo with patches loaded from ``repo_dir``.
        """
        repo = PatchRepository()
        for name in os.listdir(repo_dir):
            patch_path = os.path.join(repo_dir, name)
            if name == "repo_name":
                with open(patch_path, 'r') as fp:
                    repo.repo_name = fp.read().strip()
            if not self._patch_loader.is_patch(patch_path):
                continue
            patch = self._patch_loader.load_patch(patch_path)
            repo.add_patch(patch)
        assert hasattr(repo, 'repo_name')
        return repo

def generate_upgrade_plan(applied_patches, to_be_applied_patches):
    """Return the ordered list of patches to be installed, so that
    `to_be_applied_patches`, the list of patches that should be present
    after the upgrade, is installed, including all dependencies.

    Note: Assumes that the patch dependency graph is acyclic, otherwise the
    method will enter an infinite loop.
    """
    missing_patches = set(to_be_applied_patches).\
            difference(applied_patches)
    install_list = []

    while len(missing_patches) > 0:
        for missing_patch in missing_patches.copy():
            missing_deps = set(missing_patch.depends_on)
            missing_deps.difference_update(applied_patches)
            missing_deps.difference_update(set(install_list))
            if len(missing_deps) == 0:
                # At this point, we can install the patch, because the
                # dependencies are either a) already installed or b) will
                # be installed before this patch.
                install_list.append(missing_patch)
                missing_patches.remove(missing_patch)
            else:
                # Dependencies are missing. Add them to the set of patches
                # that should be upgraded.
                missing_patches.update(missing_deps)

    return install_list

def generate_downgrade_plan(applied_patches, to_be_removed_patches):
    """Return the ordered list of patches to uninstall. The list will also
    contain any patches that depend on the patches that are supposed to
    be uninstalled according to `to_be_removed_patches`.
    """
    to_be_removed_patches = set(to_be_removed_patches).\
            intersection(applied_patches)
    uninstall_list = []

    while len(to_be_removed_patches) > 0:
        for downgrade_patch in to_be_removed_patches.copy():
            has_dependencies = False
            for patch in set(applied_patches).\
                    difference(set(uninstall_list)):
                if downgrade_patch in patch.depends_on:
                    # This patch has a dependency on the to-be uninstalled
                    # patch, so it needs to go too.
                    has_dependencies = True
                    to_be_removed_patches.add(patch)
            if not has_dependencies:
                # Patch has no remaining dependencies.
                uninstall_list.append(downgrade_patch)
                to_be_removed_patches.remove(downgrade_patch)

    return uninstall_list
