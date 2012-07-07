# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
'''
Tests the ``spabademy.database.migrations.patch`` module.
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

import tempfile
import shutil
import os.path
from nose.tools import eq_
from spabademy.database.migrations.patch import Patch
from spabademy.database.migrations.patch import PatchRepository
from spabademy.database.migrations.patch import DirPatchLoader
from spabademy.database.migrations.patch import DirPatchRepositoryLoader
from spabademy.database.migrations.patch import generate_upgrade_plan
from spabademy.database.migrations.patch import generate_downgrade_plan

def test_create_empty_patch():
    """Check whether creating a Patch instance works."""
    _p = Patch('some_patch_name')

def test_resolve_deps():
    """Check that patch name lookup via the repo works."""
    p1 = Patch('patch1', depends_on_names=[
            ('patch3', False), ('patch4', False)])
    p3 = Patch('patch3')
    p4 = Patch('patch4')
    repo = PatchRepository()
    repo.add_patch(p1)
    repo.add_patch(p3)
    repo.add_patch(p4)
    repo.resolve_dependencies()
    eq_(set(p1.depends_on), set([p3, p4]))
    eq_(p3.depends_on, [])
    eq_(p4.depends_on, [])

def test_lookup_patches():
    p1 = Patch('patch1')
    p3 = Patch('patch3')
    p4 = Patch('patch4')
    repo = PatchRepository()
    repo.add_patch(p1)
    repo.add_patch(p3)
    repo.add_patch(p4)
    patches = repo.lookup_patch_names(['patch1', 'patch4'])
    eq_(patches, [p1, p4])

class TempDirTestCase(object):
    tmp_dir_path = None

    def setUp(self):
        self.tmp_dir_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir_path, ignore_errors=True)

class TestDirPatchLoader(TempDirTestCase):
    def test_dir_load(self):
        patch_name = 'the_patch'
        patch_dir = os.path.join(self.tmp_dir_path, patch_name)
        os.mkdir(patch_dir)
        with open(os.path.join(patch_dir, 'depends_on'), 'wb') as fp:
            fp.write('other_patch\n')
            fp.write('yet_another_one\n')
            fp.write('optional_one?\n')
        with open(os.path.join(patch_dir, 'upgrade.sql'), 'wb') as fp:
            fp.write('SELECT 1\n')
        with open(os.path.join(patch_dir, 'downgrade.sql'), 'wb') as fp:
            fp.write('SELECT 2\n')

        loader = DirPatchLoader()
        patch = loader.load_patch(patch_dir)
        eq_(patch.name, patch_name)
        eq_(set(patch.depends_on_names), set([('other_patch', False),
                ('yet_another_one', False), ('optional_one', True)]))
        eq_(patch.upgrade_sql, 'SELECT 1\n')
        eq_(patch.downgrade_sql, 'SELECT 2\n')

class TestDirPatchRepositoryLoader(TempDirTestCase):
    def test_repo_dir_load(self):
        patch_dir = os.path.join(self.tmp_dir_path, 'patch1')
        os.mkdir(patch_dir)
        with open(os.path.join(patch_dir, 'depends_on'), 'wb') as fp:
            fp.write('patch2\n')
            fp.write('patch7\n')
        with open(os.path.join(patch_dir, 'upgrade.sql'), 'wb') as fp:
            fp.write('SELECT 1\n')
        with open(os.path.join(patch_dir, 'downgrade.sql'), 'wb') as fp:
            fp.write('SELECT 2\n')

        patch_dir = os.path.join(self.tmp_dir_path, 'patch2')
        os.mkdir(patch_dir)
        with open(os.path.join(patch_dir, 'upgrade.sql'), 'wb') as fp:
            fp.write('SELECT 3\n')
        with open(os.path.join(patch_dir, 'downgrade.sql'), 'wb') as fp:
            fp.write('SELECT 4\n')

        patch_dir = os.path.join(self.tmp_dir_path, 'patch7')
        os.mkdir(patch_dir)
        with open(os.path.join(patch_dir, 'depends_on'), 'wb') as fp:
            fp.write('patch2\n')
        with open(os.path.join(patch_dir, 'upgrade.sql'), 'wb') as fp:
            fp.write('SELECT 5\n')

        open(os.path.join(self.tmp_dir_path, 'unrelated_file'), 'wb').close()

        repo_loader = DirPatchRepositoryLoader(patch_loader=DirPatchLoader())
        repo = repo_loader.load_repo(self.tmp_dir_path)
        eq_(set(repo.patches.keys()), set(['patch1', 'patch2', 'patch7']))
        patch = repo.patches['patch7']
        eq_(patch.name, 'patch7')
        eq_(set(patch.depends_on_names), set([('patch2', False)]))
        eq_(patch.upgrade_sql, 'SELECT 5\n')
        eq_(patch.downgrade_sql, None)

        repo.resolve_dependencies()
        eq_(set(patch.depends_on), set([repo.patches['patch2']]))


def test_upgrade_from_empty():
    patchrepo = PatchRepository()
    patch1 = Patch('patch1', depends_on_names=[('patch2', False)])
    patch2 = Patch('patch2', depends_on_names=[('patch3', False)])
    patch3 = Patch('patch3', depends_on_names=[])
    patchrepo.add_patches(patch1, patch2, patch3)
    patchrepo.resolve_dependencies()

    plan = generate_upgrade_plan(applied_patches=[],
            to_be_applied_patches=[patch1])
    eq_(plan, [patch3, patch2, patch1])

    plan = generate_upgrade_plan(applied_patches=[],
            to_be_applied_patches=[patch3])
    eq_(plan, [patch3])

def test_complex_upgrade_from_empty():
    patchrepo = PatchRepository()
    patch1 = Patch('patch1', depends_on_names=[('patch2', False)])
    patch2 = Patch('patch2', depends_on_names=[('patch3', False), ('patch4', False)])
    patch3 = Patch('patch3', depends_on_names=[('patch4', False), ('patch5', False)])
    patch4 = Patch('patch4', depends_on_names=[])
    patch5 = Patch('patch5', depends_on_names=[])
    patchrepo.add_patches(patch1, patch2, patch3, patch4, patch5)
    patchrepo.resolve_dependencies()

    plan = generate_upgrade_plan(applied_patches=[],
            to_be_applied_patches=[patch1])
    eq_(plan, [patch4, patch5, patch3, patch2, patch1])

def test_complex_upgrade():
    patchrepo = PatchRepository()
    patch1 = Patch('patch1', depends_on_names=[('patch2', False)])
    patch2 = Patch('patch2', depends_on_names=[('patch3', False),
            ('patch4', False)])
    patch3 = Patch('patch3', depends_on_names=[('patch4', False),
            ('patch5', False)])
    patch4 = Patch('patch4', depends_on_names=[])
    patch5 = Patch('patch5', depends_on_names=[])
    patchrepo.add_patches(patch1, patch2, patch3, patch4, patch5)
    patchrepo.resolve_dependencies()

    plan = generate_upgrade_plan(applied_patches=[patch4, patch5],
            to_be_applied_patches=[patch1])
    eq_(plan, [patch3, patch2, patch1])

def test_downgrade():
    patchrepo = PatchRepository()
    patch1 = Patch('patch1', depends_on_names=[('patch2', False)])
    patch2 = Patch('patch2', depends_on_names=[('patch3', False),
            ('patch4', False)])
    patch3 = Patch('patch3', depends_on_names=[('patch4', False),
            ('patch5', False)])
    patch4 = Patch('patch4', depends_on_names=[])
    patch5 = Patch('patch5', depends_on_names=[])
    patchrepo.add_patches(patch1, patch2, patch3, patch4, patch5)
    patchrepo.resolve_dependencies()

    applied_patches = [patch1, patch2, patch3, patch4, patch5]
    plan = generate_downgrade_plan(applied_patches=applied_patches,
            to_be_removed_patches=[patch1])
    eq_(plan, [patch1])

    plan = generate_downgrade_plan(applied_patches=applied_patches,
            to_be_removed_patches=[patch3])
    eq_(plan, [patch1, patch2, patch3])

    plan = generate_downgrade_plan(applied_patches=applied_patches,
            to_be_removed_patches=[patch5])
    eq_(plan, [patch1, patch2, patch3, patch5])

    plan = generate_downgrade_plan(applied_patches=applied_patches,
            to_be_removed_patches=[patch5, patch4])
    assert plan == [patch1, patch2, patch3, patch4, patch5] or \
            plan == [patch1, patch2, patch3, patch5, patch4]
