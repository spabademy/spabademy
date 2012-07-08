# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
'''
Tests the ``spabademy.database.migration.driver`` module.
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

from spabademy.database.migrations.driver import Driver
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.interfaces import PoolListener
from spabademy.database import table_exists
from spabademy.database.migrations.db import Repository
from spabademy.database.migrations.patch import PatchRepository
from spabademy.database.migrations.patch import Patch
from nose.tools import eq_

class SQLiteForeignKeysListener(PoolListener):
    """Listens for DB connections and activates foreign key checks. Will not
    fail on older SQLite versions, because it uses PRAGMA.
    """
    def connect(self, dbapi_con, _):
        dbapi_con.execute('PRAGMA foreign_keys=ON')

class TestDriver(object):
    def __init__(self):
        self.engine = None
        self.sess = None
        self.driver = None
        self.patchrepo = None
        self.patch1 = None
        self.patch2 = None
        self.patch3 = None

    def setUp(self):
        self.engine = create_engine('sqlite://',
                listeners=[SQLiteForeignKeysListener()])

        Session = sessionmaker(bind=self.engine)
        self.sess = Session()

        self.patchrepo = PatchRepository()
        self.driver = Driver(self.sess, 'test_repo', self.patchrepo)

        self.patch1 = None
        self.patch2 = None
        self.patch3 = None

    def init_repo(self):
        self.driver.init_repo()

        self.patch1 = Patch('patch1', depends_on_names=[('patch2', False)],
                upgrade_sql='CREATE TABLE t1(a integer);\n' + \
                        'CREATE TABLE t12(a integer);',
                downgrade_sql='DROP TABLE t1; DROP TABLE t12;')
        self.patch2 = Patch('patch2', depends_on_names=[('patch3', False)],
                upgrade_sql='CREATE TABLE t2(a integer);',
                downgrade_sql='DROP TABLE t2;')
        self.patch3 = Patch('patch3', depends_on_names=[],
                upgrade_sql='CREATE TABLE t3(a integer);',
                downgrade_sql='DROP TABLE t3;')
        self.patchrepo.add_patches(self.patch1, self.patch2, self.patch3)
        self.patchrepo.resolve_dependencies()

    def assert_table_exists(self, table_name):
        assert table_exists(self.sess, table_name), \
                "Expected table '%s' to exist" % (table_name)

    def assert_tables_exist(self, table_names):
        for table_name in table_names:
            self.assert_table_exists(table_name)

    def assert_table_not_exists(self, table_name):
        assert not table_exists(self.sess, table_name), \
                "Expected table '%s' to NOT exist" % (table_name)

    def assert_tables_not_exist(self, table_names):
        for table_name in table_names:
            self.assert_table_not_exists(table_name)

    def test_init(self):
        self.assert_tables_not_exist(['migrate_repositories',
                'migrate_applied_patches'])
        self.driver.init_repo()
        dbrepos = self.sess.query(Repository.repository_name).all()
        eq_(dbrepos, [('test_repo',)])
        self.assert_tables_exist(['migrate_repositories',
                'migrate_applied_patches'])

    def test_uninit(self):
        self.driver.init_repo()
        self.assert_tables_exist(['migrate_repositories',
                'migrate_applied_patches'])
        self.driver.uninit_repo()
        self.assert_tables_not_exist(['migrate_repositories',
                'migrate_applied_patches'])

    def test_uninit_with_patches(self):
        self.init_repo()
        self.assert_tables_exist(['migrate_repositories',
                'migrate_applied_patches'])
        self.driver.upgrade_patches(patches=[self.patch3], execute_sql=False)
        self.sess.flush()
        self.driver.uninit_repo()
        self.assert_tables_not_exist(['migrate_repositories',
                'migrate_applied_patches'])

    def test_uninit_other_repo(self):
        self.driver.init_repo()
        other_driver = Driver(self.sess, 'test_repo2', self.patchrepo)
        other_driver.init_repo()
        self.assert_tables_exist(['migrate_repositories',
                'migrate_applied_patches'])
        dbrepos = self.sess.query(Repository.repository_name).all()
        eq_(set(dbrepos), set([('test_repo',), ('test_repo2',)]))

        self.driver.uninit_repo()
        self.assert_tables_exist(['migrate_repositories',
                'migrate_applied_patches'])
        dbrepos = self.sess.query(Repository.repository_name).all()
        eq_(dbrepos, [('test_repo2',)])
        other_driver.uninit_repo()
        self.assert_tables_not_exist(['migrate_repositories',
                'migrate_applied_patches'])

    def test_upgrade_to_patch(self):
        self.init_repo()

        eq_(self.driver.applied_patches, [])
        self.assert_tables_not_exist(['t1', 't12', 't2', 't3'])
        self.driver.upgrade_patches([self.patch2])
        eq_(set(self.driver.applied_patches), set([self.patch2, self.patch3]))
        self.assert_tables_not_exist(['t1', 't12'])
        self.assert_tables_exist(['t2', 't3'])

    def test_upgrade_from_empty(self):
        self.init_repo()

        eq_(self.driver.applied_patches, [])
        self.assert_tables_not_exist(['t1', 't12', 't2', 't3'])
        self.driver.upgrade()
        eq_(set(self.driver.applied_patches), set([self.patch1, self.patch2,
                self.patch3]))
        self.assert_tables_exist(['t1', 't12', 't2', 't3'])

    def test_downgrade_patch(self):
        self.init_repo()

        self.driver.upgrade_patches([self.patch1, self.patch2, self.patch3])
        eq_(self.driver.applied_patches, [self.patch1, self.patch2,
                self.patch3])
        self.assert_tables_exist(['t1', 't12', 't2', 't3'])

        self.driver.downgrade_patches([self.patch2])
        eq_(set(self.driver.applied_patches), set([self.patch3]))
        self.assert_tables_not_exist(['t1', 't12', 't2'])
        self.assert_tables_exist(['t3'])

    def test_downgrade(self):
        self.init_repo()

        self.driver.upgrade_patches([self.patch1, self.patch2, self.patch3])
        eq_(self.driver.applied_patches, [self.patch1, self.patch2,
                self.patch3])
        self.assert_tables_exist(['t1', 't12', 't2', 't3'])

        self.driver.downgrade()
        eq_(set(self.driver.applied_patches), set([]))
        self.assert_tables_not_exist(['t1', 't12', 't2', 't3'])

    def test_empty_upgrade(self):
        self.driver.init_repo()

        self.patch1 = Patch('patch1', depends_on_names=[('patch2', False)])
        self.patch2 = Patch('patch2', depends_on_names=[('patch3', False)])
        self.patch3 = Patch('patch3', depends_on_names=[])
        self.patchrepo.add_patches(self.patch1, self.patch2, self.patch3)
        self.patchrepo.resolve_dependencies()

        self.driver.upgrade()
        eq_(self.driver.applied_patches, [self.patch1, self.patch2,
                self.patch3])
        self.driver.downgrade()
        eq_(set(self.driver.applied_patches), set([]))

    def test_calculate_minimal_deps(self):
        self.init_repo()
        patch4 = Patch('patch4', depends_on_names=[('patch5', False)])
        patch5 = Patch('patch5', depends_on_names=[])
        self.patchrepo.add_patches(patch4, patch5)
        self.patchrepo.resolve_dependencies()

        eq_(set(self.driver.calculate_minimal_deps(patches=[self.patch1,
            self.patch2, self.patch3, patch4, patch5])), set([self.patch1,
                patch4]))
