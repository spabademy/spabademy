# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
'''
Contains the methods and classes used for interaction with the database and for
storage of the repository's state.
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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import ForeignKey
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import relation
from sqlalchemy.orm import backref

_metadata = MetaData()
_Base = declarative_base(metadata=_metadata)

class Repository(_Base):
    """@DynamicAttrs"""
    __tablename__ = 'migrate_repositories'

    repository_id = Column(Integer, primary_key=True)
    repository_name = Column(String, unique=True)

    applied_patches = relation('AppliedPatch', backref=backref('repository',
            primaryjoin="Repository.repository_id == AppliedPatch.repository_id"),
            cascade='delete')

    def __init__(self, repository_id=None, repository_name=None):
        self.repository_id = repository_id
        self.repository_name = repository_name

    def __repr__(self):
        return "<MigrateRepository('%d','%s')>" % (self.repository_id,
                self.repository_name)

class AppliedPatch(_Base):
    """@DynamicAttrs"""
    __tablename__ = 'migrate_applied_patches'

    repository_id = Column(Integer,
            ForeignKey('migrate_repositories.repository_id'), primary_key=True)
    patch_name = Column(String, primary_key=True)

    def __init__(self, repository_id, patch_name):
        self.repository_id = repository_id
        self.patch_name = patch_name

    def __repr__(self):
        return "<AppliedPatch('%d','%s')>" % (self.repository_id,
                self.patch_name)

    @staticmethod
    def get_all(sess, repository_name):
        """Returns the list of applied patches for repository `repository_name`.
        """
        return sess.query(AppliedPatch)\
                .join(Repository)\
                .filter(Repository.repository_name == repository_name)\
                .all()

    @staticmethod
    def is_applied(sess, repository_name, patch_name):
        patch = sess.query(AppliedPatch)\
                .join(Repository)\
                .filter(Repository.repository_name == repository_name)\
                .filter(AppliedPatch.patch_name == patch_name)\
                .first()
        return patch is not None

DB_CLASSES = [Repository, AppliedPatch]

def create_tables(bind, checkfirst=True):
    for dbcls in DB_CLASSES:
        dbcls.__table__.create(bind, checkfirst=checkfirst)

def drop_tables(bind, checkfirst=True):
    for dbcls in reversed(DB_CLASSES):
        dbcls.__table__.drop(bind, checkfirst=checkfirst)

def _clear_connection(sess):
    """Patches might not always reset the connection's role or search_path, so
    explicitly do that here (currently only for PostgreSQL).
    """
    dialect = sess.connection().engine.dialect
    if dialect.name == 'postgresql':
        sess.execute('SET ROLE NONE')
        sess.execute('SET search_path = public')

def execute_script(sess, sql_text):
    """Execute an SQL script on a session. Works around limitation in SQLite
    back-end, which doesn't allow multiple statements, by using the
    SQLite-specific ``executescript()``-method, when available. The approach
    was borrowed from ``migrate.versioning.script.sql``.
    """
    dbapi = sess.connection().engine.raw_connection()
    if getattr(dbapi, 'executescript', None) is not None:
        dbapi.executescript(sql_text)
    else:
        sess.execute(sql_text)
    _clear_connection(sess)
