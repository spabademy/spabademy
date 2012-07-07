# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
"""
This module provides the basics of database connection handling and
versioning.
"""
# Copyright © 2010  Philipp Kern <pkern@debian.org>
# Copyright © 2010, 2011  Fabian Knittel <fabian.knittel@avona.com>
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

import re
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.exc
import sqlalchemy.engine.url
import sqlalchemy.sql
from copy import deepcopy
from spabademy.warnings import filterwarnings
from spabademy.database.migrations import check_repository_has_patches
from pkg_resources import iter_entry_points


def has_role(sess, role_name):
    return sess.execute(sqlalchemy.sql.func.pg_has_role(role_name, 'USAGE'))\
            .fetchone()[0]

class Database(object):
    def __init__(self, callbacks=None):
        self.callbacks = {} if callbacks is None else callbacks
        self.server = None
        self.port = None
        self.dbname = None
        self.username = None
        self.password = None
        self.query = None
        self.meta = None
        self.session = None
        self.create_session = None
        self._engine = None
        self._connection = None

    def load_config(self, config, config_section='database'):
        self.server = config.getdef(config_section, 'server',
                default=self.server)
        self.port = config.getintdef(config_section, 'port',
                default=self.port)
        self.dbname = config.getdef(config_section, 'dbname',
                default=self.dbname)
        self.username = config.getdef(config_section, 'username',
                default=self.username)
        self.password = config.getdef(config_section, 'password',
                default=self.password)
        self.query = config.getdict(config_section, 'query',
                default=self.query)

    def _is_password_failure(self, exc):
        """This method checks if the exception's text contains a PostgreSQL
        password failure message.  This is probably heavily dependent on the DB
        server's locale.  It's at least not dependent on the client locale.
        """
        messages = [r'.*password authentication failed for user.*',
                r'.*PAM authentication failed for user.*',
                r'.*Passwort-Authentifizierung .* fehlgeschlagen.*']
        for message in messages:
            if re.match(message, exc.args[0]):
                return True
        return False

    def _is_missing_password_failure(self, exc):
        if re.match('.*no password supplied.*', exc.args[0]):
            return True
        return False

    def _interact_get_password(self):
        if self.password is not None:
            return self.password
        if 'password' in self.callbacks:
            return self.callbacks['password']()
        return None

    def _interact_password_correct(self):
        if 'password_correct' in self.callbacks:
            return self.callbacks['password_correct']()

    def _interact_password_incorrect(self):
        if 'password_incorrect' in self.callbacks:
            return self.callbacks['password_incorrect']()

    def _connect_internal(self, url):
        if url.database is None:
            raise sqlalchemy.exc.OperationalError(None,
                    ['no database specified'], None)

        # psycopg2 requires the url parameters to be UTF-8 encoded.
        utf8_url = deepcopy(url)
        if url.password is not None:
            utf8_url.password = url.password.encode('UTF-8')

        self._engine = sqlalchemy.create_engine(utf8_url, convert_unicode=True,
                encoding='UTF-8')
        self._connection = self._engine.connect()

    def _connect(self, url):
        try:
            self._connect_internal(url)
            if url.password is not None:
                self._interact_password_correct()
        except sqlalchemy.exc.OperationalError, e:
            if url.password is None and self._is_missing_password_failure(e):
                # Simple connect did not work, let's try with password.
                url.password = self._interact_get_password()
                if url.password is None:
                    raise
                return self._connect(url)
            elif url.password is not None and self._is_password_failure(e):
                # Obviously the password was incorrect, let's try again
                # until the user cancels.
                self._interact_password_incorrect()
                url.password = self.callbacks['password']()
                if url.password is None:
                    raise
                return self._connect(url)
            else:
                # Woah, something else went wrong.
                raise

    def setup(self):
        url = sqlalchemy.engine.url.URL('postgresql',
                port=self.port, host=self.server, database=self.dbname,
                username=self.username, password=self.password,
                query=self.query)
        self._connect(url)
        self.meta = sqlalchemy.MetaData()
        self.meta.bind = self._engine
        self.create_session = orm.sessionmaker(bind=self._engine,
                autocommit=False)

        self.session = self.create_session()

def reflect_tables(meta, table_names, tables=None):
    if tables is None:
        tables = {}
    for name in table_names:
        if isinstance(name, str):
            tables[name] = sqlalchemy.Table(name, meta, autoload=True)
        elif isinstance(name, tuple):
            tables[name[1]] = sqlalchemy.Table(name[1], meta, autoload=True,
                                                    schema=name[0])
        else:
            raise Exception, 'Unknown table name.'
    return tables

def table_exists(sess, table_name):
    """Returns True in case table `table_name` exists in the database.
    """
    conn = sess.connection()
    dialect = conn.engine.dialect
    return dialect.has_table(conn, table_name)

def enable_debugging():
    import logging
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)

def filter_reflection_warnings():
    filterwarnings("ignore", r'Skipped unsupported reflection of '\
            'expression-based index', sqlalchemy.exc.SAWarning)

class TransactionContext(object):
    """This context makes sure that the associated transaction is rolled back
    at its end.  The database session is required to have autotransaction turned
    off."""
    def __init__(self, *sessions):
        self.sessions = sessions

    def __enter__(self):
        pass

    def __exit__(self, *_):
        for sess in self.sessions:
            sess.rollback()

def escape_search_text(text):
    """Perform escaping of the search string.  All characters sent through
    `text` will be searched for verbatim, without special meanings.
    """
    escape = '\\'
    text = text\
            .replace(escape, escape + escape)\
            .replace('%', escape + '%')\
            .replace('_', escape + '_')
    return text

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

class TypeDescriptionMixin(object):
    """This mix-in class is intended for simple SQL tables mapped via
    SQLAlchemy, that contain a list of type ids.  This class provides a
    description getter that allows object's id to be translated into a human
    readable string.  All that needs to be done by the target class is to
    override the ``_DESCRIPTIONS`` property.
    """

    _DESCRIPTIONS = None
    """A dictionary of id to description string mappings.
    """

    def _get_type_description(self):
        """Returns the description for the current instance of the base class.

        The primary key of the object is used as the key to the descriptions
        dictionary ``_DESCRIPTIONS``.
        """
        mapper = orm.object_mapper(self)
        key = mapper.primary_key_from_instance(self)[0]
        if key in self._DESCRIPTIONS:
            return self._DESCRIPTIONS[key]
        else:
            return key

class TypeDescriptionPluginMixin(TypeDescriptionMixin):
    """This mix-in class extends :class:`TypeDescriptionMixin` and loads the
    descriptions from dictionaries associated with the specified entry point
    ``_ENTRY_POINT_NAME``.
    """

    _ENTRY_POINT_NAME = None
    """The name of the entry point with which description dictionaries are
    associated and loaded on first use.
    """

    def _load_descriptions(self):
        """Lazily initialise the description dictionary.
        """
        self._DESCRIPTIONS = {}
        for desc_prov in iter_entry_points(self._ENTRY_POINT_NAME):
            self._DESCRIPTIONS.update(desc_prov.load())

    def _get_type_description(self):
        """Returns the description for the current instance of the base class.
        Overrides :meth:`TypeDescriptionMixin._get_type_description` and lazy
        loads the descriptions first.
        """
        if self._DESCRIPTIONS is None:
            self._load_descriptions()

        return TypeDescriptionMixin._get_type_description(self)
