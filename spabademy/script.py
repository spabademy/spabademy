#!/usr/bin/env python
# vim:set fileencoding=utf-8 ft=python sts=4 sw=4 ts=8 cindent et:
#
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

import sys
import os.path
import argparse
from sqlalchemy.engine.url import make_url
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import exc as sa_exc
from spabademy.database import build_description_url
from spabademy.database.migrations.driver import Driver
from spabademy.database.migrations.driver import PatchFailedException
from spabademy.database.migrations.patch import DirPatchLoader
from spabademy.database.migrations.patch import DirPatchRepositoryLoader
from spabademy import TextUserHostPasswordPrompt

PATCH_REPO_PATH = os.path.join('sql_patches')

def cmd_init(options, repo, driver):
    if options.all_patches:
        driver.init_repo(patches=repo.patches.values())
    else:
        driver.init_repo(patches=repo.lookup_patch_names(options.patches))

def cmd_convert_init(repo, driver, **_):
    from sqlalchemy import Table, Column, Integer, String, MetaData, func, sql

    metadata = MetaData()
    table = Table('migrate_version', metadata,
                Column('repository_id', String, primary_key=True),
                Column('version', Integer),
            )

    sess = driver.sess
    conn = sess.connection()
    if not table.exists(conn):
        print >>sys.stderr, "error: cannot migrate, because table "\
                "'migrate_version' does not exist."
        sys.exit(1)
    print "notice: migration_version table exists"

    res = sess.execute(sql.select([table.c.version],
            table.c.repository_id==driver.repo_name)).fetchone()
    if res is None:
        print >>sys.stderr, "error: cannot migrate, because table "\
                "'migrate_version' contains no reference to the '%s' "\
                "repository." % (driver.repo_name)
        sys.exit(1)

    ver = res[0]
    if ver is None:
        print >>sys.stderr, "error: cannot migrate, because table "\
                "'migrate_version' does not contain our repository id "\
                "'%s'." % (driver.repo_name)
        sys.exit(1)
    print "notice: detected migration version %d" % ver

    num_repos = sess.execute(sql.select([func.count(table.c.repository_id)]))\
            .fetchone()[0]
    if num_repos == 1:
        print "notice: dropping migration_version table"
        table.drop(conn)
    else:
        print "notice: dropping '%s' repository from migration_version "\
                "table" % (driver.repo_name)
        sess.execute(table.delete().where(
                table.c.repository_id==driver.repo_name))

    if ver == 11:
        # We special case this, because our user base is very small and all
        # known repos are at version 11.
        patch_names = [
#            '001_initial',
#            '004_role_check_instead_of_user_check',
#            '005_fix_add_print_account_log_subtotal_and_print_account_update',
#            '006_split_user_create_and_snack_account_creation',
#            '007_fix_user_create_and_add_user_trans_log_subtotaling',
#            '008_user_subtotals_fix',
#            '009_print_accounts_differentiate_between_existing_and_open',
#            '010_add_user_close#',
            '011_add_configuration',
        ]
        patches = repo.lookup_patch_names(patch_names)
    else:
        print >>sys.stderr, "error: do not know how to migrate from "\
                "repository version %d." % ver
        sys.exit(1)

    print "notice: creating new migration metadata"
    driver.init_repo(patches=patches)

    print "notice: conversion complete"

def cmd_uninit(driver, **_):
    driver.uninit_repo()

def list_patches(patches):
    for patch in patches:
        if patch.origin is not None:
            print '* %s' % (patch.name)
        else:
            print '! %s (unknown origin)' % (patch.name)

def cmd_status(driver, **_):
    print "Currently applied patches:"
    applied_patches = driver.applied_patches
    if len(applied_patches) > 0:
        list_patches(applied_patches)
    else:
        print ' None.'

    unapplied_patches = driver.unapplied_patches
    print "Currently unapplied patches:"
    if len(unapplied_patches) > 0:
        list_patches(unapplied_patches)
    else:
        print ' None.'

def cmd_upgrade(options, repo, driver):
    execute_sql = (not options.skip_sql)
    if len(options.patches) > 0:
        driver.upgrade_patches(repo.lookup_patch_names(options.patches),
                execute_sql=execute_sql)
    else:
        driver.upgrade(execute_sql=execute_sql)

def cmd_renew(options, repo, driver):
    driver.renew_patches(repo.lookup_patch_names(options.patches))

def cmd_test(options, repo, driver):
    if not options.simulate:
        print >>sys.stderr, "notice: enforcing simulation"
        options.simulate = True

    if len(options.patches) > 0:
        driver.test_upgrade_patches(repo.lookup_patch_names(options.patches))
    else:
        driver.test_upgrade()

def cmd_downgrade(options, repo, driver):
    execute_sql = (not options.skip_sql)
    if len(options.patches) > 0:
        driver.downgrade_patches(repo.lookup_patch_names(options.patches),
                execute_sql=execute_sql)
    else:
        driver.downgrade(execute_sql=execute_sql)

def cmd_calc_minimal(options, repo, driver):
    minimal_patches = list(driver.calculate_minimal_deps(
            patches=repo.lookup_patch_names(options.patches)))
    minimal_patches.sort(key=lambda p: p.name)
    for patch in minimal_patches:
        print patch.name

def open_engine(url_str):
    """Uses the database URL to connect to the database and - in case some
    credentials are missing - requests these credentials from the user. Returns
    a properly configured engine object.
    """
    try:
        url = make_url(url_str)
        dlg = TextUserHostPasswordPrompt(username=url.username, appname='Garfield',
                host=build_description_url(dbhost=url.host, dbname=url.database,
                        dbport=url.port))
        while True:
            engine = create_engine(url)
            try:
                conn = engine.connect()
                conn.close()
                dlg.password_is_correct()
                # Connection succeeded, return properly configured engine instance.
                return engine
            except sa_exc.OperationalError, e:
                if e.args[0].find('authentication failed') != -1:
                    dlg.clear_password()
                    print >>sys.stderr, "Authentication failed."
                elif e.args[0].find('fe_sendauth: no password supplied') == -1:
                    raise
                if url.username is None:
                    print >>sys.stderr, "Server needs authentication, "\
                            "specify a username."
                    sys.exit(1)
                url.password = dlg.request_password()
    except KeyboardInterrupt:
        print >>sys.stderr, "Received Ctrl-C, exiting."
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
            description='Migrate SQL schemas (and data) from one set of SQL '
            'patches to another set.')
    cmd_parser = parser.add_subparsers(title='migration commands',
            description='the list of migration operations that can be '
            'performed')

    init_parser = cmd_parser.add_parser('init', help='initialise the '
            'repository with the book-keeping tables needed for migration')
    patch_group = init_parser.add_mutually_exclusive_group()
    patch_group.add_argument('--patches', metavar='PATCH', nargs='+',
            help='list of patches that are assumed to be applied already',
            default=[])
    patch_group.add_argument('--all-patches', help='assume that all currently '
            'known patches were applied already', action='store_true',
            default=False)
    init_parser.set_defaults(cmd_func=cmd_init)

    convert_init_parser = cmd_parser.add_parser('convert-init',
            help='convert a repository from the pre-0.15 format to the ' \
            'current repository format, detecting all applied patches')
    convert_init_parser.set_defaults(cmd_func=cmd_convert_init)

    uninit_parser = cmd_parser.add_parser('uninit', help='uninitialise the '
            'repository by removing the book-keeping tables')
    uninit_parser.set_defaults(cmd_func=cmd_uninit)

    status_parser = cmd_parser.add_parser('status', help='list which patches '\
            'are currently applied to the repository')
    status_parser.set_defaults(cmd_func=cmd_status)

    upgrade_parser = cmd_parser.add_parser('upgrade', help='upgrade the '
            'repository by applying additional SQL patches')
    upgrade_parser.add_argument('patches', metavar='PATCH', nargs='*',
            help='list of patches that will be applied (defaults to all '
            'missing patches)', default=[])
    upgrade_parser.add_argument('--skip-sql', help='only modify the metadata '
            'but do not execute the SQL of the patches', action='store_true',
            default=False)
    upgrade_parser.set_defaults(cmd_func=cmd_upgrade)

    test_parser = cmd_parser.add_parser('test', help='test the '
            'specified SQL patch')
    test_parser.add_argument('patches', metavar='PATCH', nargs='*',
            help='list of patches that will be tested (defaults to all '
            'missing patches)', default=[])
    test_parser.set_defaults(cmd_func=cmd_test)

    downgrade_parser = cmd_parser.add_parser('downgrade', help='downgrade the '
            'repository by reverting SQL patches')
    downgrade_parser.add_argument('patches', metavar='PATCH', nargs='*',
            help='list of patches to remove (defaults to all applied patches)',
            default=[])
    downgrade_parser.add_argument('--skip-sql', help='only modify the metadata '
            'but do not execute the SQL of the patches', action='store_true',
            default=False)
    downgrade_parser.set_defaults(cmd_func=cmd_downgrade)

    renew_parser = cmd_parser.add_parser('renew', help='renew the '
            'repository by reverting and then reapplying the specified SQL '
            'patches. Additional patches that needed to be reverted will be '
            're-applied too.')
    renew_parser.add_argument('patches', metavar='PATCH', nargs='+',
            help='list of patches that will be reverted and then reapplied',
            default=[])
    renew_parser.set_defaults(cmd_func=cmd_renew)

    calc_minimal_parser = cmd_parser.add_parser('calc-minimal',
            help='calculcate the minimal set of patches necessary to cause the '
            'listed set of patches to be applied')
    calc_minimal_parser.add_argument('patches', metavar='PATCH', nargs='+',
            help='list of patches for which the minimal set will be determined')
    calc_minimal_parser.set_defaults(cmd_func=cmd_calc_minimal)

    parser.add_argument('--add-repo', help='additional repository of patches '
            'to query', metavar='REPO', dest='repo_paths',
            action='append', default=[])
    parser.add_argument('--simulate', help='rollback all changes afterwards',
            action='store_true', default=False)
    parser.add_argument('url', help='SQL database connection URL')

    options = parser.parse_args()

    engine = open_engine(options.url)
    Session = sessionmaker(bind=engine, autocommit=False)
    sess = Session()

    repo_loader = DirPatchRepositoryLoader(patch_loader=DirPatchLoader())
    repo = repo_loader.load_repo(PATCH_REPO_PATH)
    for repo_path in options.repo_paths:
        override_repo = repo_loader.load_repo(repo_path)
        repo.patches.update(override_repo.patches)
    repo.resolve_dependencies()

    try:
        driver = Driver(sess, repo)
        options.cmd_func(options=options, repo=repo, driver=driver)
        if options.simulate:
            print >>sys.stderr, "notice: simulation option set, rolling back "\
                    "any changes."
            sess.rollback()
        else:
            sess.commit()
    except PatchFailedException, ex:
        print >>sys.stderr, "error: %s" % (ex.args[0])
        if ex.details is not None:
            for detail in ex.details:
                print >>sys.stderr, "error: details: %s" % (detail)
        print >>sys.stderr, "notice: rolling back any changes to the database."
        sess.rollback()
        sys.exit(1)
    except:
        print >>sys.stderr, "notice: rolling back any changes to the database "\
                "due to an error"
        sess.rollback()
        raise

if __name__ == '__main__':
    main()
