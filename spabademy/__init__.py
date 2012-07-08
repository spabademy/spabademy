# vim:set fileencoding=utf-8 ft=python ts=8 sw=4 sts=4 et cindent:
#
# Copyright © 2011  Philipp Kern <pkern@debian.org>
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

import getpass

class AbstractUserHostPasswordPrompt(object):
    def __init__(self, username=None, host=None, appname=None):
        self.appname = appname
        self._username = None
        self.password = None

        self.host = host
        self.username = username

    def _set_username(self, username):
        if username == self._username:
            return
        self._username = username
        if self._username is None:
            self._pwd_in_keyring = False
            return
        if not self.use_keyring:
            return
        if not self._keyring_opened:
            return
        self._query_keyring()
    def _get_username(self):
        return self._username
    username = property(_get_username, _set_username)

    def request_password(self):
        password_ret = self.show_password_prompt()
        if password_ret is None:
            return None
        self.password = password_ret
        return self.password

    def show_password_prompt(self):
        """Displays a password prompt to the user and returns the entered
        password. Return ``None`` in case the user aborted the password entry.
        """
        raise NotImplementedError(
                'Implement `show_password_prompt` in child class')

    def password_is_correct(self):
        return

    def clear_password(self):
        self.password = None

    def is_password_stored(self):
        return False

class TextUserHostPasswordPrompt(AbstractUserHostPasswordPrompt):
    def show_password_prompt(self):
        pwd = getpass.getpass()
        if '\x03' in pwd:
            # Work-around http://bugs.python.org/issue11236 which only
            # affects Python 2.6 now.
            raise KeyboardInterrupt
        return pwd
