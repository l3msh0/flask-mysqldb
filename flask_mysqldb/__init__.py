import MySQLdb
from MySQLdb import cursors
from flask import _app_ctx_stack
from DBUtils.PooledDB import PooledDB

class MySQL(object):

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """Initialize the `app` for use with this
        :class:`~flask_mysqldb.MySQL` class.
        This is called automatically if `app` is passed to
        :meth:`~MySQL.__init__`.

        :param flask.Flask app: the application to configure for use with
            this :class:`~flask_mysqldb.MySQL` class.
        """

        app.config.setdefault('MYSQL_HOST', 'localhost')
        app.config.setdefault('MYSQL_USER', None)
        app.config.setdefault('MYSQL_PASSWORD', None)
        app.config.setdefault('MYSQL_DB', None)
        app.config.setdefault('MYSQL_PORT', 3306)
        app.config.setdefault('MYSQL_UNIX_SOCKET', None)
        app.config.setdefault('MYSQL_CONNECT_TIMEOUT', 10)
        app.config.setdefault('MYSQL_READ_DEFAULT_FILE', None)
        app.config.setdefault('MYSQL_USE_UNICODE', True)
        app.config.setdefault('MYSQL_CHARSET', 'utf8')
        app.config.setdefault('MYSQL_SQL_MODE', None)
        app.config.setdefault('MYSQL_CURSORCLASS', None)

        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)


        kwargs = {}

        if app.config['MYSQL_HOST']:
            kwargs['host'] = app.config['MYSQL_HOST']

        if app.config['MYSQL_USER']:
            kwargs['user'] = app.config['MYSQL_USER']

        if app.config['MYSQL_PASSWORD']:
            kwargs['passwd'] = app.config['MYSQL_PASSWORD']

        if app.config['MYSQL_DB']:
            kwargs['db'] = app.config['MYSQL_DB']

        if app.config['MYSQL_PORT']:
            kwargs['port'] = app.config['MYSQL_PORT']

        if app.config['MYSQL_UNIX_SOCKET']:
            kwargs['unix_socket'] = app.config['MYSQL_UNIX_SOCKET']

        if app.config['MYSQL_CONNECT_TIMEOUT']:
            kwargs['connect_timeout'] = \
                app.config['MYSQL_CONNECT_TIMEOUT']

        if app.config['MYSQL_READ_DEFAULT_FILE']:
            kwargs['read_default_file'] = \
                app.config['MYSQL_READ_DEFAULT_FILE']

        if app.config['MYSQL_USE_UNICODE']:
            kwargs['use_unicode'] = app.config['MYSQL_USE_UNICODE']

        if app.config['MYSQL_CHARSET']:
            kwargs['charset'] = app.config['MYSQL_CHARSET']

        if app.config['MYSQL_SQL_MODE']:
            kwargs['sql_mode'] = app.config['MYSQL_SQL_MODE']

        if app.config['MYSQL_CURSORCLASS']:
            kwargs['cursorclass'] = getattr(cursors, app.config['MYSQL_CURSORCLASS'])

        self.pooled_db = PooledDB(
            creator=MySQLdb,  # Modules using linked databases
            maxconnections=1024,  # Maximum number of connections allowed by connection pool, 0 and None denote unrestricted number of connections
            mincached=1024,  # At the time of initialization, at least an idle link is created in the link pool. 0 means no link is created.
            maxcached=0,  # The maximum number of idle links in the link pool, 0 and None are unrestricted
            maxshared=0,  # The maximum number of shared links in the link pool, 0 and None represent all shared links. PS: It's useless, because the threadsafe of modules like pymysql and MySQLdb are all 1. No matter how many values are set, _maxcached is always 0, so all links are always shared.
            blocking=True,  # If there is no connection available in the connection pool, whether to block waiting. True, wait; False, don't wait and report an error
            maxusage=None,  # The maximum number of times a link is reused. None means unlimited.
            setsession=[],  # A list of commands executed before starting a session. For example: ["set datestyle to...", "set time zone..."]
            ping=0,
            # ping MySQL On the server side, check if the service is available.# For example: 0 = None = never, 1 = Default = when it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            **kwargs
        )

    @property
    def connect(self):
        return self.pooled_db.connection()

    @property
    def connection(self):
        """Attempts to connect to the MySQL server.

        :return: Bound MySQL connection object if successful or ``None`` if
            unsuccessful.
        """

        ctx = _app_ctx_stack.top
        if ctx is not None:
            if not hasattr(ctx, 'mysql_db'):
                ctx.mysql_db = self.connect
            return ctx.mysql_db

    def teardown(self, exception):
        ctx = _app_ctx_stack.top
        if hasattr(ctx, 'mysql_db'):
            ctx.mysql_db.close()
