'''
    pagestore.py (C) 2013 Daniel Fairhead
    ---------------------------------------------------
    The PageStore class, for caching webpage type data into a sqlite database.

    The idea is that for each page/page/whatever, you send the PageStore:
    - a key
    - a JSON represenation of the page
    - a searchable 'fulltext' (which is indexed and disgarded)
    - some HTML, if you want.
    - some tags

    the HTML can either store the full rendered page, if you aren't simply
    serving them as static assets, or the segment that you want to display in
    a search page, or in a tag/category listing page, or whatever.

    the JSON object would normally (for a modern ajaxy sort of site) be enough -
    you should be able to throw the JSON at your javascript client side search
    page, which is then rendered at the viewers end.

    alternatively, you could always read the json object back into a python
    object to manipulate and templatify serverside...

    tags are also searchable, listable, and all the rest of it. You can request
    all pages (either in json, key, or html formats) by tag.

    Note that tags are *not* automatically added to your full text search
    index - you should do this manually before sending the store your fulltext.
    This is so that you can add your own weird and wonderful server-side-only
    tags, if you want.

    Also note that by default, the db is run with synchronous OFF - which
    should be faster but also means there is a small chance if the OS crashes
    during a write, the db has a 'small chance' it could get corrupted.
    The normal usecase for this is that it's a CACHE.  So just regenerate it.

    =================================================
    TODO:
    =================================================

    - Some kind of ranking sort thingy.  Either as a registered sqlite function,
      which would probably work.  See

      http://chipaca.com/post/16877190061/doing-full-text-search-in-sqlite-from-python

      or perhaps it would actually be faster/easier????  to grab all the rows,
      and sort once we've got it back to python land?  I can't imagine that
      all those calls back and forth (with python's well known
      function-call-tax) will be fast... then again, premature optimisation....

'''
# Other Notes:

# when querying, some methods have a 'columns' arg, which you can specify
# which things you want ('json','key') etc.
# There /is/ an assert which checks that you are requesting valid
# column names, which somewhat prevents sql injection - BUT, only if this
# assert is run.  So if you run python -O, and so dump the
# asserts, you lose this safety.  But you don't accept
# random column names from your untrusted clients anyway, right?


# from itertools import islice # there was a reason for this.
import sqlite3 as lite
import logging


####################################################
#
# Schemas:
#

_CONTENT_TABLE_SQL = \
    u'''CREATE TABLE 'page'
         (id INTEGER PRIMARY KEY,
          key TEXT UNIQUE ON CONFLICT IGNORE NOT NULL,
          html TEXT,
          json TEXT)
    '''

_FTS_TABLE_SQL = \
    u''' CREATE VIRTUAL TABLE 'pagefts' USING FTS4
            (fulltext)
     '''

_TAGS_TABLE_SQL = \
    u''' CREATE TABLE 'tag'
            (id INTEGER PRIMARY KEY,
              name TEXT UNIQUE ON CONFLICT IGNORE)
     '''

_TAGS_XREF_SQL = \
    u'''CREATE TABLE 'tagxref'
           (tagid INTEGER NOT NULL,
            pageid INTEGER NOT NULL,
            FOREIGN KEY(tagid) REFERENCES tag(id) ON DELETE CASCADE,
            FOREIGN KEY(pageid) REFERENCES page(id) ON DELETE CASCADE)
     '''

# valid column names (for asserts)

_VALID_COLUMNS = (u'id', u'key', u'html', u'json')

def _col_select(columns=_VALID_COLUMNS, query=''):
    ''' checks $columns is a valid option, and returns a
        u'SELECT x,y,z' query from it. appends $query on the end.
        Saves a lot of boilerplate & potential mistakes. DRY. '''
    t = type(columns)
    if t == unicode or t == str:
        assert columns in _VALID_COLUMNS
        return u' '.join((u'SELECT', columns, query))
    else:
        assert all((False for c in columns if c not in _VALID_COLUMNS))
        return u' '.join((u'SELECT', u','.join(columns), query))

def _qs(items):
    ''' returns a list of '?' for each item in $items, for use in queries. '''
    return u','.join((u'?' for _ in items)) # ?, ?, ...

#####################################################
#
# PageStore:
#

class PageStore(object):
    ''' A reasonably simple sqlite based searchable Page storing object.
        It stores the following values:
        - key
        - html (full rendered page html)
        - json (quick representation for use in ajax type queries)
        - tags (also searchable)

        there is also a FTS(full text search) table, which should get given a
        'pure text' version of the page, with whatever other data you want to be
        thrown in there (so including comments, media info, whatever).  This is
        *NOT* supposed to be retrievable, but is used simply to point at the
        appropriate 'real' data.

        The idea of this object is for fast searching, rather than for direct
        display purposes, so data is stored in here as plain text, not as
        rendered HTML.  The HTML cache should be stored elsewhere - usually
        just as plain .html files in a static directory for easy serving (or
        handled by Varnish or Redis or whatever).
        '''

    changed = False

    def __init__(self, db_filename=':memory:', synchronous='OFF'):

        self.log = logging.getLogger(__name__)
        self.log.addHandler(logging.NullHandler())
        self.log.debug('Loading SQLite database: %s', db_filename)
        self.connection = lite.connect(db_filename)
        self.cur = self.connection.cursor()
        # perhaps we should remove this later on?
        # theoretically, if people only use this class, and our unit tests are
        # solid, then no run-time foreign key checks are really needed...
        # One day doing some performance checks would be a good idea:
        self.cur.execute(u'PRAGMA foreign_keys = ON')

        # this should make things even faster, for our usual usecase.
        # I suppose we could turn synchronous ON before write-type operations?
        assert synchronous in ('ON','OFF')
        self.cur.execute(u'PRAGMA synchronous = ' + unicode(synchronous))

    def initialise(self):
        ''' Initialises a new database,
            sets up the tables with the right schemas '''
        self.changed = True

        self.log.debug('Initialising new tables from schema')

        self.cur.execute(_CONTENT_TABLE_SQL)

        self.cur.execute(_FTS_TABLE_SQL)

        self.cur.execute(_TAGS_TABLE_SQL)

        self.cur.execute(_TAGS_XREF_SQL)

    def __enter__(self):
        ''' called with the 'with' pattern. '''
        return self

    def __exit__(self, exptype, expvalue, exptb):
        ''' leave a 'with' block '''

        # TODO: exception handling roll back?

        if self.changed:
            self.log.debug('Comitting Changes to database')
            self.connection.commit()

        self.log.debug('Closing database.')
        self.connection.close()

    def execute(self, query, *values):
        ''' run a query with the cursor, simpler.
            Doesn't require tupling everything. '''
        try:
            self.log.debug('Running SQL query: %s; Values: %s', query, values)
            return self.cur.execute(query, values)
        except Exception as e:
            self.log.error('SQL Error in Query: %s; Values: %s', query, values)
            raise e

    def _return_columns(self, columns, query, *values):
        ''' wrapper for execute & fetchall, which then strips single column
            return lists into straight lists. [('x',),('y',)] -> ['x','y']
            Saves boilerplate.'''

        t = type(columns)
        if t == unicode or t == str:
            return [x[0] for x in self.execute(query, *values).fetchall()]
        else:
            return self.execute(query, *values).fetchall()

    def all_pages(self, columns=u'json', limit=-1):
        ''' get a list of all pages '''

        return self._return_columns(columns,
                                    _col_select(columns, \
                                             u'FROM page LIMIT ?'), int(limit))

    def all_tags(self):
        ''' get a list of all tags '''
        return [t[0] for t in \
                self.cur.execute(u"SELECT name FROM tag").fetchall()]


    def search(self, needle, columns=u'json', limit=-1, ):
        ''' do a full text search for $needle,
            and return whichever columns you ask for. '''

        query = _col_select(columns, u'FROM page WHERE id IN' \
                u' (SELECT docid FROM pagefts WHERE fulltext MATCH ? ' \
                u'  LIMIT ? )')

        return self._return_columns(columns, query, needle, int(limit))


    def get_by_key(self, key, columns=u'json'):
        ''' retrieve an page by key '''

        item = self.execute(_col_select(columns, u'FROM page WHERE key = ?'), \
                           key).fetchone()
        t = type(columns)
        if item is None:
            return None
        elif t is str or t is unicode:
            return item[0]
        else:
            return item

    def get_tags_of_page(self, key):
        return [x[0] for x in self.execute(
                    u"SELECT tag.name FROM tag, tagxref"
                    u" WHERE tagxref.tagid = tag.id "
                    u"   AND tagxref.pageid ="
                    u"   (SELECT id FROM page WHERE key = ?)", key).fetchall()]
    # TODO: get_by_keys (with LIKE, !=, etc...)

    def get_by_tag(self, tag, columns=u'json'):
        ''' retrieve a list of pages by tag '''

        query = _col_select(columns,
                u" FROM page, tag, tagxref " \
                u" WHERE tag.name == ?" \
                u"   AND tagxref.pageid == page.id" \
                u"   AND tagxref.tagid == tag.id")

        return self._return_columns(columns, query, tag)

    def get_by_tags(self, tags, columns=u'json', exclude=()):
        ''' gets all pages which have *any* of the tags listed.
            there is an exclude option too. '''
        # I know, I know, isinstance considered harmful. However,
        # this is the simplist way to do it:
        # (allow tags or exclude to be strings, or lists/tuples)
        if isinstance(tags, str) or isinstance(tags, unicode):
            tags = (tags,)
        elif type(tags) is not tuple:
            tags = tuple(tags)

        if isinstance(exclude, str) or isinstance(exclude, unicode):
            exclude = (exclude,)
        elif type(exclude) is not tuple:
            exclude = tuple(exclude)

        # I feel sure there should be a way to do this with JOINs, which
        # might be quicker...
        query = _col_select(columns,
            u" FROM page " \
            u" WHERE page.id IN " \
            u"           ( SELECT pageid from tag, tagxref " \
            u"              WHERE tag.name IN ( {0} )" \
            u"                AND tagxref.tagid == tag.id) " \
            u" AND page.id NOT IN " \
            u"           ( SELECT pageid from tag, tagxref " \
            u"              WHERE tag.name IN ( {1} ) " \
            u"                AND tagxref.tagid == tag.id)".format(
            _qs(tags), _qs(exclude)))

        return self._return_columns(columns, query, *(tags + exclude))


    def purge(self, page_key=False, everything=False):
        ''' clear either one page(by key) or the whole cache. '''
        self.changed = True

        if page_key:
            self.execute(u"DELETE FROM 'page' WHERE key == ?", page_key)
            # this AUTOMATICALLY (due to SQL coolness)
            # should delete tagxrefs too...
            # TODO: think about checking here for unused tags?

        if everything:
            # possible slight performance hit - both here and in 'initialise'
            # we check IF EXISTS on all tables.  Should be negligable, though.
            self.execute(u"DROP TABLE IF EXISTS tagxref")
            self.execute(u"DROP TABLE IF EXISTS tag")
            self.execute(u"DROP TABLE IF EXISTS page")
            self.execute(u"DROP TABLE IF EXISTS pagefts")
            # we could possibly do all this just as one single executescript
            # command? since this should happen so rarely, it's almost certainly
            # not worth it...
            self.initialise()

    def create_tags(self, tags):
        ''' create any new tags needed from $tags list '''
        self.changed = True
        self.cur.executemany('INSERT OR IGNORE INTO tag(name) VALUES(?)',
            ((t,) for t in tags))

    def _link_tags(self, page, tags):
        ''' create any needed xref links for page<->tag '''
        self.changed = True
        # (a bit ugly python)
        self.execute(u'INSERT INTO tagxref(tagid, pageid) ' \
                     u'  SELECT rowid, ? FROM tag WHERE name IN (' \
                         + _qs(tags) + u')', # ?, ?, ...
                     page, *tags)

    def store(self, key, html, json, fulltext, tags):
        ''' store an page in the store, including setting up the searchable
            text and tags '''
        self.changed = True

        # write main page:
        self.execute(u"INSERT INTO page(key, html, json) VALUES(?, ?, ?)",
                     key, html, json)

        # get new page id:
        rowid = self.cur.lastrowid

        # add full searchable text:
        self.execute(u"INSERT INTO pagefts(docid, fulltext) VALUES(?, ?)",
                     rowid, fulltext)

        # create any new needed tags:
        self.create_tags(tags)

        # link page to tags:
        self._link_tags(rowid, tags)


#    def store_many(self, generator):
#        ''' You give this function a generator (or list) which has in it tuples
#            (or lists) in the format:
#            (key, html, json, fulltext, tags)
#            which will then be written (fast) to the database.  More performant
#            than calling store(...) hundreds of times. '''
#        self.changed = True
#        # TODO
#        pass

    def update(self, key, html, json, fulltext, tags, old_key=None):
        ''' Update an already stored page (found by key).
            If you want to update the key, use old_key to specify the
            previous key.
            If the key is not found, then a new page will be added. '''
        self.changed = True

        # first get the appropriate id:
        self.execute(u"SELECT id FROM page WHERE key = ?", \
                     old_key if old_key else key)
        docid = self.cur.fetchone()

        # doesn't exist already, so write new entry.
        if not docid:
            return self.store(key, html, json, fulltext, tags)
        else:
            docid = docid[0]

        # update the main table:
        self.execute(u"UPDATE page SET key=?, html=?, json=? WHERE id=?",
                     key, html, json, docid)

        # update the fts table
        self.execute(u"UPDATE pagefts SET fulltext=? WHERE docid=?",
                     fulltext, docid)

        # update the tags table
        self.create_tags(tags)
        # TODO: think about deleting unused tags

        # update the tagxref table:
        self.execute(u'DELETE FROM tagxref WHERE pageid=?', docid)
        self._link_tags(docid, tags)
