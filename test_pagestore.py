import unittest
from os.path import exists
import os
from pagestore import _col_select, PageStore
from sqlite3 import connect, InterfaceError

_DB = '/tmp/test.db'
def dump_db():
    # dump a copy of the SQL, so we can check it in case of insanity...
    if exists(_DB + '.dump.sql'):
        os.remove(_DB + '.dump.sql')
    with open(_DB + '.dump.sql', 'w') as f:
        db = connect(_DB)
        [f.write(x) for x in db.iterdump()]

##################################
# Some daft demo data:

choc = {'key':'chocolate','html':'<b>choc</b>',
        'json': '"chocolates!"','fulltext':'yummy chocolate',
        'tags':['food', 'yum', 'unhealthy', 'processed']}

mango = {'key': 'mango', 'html': '<i>MANGO!</i>',
         'json':'["philippines","has","mango"]',
         'fulltext':'mango fruit smoothies in the philippines are the best.',
         'tags': ('food', 'yum', 'fruit', 'healthy')}

durian = {'key': 'durian', 'html':'The KING of fruits!',
          'json':'"el grande spikeyfruit"',
          'fulltext': "I'm not such a fan of durian, but hey, some love this crazy fruit",
          'tags': ['food', 'yuck', 'fruit', 'healthy']}

food = [choc, mango, durian]

#################################
# Test internals:

class TestColSelect(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(_col_select(),'SELECT id,key,html,json ')

    def test_json_only(self):
        self.assertEqual(_col_select('json'),'SELECT json ')

    def test_invalid_column_name(self):
        with self.assertRaises(AssertionError):
            _col_select('invalid')

#################################
# Test interfaces:

class TestBasicPageStore(unittest.TestCase):
    def setUp(self):
        assert not exists(_DB)

    def tearDown(self):
        assert exists(_DB)
        os.remove(_DB)

    def test_initialise(self):
        with PageStore(_DB) as c:
            c.initialise()

    def test_sqlinject_attempt(self):
        with PageStore(_DB) as c:
            c.initialise()
            with self.assertRaises(AssertionError):
                c.get_by_key('none', '; DROP tags;')

    def test_store_and_restore(self):
        ''' first check the store is, with sane data, working basically
            as expected... '''
        with PageStore(_DB) as c:
            c.initialise()

            # store a single basic row:
            c.store('basic key', '<html>', '{"json":"column"}',
                    'full searchable text', ('tag1', 'tag2'))

            # check tags are correct:
            self.assertEqual(c.all_tags(), ['tag1', 'tag2'])

            # check get key/html/json is good:
            self.assertEqual(c.get_by_key('basic key',('key', 'html', 'json')),
                             ('basic key', '<html>', '{"json":"column"}'))

            # check key only is good:
            self.assertEqual(c.get_by_key('basic key', 'key'), 'basic key')

            # check html only is good:
            self.assertEqual(c.get_by_key('basic key', 'html'), '<html>')

            # check json only is good:
            self.assertEqual(c.get_by_key('basic key', 'json'), '{"json":"column"}')

            # check fts:
            self.assertEqual(c.search('searchable', 'html'), ['<html>'])

            # check tags return item:
            self.assertEqual(c.get_by_tag('tag1', 'html'), ['<html>'])
            self.assertEqual(c.get_by_tag('tag2', 'html'), ['<html>'])

            # check multi-tag select:
            self.assertEqual(c.get_by_tags('tag1', 'html'), ['<html>'])
            self.assertEqual(c.get_by_tags(('tag1', 'tag2'), 'html'), ['<html>'])
            self.assertEqual(c.get_by_tags('tag1', 'html', 'tag2'), [])

class TestMediumPageStore(unittest.TestCase):
    def setUp(self):
        assert not exists(_DB)
        with PageStore(_DB) as c:
            c.initialise()

            for row in (choc, mango, durian):
                c.store(row['key'], row['html'], row['json'],
                        row['fulltext'], row['tags'])

    def tearDown(self):
        assert exists(_DB)
        os.remove(_DB)


    def test_all_tags(self):
        with PageStore(_DB) as c:
            # there are no options:
            self.assertEqual(sorted(c.all_tags()),
                sorted(['processed','food','yum','unhealthy',
                        'fruit', 'healthy','yuck']))


    def test_all_pages(self):
        with PageStore(_DB) as c:
            # no limit:
            self.assertEqual(
                sorted(c.all_pages('key')),
                sorted([i['key'] for i in food]))
            # single limit:
            self.assertEqual(
                sorted(c.all_pages('key',1)),
                sorted([food[0]['key']]))
            # get different columns:
            self.assertEqual(
                sorted(c.all_pages(['key','json'])),
                sorted([(i['key'], i['json']) for i in food]))


    def test_search(self):
        with PageStore(_DB) as c:
            # normal:
            self.assertEqual(c.search('yummy', 'key'), ['chocolate'])

            # empty:
            self.assertEqual(c.search(None, 'key'), [])
            self.assertEqual(c.search('', 'key'), [])
            self.assertEqual(c.search(''), [])

            # multiple:
            self.assertEqual(c.search('fruit', 'key'), ['mango', 'durian'])

            # not there:
            self.assertEqual(c.search('coconut'), [])

            # TODO: search match with * and so on...


    def test_get_by_key(self):
        with PageStore(_DB) as c:
            # get normal
            self.assertEqual(c.get_by_key('chocolate', 'key'), 'chocolate')

            # try to get non-existant key
            self.assertEqual(c.get_by_key('not here', 'key'), None)

            # try empty key:
            self.assertEqual(c.get_by_key('', 'key'), None)

            # try other types:
            self.assertEqual(c.get_by_key(None, 'key'), None)
            with self.assertRaises(InterfaceError):
                self.assertEqual(c.get_by_key(lambda: 7+0, 'key'), None)
            self.assertEqual(c.get_by_key(42, 'key'), None)

            # try to corrupt the database
            with self.assertRaises(AssertionError):
                c.get_by_key(';DROP page;', ';DROP tag;')

            # try to get a mango (in case the db /did/ corrupt...
            self.assertEqual(c.get_by_key('mango', 'key'), 'mango')

    def test_get_tags_of_page(self):
        with PageStore(_DB) as c:
            # get normal
            self.assertEqual(sorted(c.get_tags_of_page('chocolate')),
                             sorted(choc['tags']))

            # get non-existant-page
            self.assertEqual(c.get_tags_of_page('souvlakia'), [])

            # get page which has no tags
            c.store('banoffie','<banoffie>','["banana","toffie"]',
                    "yum in a pie", [])

            self.assertEqual(c.get_tags_of_page('banoffie'), [])

    def test_get_by_tag(self):
        with PageStore(_DB) as c:
            # basic get (multiple rows):
            self.assertEqual(c.get_by_tag('yum'), [choc['json'],mango['json']])

            # get key (non-default columns):
            self.assertEqual(c.get_by_tag('healthy','key'),
                             ['mango', 'durian'])

            # get non-existant tag:
            self.assertEqual(c.get_by_tag('plasticky'), [])

            # empty tag:
            self.assertEqual(c.get_by_tag(''), [])

    def test_get_by_tags(self):
        with PageStore(_DB) as c:
            # get empty taglist
            self.assertEqual(c.get_by_tags([]), [])

            # get single tag -> single page
            self.assertEqual(c.get_by_tags('unhealthy'), [choc['json']])

            # get single tag -> multiple pages
            self.assertEqual(c.get_by_tags('fruit', 'key'),
                             [mango['key'], durian['key']])

            # get multiple valid tags which return one page
            self.assertEqual(c.get_by_tags(['processed','unhealthy']),
                             [choc['json']])

            # get multiple valid tags which return multiple pages
            self.assertEqual(c.get_by_tags(['fruit','healthy'],'html'),
                [mango['html'],durian['html']])

            # get non-existant tag
            self.assertEqual(c.get_by_tags('yellowy-pink'),[])

            # get non-str. tags:
            with self.assertRaises(TypeError):
                self.assertEqual(c.get_by_tags(42),[])

            with self.assertRaises(TypeError):
                c.get_by_tags(lambda z: z + z)

            # get valid & invalid tags
            self.assertEqual(c.get_by_tags(('fruit','mouldy')),
                [mango['json'], durian['json']])

    def test_purge_single(self):
        with PageStore(_DB) as c:
            # get two pages, check they exists first.
            self.assertEqual(c.get_by_key('durian'), durian['json'])
            self.assertEqual(c.get_by_key('chocolate'), choc['json'])

            # run purge on durian ONLY
            c.purge('durian')

            # check that the page is gone.
            self.assertEqual(c.get_by_key('durian'), None)

            # check that other pages still exist...
            self.assertEqual(c.get_by_key('chocolate'), choc['json'])

    def test_purge_all(self):
        with PageStore(_DB) as c:
            # get two pages, check they exists first.
            self.assertEqual(c.all_pages('html'),
                [x['html'] for x in food])

            # run purge on durian ONLY
            c.purge(everything=True)

            # check that all pages are gone.
            self.assertEqual(c.all_pages('html'), [])

    def test_store(self):
        # should be done already, basically
        # TODO
        pass

    def test_update_basic(self):
        with PageStore(_DB) as c:
            # page exists, and has old values:
            self.assertEqual(c.get_by_key('chocolate',('key','html','json')), \
                    (choc['key'], choc['html'], choc['json']))

            # update:
            c.update('chocolate','<!-- -->', '[1,2,3]',
                     'stuff changed', choc['tags'])

            # now has new values!
            self.assertEqual(c.get_by_key('chocolate', ('key','html','json')), \
                    ('chocolate', '<!-- -->', '[1,2,3]'))

    def test_update_key_changed(self):
        with PageStore(_DB) as c:
            # page exists, and has old values:
            self.assertEqual(c.get_by_key('chocolate',('key','html','json')), \
                    (choc['key'], choc['html'], choc['json']))

            # check search (fts) finds it:
            self.assertEqual(c.search('chocolate','key'), ['chocolate'])

            # update:
            c.update('chocolate cake','<!-- -->', '[1,2,3]',
                     'stuff changed', choc['tags'], old_key='chocolate')

            # check old key is gone:
            self.assertEqual(c.get_by_key('chocolate'), None)

            # check new row exists:
            self.assertEqual(c.get_by_key('chocolate cake', ('key','html','json')), \
                    ('chocolate cake', '<!-- -->', '[1,2,3]'))

            # check fts no longer finds it, but finds new one:
            self.assertEqual(c.search('chocolate','key'), [])
            self.assertEqual(c.search('stuff','key'), ['chocolate cake'])

    def test_update_tags_changed(self):
        with PageStore(_DB) as c:
            # page exists, and has old values:
            self.assertEqual(c.get_by_key('chocolate',('key','html','json')), \
                    (choc['key'], choc['html'], choc['json']))

            # can be found by tag:
            self.assertEqual(c.get_by_tag('yum'),[choc['json'],mango['json']])

            # update:
            c.update('chocolate cake','<!-- -->', '[1,2,3]',
                     'stuff changed', ['we','all','lived','in','a','yellow'], old_key='chocolate')

            # can no longer be found by old tag:
            self.assertEqual(c.get_by_tag('yum'),[mango['json']])

            # but new tags work:
            self.assertEqual(c.get_by_tag('lived'),['[1,2,3]'])
