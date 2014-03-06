import os
import sys
import time
import site
from datetime import datetime
from fnmatch import fnmatch
site.addsitedir(os.path.dirname(os.path.abspath(__file__)) + '/basex-api/src/main/python')

from optparse import OptionParser
import logging

from BaseXClient import *

DB_HOST = 'localhost'
DB_NAME = 'FULLTEXT_XML'
DB_PORT = 1984
DB_USER = 'admin'
DB_PASS = 'admin'

def init_logging(logfile, verbose=False, debug=False):
    logfile = logfile + "." + datetime.now().strftime("%Y%m%d-%H%M%S")
    logging.basicConfig(
        filename = logfile, 
        level = logging.INFO,
        format = '%(asctime)s %(levelname)s %(message)s'
    )
    log = logging.getLogger()
    log.debug("logging to %s" % logfile)
    if verbose:
        log.addHandler(logging.StreamHandler(sys.stdout))
        log.debug("logging to stdout")
    if debug:
        log.setLevel(logging.DEBUG)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(thread)d %(filename)s %(lineno)d %(message)s')
        for h in log.handlers:
            h.setFormatter(fmt)
        log.debug("debug level logging enabled")
    return log

def main(opts):

    log = logging.getLogger()

    basex = Session(DB_HOST, DB_PORT, DB_USER, DB_PASS)
    basex.execute('open %s' % DB_NAME)
    basex.execute('set INTPARSE')
     
    def db_entry_exists(path):
        
        xquery = 'db:list("%s/%s")' % (DB_NAME, path)
        log.debug("issuing %s" % xquery)
        q = basex.query(xquery)
        result = q.execute()
        return len(result) and True or False

    def add_file(pattern, dir, files):
        for filename in files:
            log.debug("working on %s" % filename)
            if fnmatch(filename, pattern):
                file_path = os.path.join(dir, filename)
                db_entry = '-'.join(file_path.split('/')[-3:])
                if not opts.force and db_entry_exists(db_entry):
                    log.debug("%s already exists; skipping." % db_entry)
                    return
                log.info("adding %s as %s" % (file_path, db_entry))
                try:
                    basex.execute("add as %s %s" % (db_entry, file_path))
                except Exception, e:
                    log.error("Error adding %s: %s" % (file_path, e))
 
    log.info("walking %s looking for files matching %s" % (opts.root_dir, opts.pattern))
    os.path.walk(opts.root_dir, add_file, opts.pattern)
    print "done."

if __name__ == '__main__':
    
    op = OptionParser()
    op.set_usage("usage: index.py [options] ")
    op.add_option('--verbose', dest='verbose', action='store_true',
        help='write log output to stdout', default=False)
    op.add_option('--debug', dest='debug', action='store_true',
        help='include debugging info in log output', default=False)
    op.add_option('--force', dest='force', action='store_true',
        help='add documents even if they exist', default=False)
    op.add_option('--pattern', dest='pattern', action='store',
        help='pattern to match against file names', default='*.xml')
    op.add_option('--root_dir', dest='root_dir', action='store',
        help='the root directory to walk/search for xml')
    op.add_option('--logfile', dest='logfile', action='store',
        help='write to this logfile', 
        default='%s/logs/load_fulltext.log' % os.path.dirname(os.path.abspath(__file__)))
    opts, args = op.parse_args()

    log = init_logging(opts.logfile, verbose=opts.verbose, debug=opts.debug)

    start_cpu = time.clock()
    start_real = time.time()

    main(opts)

    end_cpu = time.clock()
    end_real = time.time()

    print "%f Real Seconds" % (end_real - start_real)
    print "%f CPU Seconds" % (end_cpu - start_cpu)
    print


