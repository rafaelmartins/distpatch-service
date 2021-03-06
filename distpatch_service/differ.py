# coding: utf-8

import os
os.environ.setdefault('DISTPATCH_CELERY_CONFIG',
                      'distpatch_service.base_settings')
os.environ['ACCEPT_KEYWORDS'] = '**'

from celery import Celery

celery = Celery('distpatch_service.differ')
celery.add_defaults(dict(CELERY_ACCEPT_CONTENT = ['pickle'],
                         CELERY_MESSAGE_COMPRESSION = 'gzip',
                         CELERY_ENABLE_UTC = True))
celery.config_from_envvar('DISTPATCH_CELERY_CONFIG')

if 'DISTPATCH_DIST_DIR' in celery.conf:
    os.environ['DISTDIR'] = celery.conf['DISTPATCH_DIST_DIR']


@celery.task
def diff(package):
    from distpatch.deltadb import DeltaDB
    from distpatch.diff import DiffExists
    from distpatch.package import Package

    print '[package] %s' % package

    output_dir = celery.conf.get('DISTPATCH_OUTPUT_DIR', '/tmp/deltas')
    db_file = os.path.join(output_dir, 'delta.db')

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir, 0755)

    db = DeltaDB(db_file)

    pkg = Package(db)
    pkg.diff(package)

    if len(pkg.diffs) == 0:
        return

    pkg.fetch_distfiles()

    for diff in pkg.diffs:
        print '[diff] %s %r' % (package, diff)
        try:
            diff.generate(output_dir, True, True, False)
        except DiffExists:
            print '[diff: exists] %s %r' % (package, diff)
        else:
            print '[diff: generated] %s %r' % (package, diff)
            db.add(diff.dbrecord)
            print '[diff: added to db] %s %r' % (package, diff)
        diff.cleanup()
        print '[diff: done] %s %r' % (package, diff)

    print '[package: done] %s' % package


if __name__ == '__main__':
    celery.worker_main()
