# encoding: utf-8
import sys

import psycopg2
import requests
from ckan import model
from ckan.lib import cli
from ckan.plugins import toolkit
from pylons import config

from ckanext.spatialingestor.helpers import log

def ingest_resource(id, _):
    res = toolkit.get_action('resource_show')({'ignore_auth': True}, {'id': id})
    return toolkit.get_action('spatialingestor_ingest_resource')({
        'user': config.get('ckan.spatialingestor.ckan_user', 'default')}, res)


def purge_resource(id):
    try:
        res = toolkit.get_action('resource_show')({'ignore_auth': True}, {'id': id})
    except toolkit.ObjectNotFound:
        return
    return toolkit.get_action('spatialingestor_purge_resource_datastores')({
        'user': config.get('ckan.spatialingestor.ckan_user', 'default')}, res)


class SpatialIngestorCommand(cli.CkanCommand):
    '''Perform commands in the spatialingestor
    Usage:
        purge <pkgname> - Purges spatial child resources from pkgname
        purgeall - Purges spatial child resources from all packages
        reingest <pkgname> - Reingest child resources from pkgname
        reingestall - Reingest all resources from all packages
        purgelegacyall - Purges all artifacts from old spatial ingestor
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def _confirm_or_abort(self):
        question = (
           "Data in any datastore resource that isn't in their source files "
            "(e.g. data added using the datastore API) will be permanently "
            "lost. Are you sure you want to proceed?"
        )

        answer = cli.query_yes_no(question, default=None)
        if not answer == 'yes':
            print "Aborting..."
            sys.exit(0)

    def command(self):
        if self.args and self.args[0] == 'purge':
            if len(self.args) != 2:
                print "This command requires an argument\n"
                print self.usage
                sys.exit(1)

            self._load_config()
            self._purge(self.args[1])
        elif self.args and self.args[0] == 'purgeall':
            self._confirm_or_abort()

            self._load_config()
            self._purge_all()
        elif self.args and self.args[0] == 'reingest':
            self._confirm_or_abort()

            if len(self.args) != 2:
                print "This command requires an argument\n"
                print self.usage
                sys.exit(1)

            self._load_config()
            self._reingest(self.args[1])
        elif self.args and self.args[0] == 'reingestall':
            self._confirm_or_abort()

            self._load_config()
            self._reingest_all()
        elif self.args and self.args[0] == 'purgelegacyall':
            question = (
                "All resources ingested with the old spatial ingestor script "
                "will be permanently deleted. Reingesting with the new spatial "
                "ingestor will provide API endpoints that are named differently "
                "than from the previous resource. Are you sure you want to proceed?"
            )
            self._confirm_or_abort(question)
            self._confirm_or_abort()

            self._load_config()
            self._submit_package(self.args[1])
        else:
            print self.usage

    def _purge(self, pkg_id):
        pkg_dict = model.Package.get(pkg_id).as_dict()

        log.info("Purging spatially ingested resources from package {0}...".format(pkg_dict['name']))

        for res in pkg_dict['resources']:
            purge_resource(res['id'])

    def _purge_all(self):

        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).filter(model.Package.state != 'deleted').all()]

        log.info("Purging spatially ingested resources from all packages...")

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write(
                "\rPurging spatially ingested resources from dataset {0}/{1}".format(counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                for res in pkg_dict['resources']:
                    purge_resource(res['id'])
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        sys.stdout.write("\n>>> Process complete\n")

    def _reingest(self, pkg_id):
        pkg_dict = model.Package.get(pkg_id).as_dict()

        log.info("Re-ingesting spatial resources for package {0}...".format(pkg_dict['name']))

        for res in pkg_dict['resources']:
            ingest_resource(res['id'], False)

    def _reingest_all(self):
        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).filter(model.Package.state != 'deleted').all()]

        log.info("Re-ingesting spatial resources for all packages...")

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write("\rRe-ingesting spatial resources for dataset {0}/{1}".format(counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                for res in pkg_dict['resources']:
                    ingest_resource(res['id'], False)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        sys.stdout.write("\n>>> Process complete\n")

    def _purge_legacy_all(self):
        geoserver_info = cli.parse_db_config('ckan.spatialingestor.postgis_url')

        geoserver_credentials = (geoserver_info['db_user'], geoserver_info['db_pass'])
        geoserver_wsurl = 'http://' + geoserver_info['db_host'] + 'rest/workspaces'

        postgist_info = cli.parse_db_config('ckan.spatialingestor.postgis_url')

        def get_db_cursor():
            try:
                connection = psycopg2.connect(dbname=postgist_info['db_name'],
                                              user=postgist_info['db_user'],
                                              password=postgist_info['db_pass'],
                                              host=postgist_info['db_host'],
                                              port=postgist_info.get('db_port', None))

                connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

                return connection.cursor(), connection
            except Exception, e:
                log.error("Failed to connect with PostGIS with error {0}".format(str(e)))
                return None

        def process_pkg(db_cursor, input_pkg):
            table_name = input_pkg['id'].replace("-", "_")

            results = db_cursor.execute(
                u'''SELECT 1 FROM "_table_metadata" where name = {tab_name} and alias_of is null'''.format(
                    tab_name=table_name))
            res_exists = results.rowcount > 0

            if res_exists:
                log.debug("{0} appears to contain a legacy spatial ingestion.".format(pkg_dict['name']))
                # We have a table that exists in the PostGIS DB
                pkg_raw = model.Package.get(input_pkg['id'])

                if pkg_raw.state != 'deleted':
                    for res_raw in pkg_raw.resources:
                        res_dict = res_raw.as_dict()
                        if "http://data.gov.au/geoserver/" in res_dict.get('url', ''):
                            toolkit.get_action('resource_delete')({'ignore_auth': True}, res_dict)

                res = requests.delete(geoserver_wsurl + '/' + input_pkg['name'] + '?recurse=true&quietOnNotFound',
                                      auth=geoserver_credentials)

                log.info("Geoserver recursive workspace deletion returned {0}".format(res))

                db_cursor.execute("DROP TABLE IF EXISTS {tab_name}".format(tab_name=table_name))

                log.ingo("Dropped SQL table {0}".format(table_name))

        db_res = get_db_cursor()

        if db_res is None:
            log.error("Failed to open SQL connection to PostGIS DB")
            return None

        cursor, connection = db_res

        pkg_ids = [r[0] for r in model.Session.query(model.Package.id).all()]
        log.info("Migrating legacy spatial ingestion on all packages...")

        total_packages = len(pkg_ids)
        for counter, pkg_id in enumerate(pkg_ids):
            sys.stdout.write("\rProcessing dataset {0}/{1}".format(counter + 1, total_packages))
            sys.stdout.flush()
            pkg_dict = model.Package.get(pkg_id).as_dict()
            try:
                process_pkg(cursor, pkg_dict)
            except Exception, e:
                log.error("Processing {0} failed with error {1}, continuing...".format(pkg_dict['name'], str(e)))

        cursor.close()
        connection.close()

        sys.stdout.write("\n>>> Process complete\n")
