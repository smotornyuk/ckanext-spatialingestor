import logging

from ckan.lib import cli
from ckan import model
from ckan.plugins import toolkit
from pylons import config

log = logging.getLogger('ckanext_spatialingestor')


def get_microservice_metadata():
    for config_option in ('ckan.spatialingestor.postgis_url', 'ckan.spatialingestor.internal_geoserver_url',):
        if not config.get(config_option):
            raise Exception(
                'Config option `{0}` must be set to use the SpatialIngestor.'.format(config_option))

    core_url = config.get('ckan.site_url', 'http://localhost:8000/')
    return {'postgis': cli.parse_db_config('ckan.spatialingestor.postgis_url'),
            'geoserver': cli.parse_db_config('ckan.spatialingestor.internal_geoserver_url'),
            'geoserver_public_url': config.get('ckan.spatialingestor.public_geoserver_url',
                                               core_url + '/geoserver'),
            'target_spatial_formats': list(set([x.upper() for x in toolkit.aslist(config.get('ckan.spatialingestor.target_formats', []))]))
            }


def is_resource_blacklisted(resource):
    package = toolkit.get_action('package_show')({'ignore_auth': True}, {
        'id': resource['package_id'],
    })

    org_blacklist = list(set(toolkit.aslist(config.get('ckan.spatialingestor.org_blacklist', []))))
    pkg_blacklist = list(set(toolkit.aslist(config.get('ckan.spatialingestor.pkg_blacklist', []))))
    user_blacklist = list(
        set(map(lambda x: model.User.get(x).id, toolkit.aslist(config.get('ckan.spatialingestor.user_blacklist', [])))))

    if package['organization']['name'] in org_blacklist:
        log.error("{0} in organization blacklist".format(package['organization']['name']))
        return True
    elif package['name'] in pkg_blacklist:
        log.error("{0} in package blacklist".format(package['name']))
        return True
    else:
        activity_list = toolkit.get_action('package_activity_list')({'ignore_auth': True}, {
            'id': package['id'],
        })

        last_user = package['creator_user_id']
        if activity_list:
            last_user = activity_list[0]['user_id']

        if last_user in user_blacklist:
            log.error("{0} was last edited by blacklisted user".format(activity_list[0]['user_id']))
            return True

    return False


def get_spatial_input_format(resource):
    check_string = resource.get('__extras', {}).get('format', resource.get('format', resource.get('url', ''))).upper()

    if any([check_string.endswith(x) for x in ["SHP", "SHAPEFILE"]]):
        return 'SHP'
    elif check_string.endswith("KML"):
        return 'KML'
    elif check_string.endswith("KMZ"):
        return 'KMZ'
    elif check_string.endswith("GRID"):
        return 'GRID'
    else:
        return None


def is_spatially_ingestible_resource(resource):
    return get_spatial_input_format(resource) and not resource.get('spatial_child_of',
                                                                   '') and not is_resource_blacklisted(resource)


def spatialingestor_status_description(status):
    _ = toolkit._

    if status.get('status'):
        captions = {
            'complete': _('Complete'),
            'pending': _('Pending'),
            'submitting': _('Submitting'),
            'error': _('Error'),
        }

        return captions.get(status['status'], status['status'].capitalize())
    else:
        return _('Not Uploaded Yet')
