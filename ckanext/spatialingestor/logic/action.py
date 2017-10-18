import datetime
import json
import urlparse

import ckan.lib.navl.dictization_functions
import requests
from ckan import logic
from ckan.lib import search
from ckan.plugins import toolkit
from dateutil.parser import parse as parse_date
from pylons import config

from ckanext.spatialingestor.helpers import get_microservice_metadata, is_spatially_ingestible_resource, log

_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate


def spatialingestor_job_submit(context, data_dict):
    res_id, job_type = _get_or_bust(data_dict, ['resource_id', 'job_type'])

    toolkit.check_access('spatialingestor_job_submit', context, data_dict)

    try:
        toolkit.get_action('resource_show')(context, {
            'id': res_id,
        })
    except logic.NotFound:
        return False

    spatialingestor_url = config.get('ckan.spatialingestor.url')

    if not spatialingestor_url:
        raise Exception(
            'Config option `{0}` must be set to use the SpatialIngestor.'.format('ckan.spatialingestor.url'))

    site_url = config['ckan.site_url']
    callback_url = site_url.rstrip('/') + '/api/3/action/spatialingestor_hook'

    user = toolkit.get_action('user_show')(context, {'id': context['user']})

    task = {
        'entity_id': res_id,
        'entity_type': 'resource',
        'task_type': job_type,
        'last_updated': str(datetime.datetime.utcnow()),
        'state': 'submitting',
        'key': 'spatialingestor',
        'value': '{}',
        'error': '{}',
    }

    try:
        task_id = toolkit.get_action('task_status_show')(context, {
            'entity_id': res_id,
            'task_type': job_type,
            'key': 'spatialingestor'
        })['id']
        task['id'] = task_id
    except logic.NotFound:
        pass

    context['ignore_auth'] = True
    toolkit.get_action('task_status_update')(context, task)

    try:
        metadata_package = get_microservice_metadata()
        metadata_package['resource_id'] = res_id
        metadata_package['ckan_url'] = site_url
        if job_type == 'spatial_purge':
            metadata_package['package_name'] = _get_or_bust(data_dict, 'package_name')

        r = requests.post(
            urlparse.urljoin(spatialingestor_url, 'job'),
            headers={
                'Content-Type': 'application/json'
            },
            data=json.dumps({
                'api_key': user['apikey'],
                'job_type': job_type,
                'result_url': callback_url,
                'metadata': metadata_package
            }))
        r.raise_for_status()
    except requests.exceptions.ConnectionError, e:
        error = {'message': 'Could not connect to Spatial Ingestor.',
                 'details': str(e)}
        task['error'] = json.dumps(error)
        task['state'] = 'error'
        task['last_updated'] = str(datetime.datetime.utcnow()),
        toolkit.get_action('task_status_update')(context, task)
        raise toolkit.ValidationError(error)

    except requests.exceptions.HTTPError, e:
        m = 'An Error occurred while sending the job: {0}'.format(e.message)
        try:
            body = e.response.json()
        except ValueError:
            body = e.response.text
        error = {'message': m,
                 'details': body,
                 'status_code': r.status_code}
        task['error'] = json.dumps(error)
        task['state'] = 'error'
        task['last_updated'] = str(datetime.datetime.utcnow()),
        toolkit.get_action('task_status_update')(context, task)
        raise toolkit.ValidationError(error)

    value = json.dumps({'job_id': r.json()['job_id'],
                        'job_key': r.json()['job_key']})

    task['value'] = value
    task['state'] = 'pending'
    task['last_updated'] = str(datetime.datetime.utcnow()),
    toolkit.get_action('task_status_update')(context, task)

    return True


def spatialingestor_hook(context, data_dict):
    ''' Update spatialingestor task. This action is typically called by the
    spatialingestor whenever the status of a job changes.

    :param status: status of the job from the spatialingestor service
    :type status: string
    :param resource: resource dict
    :type resource: dict
    :param task_info: message list of task steps
    :type tast_info: list[string]
    '''
    metadata, status = _get_or_bust(data_dict, ['metadata', 'status'])
    job_type = 'spatial_ingest'

    res_id = _get_or_bust(metadata, 'resource_id')

    # Pass metadata, not data_dict, as it contains the resource id needed
    # on the auth checks
    toolkit.check_access('spatialingestor_job_submit', context, {
        'resource_id': res_id,
        'job_type': job_type})

    task = toolkit.get_action('task_status_show')(context, {
        'entity_id': res_id,
        'task_type': job_type,
        'key': 'spatialingestor'
    })

    task['state'] = status
    task['last_updated'] = str(datetime.datetime.utcnow())

    resubmit = False

    if status == 'complete':
        # Create default views for resource if necessary (only the ones that
        # require data to be in the DataStore)
        resource_dict = toolkit.get_action('resource_show')(
            context, {'id': res_id})

        # Check if the uploaded file has been modified in the meantime
        if (resource_dict.get('last_modified') and
                metadata.get('task_created')):
            try:
                last_modified_datetime = parse_date(
                    resource_dict['last_modified'])
                task_created_datetime = parse_date(metadata['task_created'])
                if last_modified_datetime > task_created_datetime:
                    log.debug('Uploaded file more recent: {0} > {1}'.format(
                        last_modified_datetime, task_created_datetime))
                    resubmit = True
            except ValueError:
                pass
        # Check if the URL of the file has been modified in the meantime
        elif (resource_dict.get('url') and
                  metadata.get('original_url') and
                      resource_dict['url'] != metadata['original_url']):
            log.debug('URLs are different: {0} != {1}'.format(
                resource_dict['url'], metadata['original_url']))
            resubmit = True

    context['ignore_auth'] = True
    resp = toolkit.get_action('task_status_update')(context, task)

    if resubmit:
        log.debug('Resource {0} has been modified, '
                  'resubmitting to DataPusher'.format(res_id))
        toolkit.get_action('spatialingestor_job_submit')(
            context, res_id, job_type)


def spatialingestor_status(context, data_dict):
    res_id, job_type = _get_or_bust(data_dict, ['resource_id', 'job_type'])

    toolkit.check_access('spatialingestor_status', context, {'id': res_id})

    task = toolkit.get_action('task_status_show')(context, {
        'entity_id': res_id,
        'task_type': job_type,
        'key': 'spatialingestor'
    })

    spatialingestor_url = config.get('ckan.spatialingestor.url')
    if not spatialingestor_url:
        raise toolkit.ValidationError({'configuration': ['ckan.spatialingestor.url not in config file']})

    value = json.loads(task['value'])
    job_key = value.get('job_key')
    job_id = value.get('job_id')
    url = None
    job_detail = None

    if job_id:
        url = urlparse.urljoin(spatialingestor_url, 'job' + '/' + job_id)
        try:
            r = requests.get(url, headers={'Content-Type': 'application/json',
                                           'Authorization': job_key})
            r.raise_for_status()
            job_detail = r.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError):
            job_detail = {'error': 'cannot connect to spatialingestor'}

    return {
        'status': task['state'],
        'job_id': job_id,
        'job_url': url,
        'last_updated': task['last_updated'],
        'job_key': job_key,
        'task_info': job_detail,
        'error': json.loads(task['error'])
    }


def ingest_resource(context, resource_dict):
    # import pdb; pdb.set_trace()
    if toolkit.asbool(resource_dict.get('spatial_parent', 'False')):
        try:
            task = toolkit.get_action('task_status_show')(
                {
                    'ignore_auth': True
                }, {
                    'entity_id': resource_dict['id'],
                    'task_type': 'spatial_ingest',
                    'key': 'spatialingestor'
                })
            if task.get('state') in ['pending']:
                # There already is a pending Spatialingestor submission,
                # skip this one ...
                log.debug(
                    'Skipping Spatial Ingestor submission for resource {0}'.format(resource_dict['id']))
                return
        except toolkit.ObjectNotFound:
            pass

        try:
            log.debug('Submitting resource {0} to Spatial Ingestor'.format(resource_dict['id']))

            toolkit.get_action('spatialingestor_job_submit')(context, {
                'resource_id': resource_dict['id'],
                'job_type': 'spatial_ingest'
            })
        except toolkit.ValidationError, e:
            log.error(e)
    elif is_spatially_ingestible_resource(resource_dict):
        try:
            dataset = toolkit.get_action('package_show')(context, {
                'id': resource_dict['package_id'],
            })
        except Exception, e:
            log.error(
                "Failed to retrieve package ID: {0} with error {1}".format(resource_dict['package_id'], str(e)))
            return

        log.info("Loaded dataset {0}.".format(dataset['name']))

        # We auto_process spatial file by updating the resource, which will re-trigger this method
        resource_dict['spatial_parent'] = 'True'
        try:
            toolkit.get_action('resource_update')(context, resource_dict)
        except toolkit.ValidationError, e:
            log.error(e)


def purge_resource_datastores(context, resource_dict):
    # Have to be careful about how to delete child resources here in a sense that
    # the Spatialingestor microservice will not be able to query CKAN to find
    # child spatial resources. So, this must be done in this thread with the IDs
    # passed back to the micro-service
    # import pdb; pdb.set_trace()
    if toolkit.asbool(resource_dict.get('spatial_parent', 'False')):
        # We have a spatial parent, so we have to get all the child resources

        # FIXME: Do we really need this? it looks, like we are getting 'pending'
        # job every time, so no purge happens at all
        # try:
        #     # Make sure there is no other purginging process is running
        #     task = toolkit.get_action('task_status_show')(
        #         {
        #             'ignore_auth': True
        #         }, {
        #             'entity_id': resource_dict['id'],
        #             'task_type': 'spatial_purge',
        #             'key': 'spatialingestor'
        #         })

        #     if task.get('state') in ['init', 'pending']:
        #         log.debug(
        #             'Skipping spatial ingestor purge for resource {0}'.format(resource_dict['id']))
        #         return
        # except toolkit.ObjectNotFound:
        #     pass

        log.debug(
            'Submitting job to purge PostGIS and Geoserver assets linked to resource {0}'.format(resource_dict['id']))

        # Submit request to microservice to clean out Geoserver and PostGIS assets
        # Then remove all children of current resource
        pkg_dict = toolkit.get_action('package_show')(
            {'ignore_auth': True},
            {'id': resource_dict['package_id']})
        toolkit.get_action('spatialingestor_job_submit')(context, {
            'resource_id': resource_dict['id'],
            'package_name': pkg_dict['name'],
            'job_type': 'spatial_purge'
        })
        children = [
            r['id'] for r in filter(
                lambda res: res.get('spatial_child_of') == resource_dict['id'], 
                pkg_dict['resources'])]
        children = [
            res['id'] for res in 
            pkg_dict['resources'] 
            if res.get('spatial_child_of') == resource_dict['id']
        ]
        for child in children:
            toolkit.get_action('resource_delete')({
                'user': context['user'], 'model': context['model']}, {
                    'id': child})

