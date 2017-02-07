from ckan.logic import get_or_bust
from ckan.logic.auth import create as auth_create, delete as auth_delete, get as auth_get


def spatialingestor_job_submit(context, data):
    res_id, job_type = get_or_bust(data, ['resource_id', 'job_type'])

    if job_type == 'ingest':
        return auth_create.resource_create(context, {'id': res_id})
    elif job_type == 'purge':
        return auth_delete.resource_delete(context, {'id': res_id})
    else:
        return False


def spatialingestor_status(context, data):
    res_id = get_or_bust(data, 'resource_id')

    return auth_get.resource_show(context, {'id': res_id})
