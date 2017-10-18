from ckan import logic, model, plugins
from ckan.common import _
from ckan.lib import base
from ckan.lib import helpers as core_helpers
from ckan.plugins import toolkit
from pylons import config

from ckanext.spatialingestor import helpers
from ckanext.spatialingestor.logic import auth, action


class SpatialIngestorPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IRoutes, inherit=True)

    legacy_mode = False
    resource_show_action = None
    d_type = model.domain_object.DomainObjectOperation

    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')

    def notify(self, entity, operation=None):
        if isinstance(entity, model.Resource):
            resource_dict = model.Resource.get(entity.id).as_dict()
            if not helpers.is_spatially_ingestible_resource(resource_dict):
                return
            d_type = model.domain_object.DomainObjectOperation
            auto_ingest = toolkit.asbool(config.get('ckan.spatialingestor.auto_ingest', 'False'))
            is_spatial_parent = toolkit.asbool(resource_dict.get('spatial_parent', 'False'))

            if (is_spatial_parent and (operation == d_type.changed or not operation)) or (
                    operation == d_type.new and auto_ingest):
                helpers.log.error(">>>>>>> Registered Ingest Trigger")
                toolkit.get_action('spatialingestor_ingest_resource')({}, resource_dict)

    def before_map(self, m):
        m.connect(
            'resource_spatialingest', '/resource_spatialingest/{resource_id}',
            controller='ckanext.spatialingestor.plugin:ResourceSpatialController',
            action='resource_spatialingest', ckan_icon='cloud-upload')
        return m

    def get_actions(self):
        return {'spatialingestor_job_submit': action.spatialingestor_job_submit,
                'spatialingestor_hook': action.spatialingestor_hook,
                'spatialingestor_status': action.spatialingestor_status,
                'spatialingestor_ingest_resource': action.ingest_resource,
                'spatialingestor_purge_resource_datastores': action.purge_resource_datastores,
            }

    def get_auth_functions(self):
        return {'spatialingestor_job_submit': auth.spatialingestor_job_submit,
                'spatialingestor_status': auth.spatialingestor_status}

    def get_helpers(self):
        return {'spatialingestor_status_description': helpers.spatialingestor_status_description,
                'spatialingestor_is_spatially_ingestible_resource': helpers.is_spatially_ingestible_resource}


class ResourceSpatialController(base.BaseController):
    def resource_spatialingest(self, resource_id):
        if toolkit.request.method == 'POST':
            try:
                resource_dict = toolkit.get_action('resource_show')({}, {'id': resource_id})
                toolkit.get_action('spatialingestor_ingest_resource')({}, resource_dict)
            except logic.ValidationError:
                pass

            base.redirect(core_helpers.url_for(
                controller='ckanext.spatialingestor.plugin:ResourceSpatialController',
                action='resource_spatialingest',
                resource_id=resource_id)
            )
        try:
            toolkit.c.resource = toolkit.get_action('resource_show')(
                None, {'id': resource_id}
            )
            toolkit.c.pkg_dict = toolkit.get_action('package_show')(
                None, {'id': toolkit.c.resource['package_id']}
            )
        except logic.NotFound:
            base.abort(404, _('Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _('Unauthorized to edit this resource'))

        try:
            spatialingestor_status = toolkit.get_action('spatialingestor_status')(None, {
                'resource_id': resource_id,
                'job_type': 'spatial_ingest'
            })
        except logic.NotFound:
            spatialingestor_status = {}
        except logic.NotAuthorized:
            base.abort(401, _('Not authorized to see this page'))

        return base.render('package/resource_spatialingest.html',
                           extra_vars={'status': spatialingestor_status})
