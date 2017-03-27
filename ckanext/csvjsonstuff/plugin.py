# encoding: utf-8

import logging
import json
import os

import ckan.plugins as p
import ckan.lib.base as base
import ckan.lib.helpers as core_helpers
# import ckanext.datapusher.logic.action as action
# import ckanext.datapusher.helpers as helpers
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as toolkit

from ckan.common import _

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust



#A global that stores form schema path
form_schema_path = None


#class DatastoreException(Exception):
#    pass

def check_json_file(path, file_desc="CSVJSONStuff config file"):
    if not os.path.isfile(path):
        raise Exception(
             '{} not found at {}'
             .format(file_desc, path))
    try:
        with open(path, "r") as f:
            json.load(f)
    except (OSError, ValueError) as e:
         raise Exception(
            "Error while checking {} at {}: \n Error details: {}"
            .format(file_desc, path, repr(e)))


class ResourceCSVController(base.BaseController):

    def get_table_schema(self):
        check_json_file(form_schema_path) #Will try to open and validate file at the path
        with open(form_schema_path, 'r') as f:
            schema = json.load(f)
        return self.adjust_schema(schema)

    def adjust_schema(self, schema):
        #ensures correct schema-to-ckan_form_element_name mapping
        preset_map = {"textbox":"input"}
        for element in schema["form_fields"]:
            #Adjust label?
            if element["preset"] in preset_map:
                element["preset"] = preset_map[element["preset"]]
            element["name"] = element.pop("field_name")
            element["is_required"] = element.pop("required")
        return schema

    def resource_csv(self, id, resource_id):
        
        table_schema = self.get_table_schema()
        contents = p.toolkit.get_action('resource_show')(None, {'id': resource_id})
        import pdb;pdb.set_trace()

        if toolkit.request.method == 'POST':
            #Loading data from form
            status = "uploaded"
            try:
                #toolkit.c.pkg_dict = p.toolkit.get_action('csvjsonstuff_submit')(None, {'resource_id': resource_id})
                #import pdb; pdb.set_trace()
                pass
            except logic.ValidationError:
                pass

        
        #Needed so that resource data is properly included in the template?
        try:
            toolkit.c.pkg_dict = p.toolkit.get_action('package_show')(
                None, {'id': id}
            )
            toolkit.c.resource = p.toolkit.get_action('resource_show')(
                None, {'id': resource_id}
            )
        except (logic.NotFound, logic.NotAuthorized):
            base.abort(404, _('Resource not found'))

        """try:
            datapusher_status = p.toolkit.get_action('datapusher_status')(
                None, {'resource_id': resource_id}
            )
        except logic.NotFound:
            datapusher_status = {}
        except logic.NotAuthorized:
            base.abort(403, _('Not authorized to see this page'))
        """

        return base.render('csvjsonstuff/resource_csv.html',
                           extra_vars={'status': status})


def submit(*args, **kwargs):
    print("submit")
    import pdb; pdb.set_trace()

def submit_auth(*args, **kwargs):
    print("submit_auth")
    import pdb; pdb.set_trace()

class CSVJSONStuffPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IActions)
    #p.implements(p.IAuthFunctions)
    #p.implements(p.IResourceUrlChange)
    #p.implements(p.IDomainObjectModification, inherit=True)
    #p.implements(p.ITemplateHelpers)
    p.implements(p.IRoutes, inherit=True)

    #Datapusher-specific vars?
    #legacy_mode = False
    #resource_show_action = None
    #end Datapusher-specific vars?

    #IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')

    #IConfigurable
    def configure(self, config):
        self.config = config

        for config_option in ('ckan.site_url', 'ckan.csvjsonstuff.form_schema_path'):
            if not config.get(config_option):
                raise Exception(
                    'Config option `{0}` must be set to use CSVJSONStuff.'
                    .format(config_option))

        self.form_schema_path = os.path.abspath(config.get('ckan.csvjsonstuff.form_schema_path'))
        check_json_file(self.form_schema_path) #Will try to open and validate file at the path
        global form_schema_path
        form_schema_path = self.form_schema_path

    #IRoutes
    def before_map(self, m):
        m.connect(
            'resource_csv', '/dataset/{id}/resource_csv/{resource_id}',
            controller='ckanext.csvjsonstuff.plugin:ResourceCSVController',
            action='resource_csv', ckan_icon='cloud-upload')
        return m

    #IActions
    def get_actions(self):
        return {'csvjsonstuff_submit': submit}
        #        'csvjsonstuff_hook': action.datapusher_hook,
        #        'csvjsonstuff_status': action.datapusher_status}


    #IAuthFunctions
    def get_auth_functions(self):
        return {'csvjsonstuff_submit': self.submit_auth}
        #        {'datapusher_status': auth.datapusher_status}

    #ITemplateHelpers
    def get_helpers(self):
        return {
            'datapusher_status': helpers.datapusher_status,
            'datapusher_status_description':
            helpers.datapusher_status_description,
        }

#Could be useful later
"""

    #IDomainObjectModification
    def notify(self, entity, operation=None):
        if isinstance(entity, model.Resource):
            if (operation == model.domain_object.DomainObjectOperation.new or
                    not operation):
                # if operation is None, resource URL has been changed, as
                # the notify function in IResourceUrlChange only takes
                # 1 parameter
                context = {'model': model, 'ignore_auth': True,
                           'defer_commit': True}
                if (entity.format and
                        entity.format.lower() in self.datapusher_formats and
                        entity.url_type != 'datapusher'):

                    try:
                        task = p.toolkit.get_action('task_status_show')(
                            context, {
                                'entity_id': entity.id,
                                'task_type': 'datapusher',
                                'key': 'datapusher'}
                        )
                        if task.get('state') == 'pending':
                            # There already is a pending DataPusher submission,
                            # skip this one ...
                            log.debug(
                                'Skipping DataPusher submission for '
                                'resource {0}'.format(entity.id))
                            return
                    except p.toolkit.ObjectNotFound:
                        pass

                    try:
                        log.debug('Submitting resource {0}'.format(entity.id) +
                                  ' to DataPusher')
                        p.toolkit.get_action('datapusher_submit')(context, {
                            'resource_id': entity.id
                        })
                    except p.toolkit.ValidationError, e:
                        # If datapusher is offline want to catch error instead
                        # of raising otherwise resource save will fail with 500
                        log.critical(e)
                        pass"""


"""            #core_helpers.redirect_to(
            #    controller='ckanext.csvjsonstuff.plugin:ResourceCSVController',
            #    action='resource_csv',
            #    id=id,
            #    resource_id=resource_id
            #)"""
