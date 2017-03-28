# encoding: utf-8

import os
import csv
import json
import logging
import requests
from StringIO import StringIO

from ckanapi import LocalCKAN
ckan_api = LocalCKAN()

import ckan.plugins as p
import ckan.lib.base as base
import ckan.lib.helpers as core_helpers
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

def check_json_file(path, file_desc="CSVMetadata config file"):
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

    def get_form_schema(self):
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
            element["is_required"] = element.pop("required") if "required" in element else False
        return schema

    def resource_csv(self, id, resource_id):
        contents = p.toolkit.get_action('resource_show')(None, {'id': resource_id})

        form_schema = self.get_form_schema()
        resource_url = contents["url"]
        
        #Checking if we received a POST request - 
        #then we need to create JSON from received data and upload it
        if toolkit.request.method == 'POST':
            #Loading data from form
            try:
                #toolkit.c.pkg_dict = p.toolkit.get_action('csvmetadata_submit')(None, {'resource_id': resource_id})
                data = p.toolkit.request.POST
                print(data)
                #TODO: process received data
                io_object = StringIO(repr(data))
                #monkeypatching because ckanapi gets filename from descriptor
                io_object.name = "bom.csv.metadata.json"
                api = LocalCKAN()
                api.action.resource_create(package_id="bom", name="bom.csv.metadata.json", url="some url", upload=io_object)
                #Successfully uploaded, now redirecting to the package contents page to show that JSON file was created successfully
                core_helpers.redirect_to(
                    controller='package',
                    action='read',
                    id=id
                )
            except logic.ValidationError:
                pass

        #Assuming GET method

        #This data is, apparently, needed so that resource data is properly included in the template?
        try:
            toolkit.c.pkg_dict = p.toolkit.get_action('package_show')(
                None, {'id': id}
            )
            toolkit.c.resource = p.toolkit.get_action('resource_show')(
                None, {'id': resource_id}
            )
        except (logic.NotFound, logic.NotAuthorized):
            base.abort(404, _('Resource not found'))

        #Getting CSV from the link

        status = "ok"

        #TODO: LIMIT RESPONSE SIZE!
        """import requests
        r = requests.get('https://github.com/timeline.json', timeout=5, stream=True)
        r.raw.decode_content = True
        content = r.raw.read(1024+1)
        if len(content) > 1024:
            raise ValueError('Too large a response')
        print content"""


        req = requests.get(resource_url)
        if req.status_code == 200:
            csv_document = req.text
            sample = csv_document[:1024]
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
            except csv.Error:
                status = "not_csv"
            else:
                csv_headers_str = sample.splitlines()[0].strip()
                delimiter = str(dialect.delimiter)
                quotechar = str(dialect.quotechar)
                csv_headers = csv.reader([csv_headers_str], delimiter=delimiter, quotechar=quotechar).next()
        else:
            status = "http_error_{}".format(req.status_code)
        
        print(status)
        print(csv_headers)
        return base.render('csvmetadata/resource_csv.html',
                           extra_vars={'status':status, 'csv_headers':csv_headers, 'schema':form_schema})


class CSVMetadataPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IRoutes, inherit=True)

    #IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')

    #IConfigurable
    def configure(self, config):
        self.config = config

        for config_option in ('ckan.site_url', ):
            if not config.get(config_option):
                raise Exception(
                    'Config option `{0}` must be set to use CSVMetadata.'
                    .format(config_option))

        plugin_path = os.path.dirname(__file__)
        self.form_schema_path = os.path.join(plugin_path, "form_schema.json")
        check_json_file(self.form_schema_path) #Will try to open and validate file at the path
        global form_schema_path
        form_schema_path = self.form_schema_path

    #IRoutes
    def before_map(self, m):
        m.connect(
            'resource_csv', '/dataset/{id}/resource_csv/{resource_id}',
            controller='ckanext.csvmetadata.plugin:ResourceCSVController',
            action='resource_csv', ckan_icon='cloud-upload')
        return m
