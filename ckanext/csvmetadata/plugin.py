# encoding: utf-8

import os
import csv
import json
import logging
import requests
from StringIO import StringIO
from collections import OrderedDict
from ast import literal_eval as l_eval


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

#A global that stores CKAN site url
ckan_site_url = None

#A limit of CSV file size to be processed in order to get headers
csv_header_byte_limit = 4096

#class DatastoreException(Exception):
#    pass

def check_json_file(path, file_desc="CSVMetadata table config file"):
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
            if element["preset"] == "select":
                for choice in element["choices"]:
                    choice["name"] = choice.pop("label")
                #Currently overriden in the template
                element["selected"] = element["choices"][0]["value"] if element["choices"] else ""
            element["required"] = element.pop("required") if "required" in element else False
        return schema

    def get_csv_sample(self, csv_url):
        status = "ok"
        csv_headers = []
        csv_info = {"delimiter":"", "encoding":"", "quotechar":""}
        
        try:
            req = requests.get(csv_url, timeout=10, stream=True)
        except:
            status = "url_fail"
        else:
            req.raw.decode_content = True
            if req.status_code == 200:
                content = req.raw.read(csv_header_byte_limit+1)
                if len(content) > csv_header_byte_limit:
                    pass #Noting that the response is too large
                #Limiting the amount of CSV to be processed
                sample = content[:csv_header_byte_limit]

                #CSV encoding can be taken from Response object we get from requests.get
                #which determines encoding from HTTP response headers
                encoding = req.encoding

                #Now trying to deduce, what kind of CSV is this and if it's CSV at all
                sniffer = csv.Sniffer()
                try:
                    dialect = sniffer.sniff(sample)
                except csv.Error:
                    #Whatever we got, Python CSV module heuristics don't recognize it as CSV
                    status = "not_csv"
                else:
                    #Reading one line from sample
                    csv_headers_str = sample.splitlines()[0].strip()
                    #Weird bug in Python CSV module - needs conversion like this
                    delimiter = str(dialect.delimiter)
                    quotechar = str(dialect.quotechar)
                    csv_headers = csv.reader([csv_headers_str], delimiter=delimiter, quotechar=quotechar).next()
                    csv_info["delimiter"] = delimiter
                    csv_info["quotechar"] = quotechar
                    csv_info["encoding"] = encoding
            else:
                status = "http_error_{}".format(req.status_code)
        return status, csv_headers, csv_info

    def fetch_json_return_values(self, json_url):
        req = requests.get(json_url)
        json_dict = json.loads(req.text)
        return json_dict

    def eval_remove_from_form(self, form_data, data_name):
        data_str = form_data.pop(data_name)
        return l_eval(data_str)

    def csvw_to_form(self, csvw_dict):
        """
           Converts data from CSVW dictionary to CSV metadata form values.
           The resulting data is used to pre-fill the CSV metadata form 
           if JSON file with CSVW description already exists for the resource selected.
        """
        form_values = {}

        column_info = csvw_dict["tableSchema"]["columns"]
        for i, column in enumerate(column_info):
            #Datatype is a separate dict inside, processing it
            datatype = column["datatype"]["base"]
            length = column["datatype"]["length"]
            column.pop("datatype")
            form_values["{}-datatype".format(i)] = datatype
            form_values["{}-length".format(i)] = length
            #foreignKeys is also a separate dict
            if "foreignKeys" in column and column["foreignKeys"]:
                form_values["{}-foreignKeys".format(i)] = True
                foreignKeys = column.pop("foreignKeys")
                resource = foreignKeys[0]["reference"]["resource"]
                columnReference = foreignKeys[0]["reference"]["columnReference"]
                form_values["{}-resource".format(i)] = resource
                form_values["{}-columnReference".format(i)] = columnReference
            #All the other keys are mapped directly to the dictionary
            for key in column.keys():
                if column[key] is False:
                    #This is a checkbox value
                    #It's better left unset to conform to HTML form POST format
                    column.pop(key)
                else:
                    #Any other values, can be processed in template
                    form_values["{}-{}".format(i, key)] = column[key]
        return form_values            

    def form_to_csvw(self, form_data):
        """
           Converts data from CSV metadata form values to CSVW dictionary.
           The form data comes as HTML form data through a POST request
           so there is POST-specific processing, too.
        """
        #Assumes info about current resource is in toolkit.c.pkg_dict
        #and info about current package is in toolkit.c.resource
        #The info is placed there by resource_csv controller (in this same class)

        #csv headers and csv info dict are passed along with form data, removing them from the form data
        csv_info = self.eval_remove_from_form(form_data, "csv_info")
        csv_headers = self.eval_remove_from_form(form_data, "csv_headers")

        #getting data about organization
        org_data = toolkit.c.pkg_dict["organization"]
        org_url = "TOFIX"

        #Dictionary to store CSVW data
        csvw_json_data = OrderedDict()
        csvw_json_data["@context"] = ["http://www.w3.org/ns/csvw", {"@language":"lv"}]
        #csvw_json_data["@type"] = "Table"
        csvw_json_data["url"] = toolkit.c.resource["url"]
        csvw_json_data["dialect"] = csv_info
        csvw_json_data["dc:title"] = toolkit.c.pkg_dict["title"]
        csvw_json_data["dcat:keyword"] = [tag["name"] for tag in toolkit.c.pkg_dict["tags"]] if "tags" in toolkit.c.pkg_dict else []
        csvw_json_data["dc:publisher"] = {"schema:name":org_data["title"],
                                          "schema:url":org_url}
        csvw_json_data["dc:license"] = {"@id":toolkit.c.pkg_dict["license_url"]}
        csvw_json_data["dc:issued"] = {"@value":toolkit.c.resource["created"].split("T")[0], "@type":"xsd:date"}
        if toolkit.c.resource["last_modified"]:
            modified_date = toolkit.c.resource["last_modified"].split("T")[0]
        else:
            modified_date = csvw_json_data["dc:issued"]["@value"]
        csvw_json_data["dc:modified"] = {"@value":modified_date, "@type":"xsd:date"}
        
        #Creating a dictionary for each CSV header so that we can stuff data from form to those dictionaries
        schema = {"columns":[dict() for i in range(len(csv_headers))]}

        #form field names come in "{header_num}-{form_field_name}"
        form_elements = form_data.keys()
        form_elements.sort()
        for key in form_elements:
            try:
                header_num_str, form_field_name = key.split("-", 1)
                header_num = int(header_num_str)
            except:
                print("Error while parsing key {}".format(key))
            else:
                schema["columns"][header_num][form_field_name] = form_data[key]
        
        #Working around checkboxes - they'll be in the form data if set, but as '"checkbox_name":""' instead of '"checkbox_name"=True'
        form_schema = self.get_form_schema()
        checkbox_ids = [element["name"] for element in form_schema["form_fields"] if element["preset"] == "checkbox"]
        for column in schema["columns"]:
            for checkbox_id in checkbox_ids:
                column[checkbox_id] = checkbox_id in column.keys()

        #Now, re-formatting the resulting schema dictionary to conform with the specification
        for column in schema["columns"]:
            #dict is passed by reference, so we don't have to replace it after changing its contents
            base = column.pop("datatype")
            length = column.pop("length")
            column["datatype"] = {"base":base, "length":length}
            #Now need to process primary and secondary keys
            resource = column.pop("resource")
            columnReference = column.pop("columnReference")
            foreign_keys = column.pop("foreignKeys")
            if foreign_keys:
                column["foreignKeys"] = [{ "reference" : {"resource":resource, "columnReference":columnReference}}]
            else:
                column["foreignKeys"] = []

        #Adding created schema to CSVW dictionary
        csvw_json_data["tableSchema"] = schema
        #Turn OrderedDict into JSON string
        csvw_json_string_data = json.dumps(csvw_json_data, indent=2)
        return csvw_json_string_data

    def find_existing_json_for_resource(self, resources, json_filename):
        """ 
            Function determines if CSVW file with a given filename already exists among resources.
            If it does, returns URL to download that file and its resource ID. Otherwise, returns None and None.
        """
        json_url = None
        json_res_id = None
        #Going through resources to see if metadata JSON already exists
        for resource in resources:
            #If there are multiple versions with same filenames, selecting the last one in the list of resources
            if self.filename_for_url(resource["url"]) == json_filename:
                json_url = resource["url"]
                json_res_id = resource["id"]
        return json_url, json_res_id

    def filename_for_url(self, url):
        """
           Given an URL, returns its last part which, most likely, contains the resource's filename.
        """
        #(Ab)uses os.path.split, though url.rsplit() could also work.
        return os.path.split(url)[1]

    def make_json_filename(self, resource_filename):
        """
            Defines how the JSON file filename is constructed. 
            Changing this format will change both creation of new files 
            and matching to old files, Accordingly, it will 
            break backwards compatibility unless code that 
            relies on this function is modified accordingly.
        """
        return "{}_metadata.json".format(resource_filename)

    def resource_csv(self, id, resource_id):
        """
            Controller for "CSV metadata" page 
        """
        #Getting information about the package and resource
        #It's necessary for the base template to render, and we can also use the data ourselves
        try:
            toolkit.c.pkg_dict = p.toolkit.get_action('package_show')(None, {'id': id})
            toolkit.c.resource = p.toolkit.get_action('resource_show')(None, {'id': resource_id})
        except (logic.NotFound, logic.NotAuthorized):
            base.abort(404, _('Resource not found'))

        #getting form schema to pass to template
        form_schema = self.get_form_schema()
        
        #getting resource url, checking if it's good
        resource_url = toolkit.c.resource["url"] if "url" in toolkit.c.resource else ""
        if not resource_url:
            #No URL shown or empty URL, failing early
            return base.render('csvmetadata/resource_csv.html',
                           extra_vars={'status':'url_fail'})

        resource_filename = self.filename_for_url(resource_url)
        #Info about other resources - for JSON file lookup
        other_resources = [resource for resource in toolkit.c.pkg_dict["resources"] if resource["id"] != resource_id]
        
        #Checking if we're in a POST request - 
        #then we need to create JSON from received data and upload it
        if toolkit.request.method == 'POST':
            #Loading data from form
            form_data = p.toolkit.request.POST
            csvw_string = self.form_to_csvw(dict(form_data))
            io_object = StringIO(csvw_string)
            
            filename = self.make_json_filename(resource_filename)
            #monkeypatching because ckanapi gets filename from descriptor
            io_object.name = filename

            resource_name = filename
            
            _, json_resource_id = self.find_existing_json_for_resource(other_resources, self.make_json_filename(resource_filename))
            if json_resource_id:
                print("Updating resource")
                ckan_api.action.resource_update(id=json_resource_id, url="", upload=io_object)
            else:
                print("Creating resource")
                ckan_api.action.resource_create(package_id=id, name=resource_name, url="", upload=io_object)
            #Successfully uploaded, now redirecting to the package contents page to show that JSON file was created successfully
            core_helpers.redirect_to(
                controller='package',
                action='read',
                id=id
            )

        #POST request processing code didn't continue, assuming GET method
        json_url, _ = self.find_existing_json_for_resource(other_resources, self.make_json_filename(resource_filename))
        values = {}
        if json_url:
            #Some kind of JSON URL is found, let's fetch it and get CSV header descriptions
            try:
                json_dict = self.fetch_json_return_values(json_url)
                values = self.csvw_to_form(json_dict)
            except KeyboardInterrupt as e:
                pass #JSON is either unfetchable or badly constructed, so we won't use it
        
        #Getting CSV from the resource url
        status, csv_headers, csv_info = self.get_csv_sample(resource_url)

        return base.render('csvmetadata/resource_csv.html',
                           extra_vars={'status':status, 
                                       'csv_headers':csv_headers, 
                                       'schema':form_schema, 
                                       'values':values,
                                       'csv_info':repr(csv_info)})


class CSVMetadataPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IRoutes, inherit=True)

    #IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_resource('fanstatic', 'csvmetadata')

    #IConfigurable
    def configure(self, config):
        global ckan_site_url

        self.config = config

        for config_option in ('ckan.site_url', ):
            if not config.get(config_option):
                raise Exception(
                    'Config option `{0}` must be set to use CSVMetadata.'
                    .format(config_option))
        ckan_site_url = config.get('ckan.site_url')
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
