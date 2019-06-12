# encoding: utf-8

import os
import json
import logging
import requests
import unicodecsv as csv
from StringIO import StringIO
from collections import OrderedDict
from ast import literal_eval as lit_eval
from helpers import csvmetadata_language_text

from ckanapi import LocalCKAN
ckan_api = LocalCKAN()

import ckan.plugins as p
import ckan.lib.base as base
import ckan.lib.helpers as core_helpers
import ckan.logic as logic
import ckan.model as model
import ckan.plugins.toolkit as tk
from ckan.lib.plugins import DefaultTranslation

from ckan.common import _

log = logging.getLogger(__name__)

#A global that stores form schema path
form_schema_path = None

#A global that stores CKAN site url
ckan_site_url = None

#A global that stores CKAN root path
ckan_root_path = None

#A global that stores CKAN API key
ckan_api_key = None

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

    def get_csv_sample(self, csv_url, url_type=None):
        status = "ok"
        csv_headers = []
        csv_info = {"delimiter":"", "encoding":"", "quoteChar":""}

        headers = {}
        if url_type == "upload":
            headers["Authorization"] = ckan_api_key
            logging.info("CSV is stored in CKAN storage!")
        elif url_type is not None:
            log.warning("Unknown resource URL type: {}".format(csv_url))

        try:
            req = requests.get(csv_url, headers=headers, timeout=10, stream=True)
        except:
            status = "url_fail"
        else:
            req.raw.decode_content = True
            if req.status_code == 200:
                content = req.raw.read(csv_header_byte_limit+1)
                if len(content) > csv_header_byte_limit:
                    pass #Noting that the response is too large

                #CSV encoding can be taken from Response object we get from requests.get
                #which determines encoding from HTTP response headers
                encoding = req.encoding

                #Limiting the amount of CSV to be processed and storing it in unicode object
                try:
                    if content[:3] == '\xef\xbb\xbf':
                        content = content[3:]
                        raise LookupError() #fall-through
                    elif content[:2] == '\xfe\xff': 
                        content = content[2:]
                        raise LookupError() #fall-through
                    sample = content[:csv_header_byte_limit]
                except LookupError:
                    #encoding not found among Python decoders, fallback option is UTF8
                    #or we fell through due to UTF-16/UTF-8 marker found
                    sample = content[:csv_header_byte_limit]
                    encoding = "utf-8"

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
                    delimiter = str(dialect.delimiter)
                    quotechar = str(dialect.quotechar)
                    csv_headers = csv.reader(StringIO(csv_headers_str), delimiter=delimiter, quotechar=quotechar, encoding=encoding).next()
                    csv_info["delimiter"] = delimiter
                    csv_info["quoteChar"] = quotechar
                    csv_info["encoding"] = encoding
            else:
                status = "http_error_{}".format(req.status_code)

        #Autogenerating CSV header fields in case  some of them are empty
        for i, header in enumerate(csv_headers):
            if not header:
                csv_headers[i] = "col{}".format(i)

        return status, csv_headers, csv_info

    def fetch_json_return_values(self, json_url, url_type):
        """
            Downloads saved JSON with metadata, parses it as JSON and
            returns an object that contains the parsed JSON - in our
            case, it's 99.999% likely to be a dictionary
        """
        headers = {}
        if url_type == "upload":
            headers["Authorization"] = ckan_api_key
            logging.info("JSON is stored in CKAN storage!")
        elif url_type is not None:
            log.warning("Unknown resource URL type: {}".format(csv_url))

        req = requests.get(json_url, headers=headers, timeout=3)
        json_dict = json.loads(req.text)
        return json_dict

    def eval_remove_from_form(self, form_data, data_name):
        """
            A small helper function that takes a dictionary, pops a 
            value out of it by key name, evaluates the value (using 
            ast.literal_eval function which only evaluates basic data 
            structures) and returns it. Great for parsing data that
            was created by repr() in the template and passed back
            to the controller.
        """
        data_str = form_data.pop(data_name)
        return lit_eval(data_str)

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
            datatype = column["datatype"]["dc:title"] or column["datatype"]["base"]
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
        #Assumes info about current resource is in tk.c.pkg_dict
        #and info about current package is in tk.c.resource
        #The info is placed there by resource_csv controller (in this same class)

        #csv headers and csv info dict are passed along with form data, removing them from the form data
        csv_info = self.eval_remove_from_form(form_data, "csv_info")
        csv_headers = self.eval_remove_from_form(form_data, "csv_headers")
        
        form_data.pop("csv_has_headers")

        #getting data about organization
        org_data = tk.c.pkg_dict["organization"]
        if ckan_root_path is not None:
            org_url = "{}/{}/organization/{}".format(ckan_site_url, ckan_root_path, org_data["name"])
        else:
            org_url = "{}/organization/{}".format(ckan_site_url, org_data["name"])

        #Dictionary to store CSVW data
        csvw_json_data = OrderedDict()
        csvw_json_data["@context"] = ["http://www.w3.org/ns/csvw", {"@language":"lv"}]
        #csvw_json_data["@type"] = "Table"
        csvw_json_data["url"] = tk.c.resource["url"]
        csvw_json_data["dialect"] = csv_info
        csvw_json_data["dc:title"] = tk.c.pkg_dict["title"]
        csvw_json_data["dcat:keyword"] = [tag["name"] for tag in tk.c.pkg_dict["tags"]] if "tags" in tk.c.pkg_dict else []
        csvw_json_data["dc:publisher"] = OrderedDict( (("schema:name", org_data["title"]),
                                          ( "schema:url", org_url)) )
        csvw_json_data["dc:license"] = {"@id":tk.c.pkg_dict["license_url"]}
        csvw_json_data["dc:issued"] = OrderedDict( (("@value", tk.c.resource["created"].split("T")[0]), ("@type", "xsd:date")) )
        if tk.c.resource["last_modified"]:
            modified_date = tk.c.resource["last_modified"].split("T")[0]
        else:
            modified_date = csvw_json_data["dc:issued"]["@value"]
        csvw_json_data["dc:modified"] = OrderedDict( (("@value", modified_date), ("@type", "xsd:date")) )
        
        #Creating a dictionary for each CSV header so that we can stuff data from form to those dictionaries
        schema = {"columns":[OrderedDict() for i in range(len(csv_headers))]}

        #form field names come in "{header_num}-{form_field_name}"
        form_elements = form_data.keys()
        form_elements.sort()
        for key in form_elements:
            try:
                header_num_str, form_field_name = key.split("-", 1)
                header_num = int(header_num_str)
            except:
                log.info("Error while parsing key {}".format(key))
            else:
                schema["columns"][header_num][form_field_name] = form_data[key]
        
        #Working around checkboxes - they'll be in the form data if set, but as '"checkbox_name":""' instead of '"checkbox_name"=True'
        form_schema = self.get_form_schema()
        checkbox_ids = [element["name"] for element in form_schema["form_fields"] if element["preset"] == "checkbox"]
        for column in schema["columns"]:
            for checkbox_id in checkbox_ids:
                if checkbox_id in column.keys():
                    column[checkbox_id] = True 

        #Now, re-formatting the resulting schema dictionary to conform with the specification
        for column in schema["columns"]:
            #dict is passed by reference, so we don't have to replace it after changing its contents
            base = column.pop("datatype")
            length = column.pop("length")
            #column["datatype"] = {"base":base, "length":length, "dc:title":base}
            column["datatype"] = OrderedDict( (("dc:title", base), ("base", base), ("length", length)) )

            # lat/long processing
            if(base == "latitude"):
                column["datatype"]["base"] = "decimal"
                column["datatype"]["minimum"] = "-90"
                column["datatype"]["maximum"] = "90"
            elif (base == "longitude"):
                column["datatype"]["base"] = "decimal"
                column["datatype"]["minimum"] = "-180"
                column["datatype"]["maximum"] = "180"
            elif (base == "X"):
                column["datatype"]["base"] = "decimal"
                column["datatype"]["minimum"] = "160000"
                column["datatype"]["maximum"] = "450000"
            elif (base == "Y"):
                column["datatype"]["base"] = "decimal"
                column["datatype"]["minimum"] = "300000"
                column["datatype"]["maximum"] = "780000"

            #Now need to process primary and secondary keys
            resource = column.pop("resource")
            columnReference = column.pop("columnReference")
            if "foreignKeys" in column:
                column.pop("foreignKeys")
                column["foreignKeys"] = [{ "reference" : OrderedDict((("resource", resource), ("columnReference", columnReference)))}]
            else:
                pass 

        #Sorting schema fields to make the column secription human-readable
        order = [u'name', u'titles', u'dc:description', u'datatype', u'length', u'required', u'primaryKey', u'foreignKeys']
        cmp = lambda x,y: 1 if x in order and y in order and order.index(x) > order.index(y) else -1
        for i, column in enumerate(schema["columns"]):
            schema["columns"][i] = OrderedDict(sorted(column.items(), cmp=cmp, key=lambda x:x[0]))

        #Adding created schema to CSVW dictionary
        csvw_json_data["tableSchema"] = schema
        #Turn OrderedDict into JSON string
        csvw_json_string_data = json.dumps(csvw_json_data, indent=2)
        return csvw_json_string_data

    def link_json_to_csv(self, csv_resource, json_resource):
        ckan_api.action.resource_update(id=csv_resource["id"], url=csv_resource["url"], conformsTo=json_resource["url"])

    def find_existing_json_for_resource(self, resource, pkg_dict):
        """ 
            Function determines if CSVW file for a given CSV file exists. 
            First, it checks if the 
            (this field is created by dpp-dev/ckanext-scheming extension)
            If field does not exist, it falls back on finding CSVW file
            by looking through filenames of existing resources in same package
            and using the one that has the same filename as a CSVW constructed
            for given resource would.
            If CSVW exists, returns URL to download it, URL type and CSVW file resource ID. Otherwise, returns None, None and None.
        """
        json_url = None
        json_res_id = None
        url_type = None

        json_resources = [r for r in pkg_dict["resources"] if r["format"] == "JSON"]

        #Check doesn't work when there's an external CSVW file linked
        #if not json_resources: 
        #    log.debug("No CSVW found for JSON!")
        #    return None, None, None

        if "conformsTo" in resource and resource["conformsTo"]:
            #the "conformsTo" field stores metadata JSON URL
            json_url = resource["conformsTo"]
            for res in json_resources:
                if res["url"] == json_url:
                    #Found a fitting JSON resource!
                    json_res_id = res["id"]
                    url_type = res["url_type"] if "url_type" in res else None
                    return json_url, url_type, json_res_id
            #Now, the json_url is set in the resource description, but it doesn't belong to a resource.
            #So, we return its URL, but indicate it has no resource associated
            #So next time the CSVW is regenerated, it'll be saved as a new resource
            log.info("CSV file {} has linked JSON but it can't be found in resource list".format(resource["id"]))
            return json_url, None, None
            

        #Couldn't find a relevant JSON by metadata    
        log.info("CSV file has no conformsTo field - falling back on filename-based detection")
        resource_filename = self.filename_from_url(resource["url"])
        json_filename = self.make_json_filename(resource_filename)
        for res in json_resources:
            #Going through resources to see if JSON with same filename exists
            #If there are multiple versions with same filenames, selecting the first one we find
            if self.filename_from_url(res["url"]) == json_filename:
                json_url = res["url"]
                json_res_id = res["id"]
                url_type = res["url_type"] if "url_type" in res else None
                return json_url, url_type, json_res_id

        #It seems we didn't find anything
        log.debug("No CSVW found for JSON!")
        return None, None, None

    def filename_from_url(self, url):
        """
           Given an URL, returns its last part which, most likely, contains the resource's filename.
        """
        #(Ab)uses os.path.split, though url.rsplit() could also work.
        return os.path.split(url)[1]

    def make_json_filename(self, resource_filename):
        """
            Defines how the JSON file filename is constructed.
            This is a fallback method for determining which JSON belongs to which CSVW.
            Changing this format will change both creation of new files 
            and matching to old files, Accordingly, it can
            break backwards compatibility unless code that 
            relies on this function is modified accordingly.
        """
        filename_no_ext = resource_filename.rsplit('.', 1)[0]
        return "{}_metadata.json".format(filename_no_ext)

    def resource_csv(self, id, resource_id):
        """
            Controller for "CSV metadata" page 
        """
        #Getting information about the package and resource
        #It's necessary for the base template to render, and we can also use the data ourselves
        try:
            tk.c.pkg_dict = tk.get_action('package_show')(None, {'id': id})
            tk.c.resource = tk.get_action('resource_show')(None, {'id': resource_id})
        except (logic.NotFound, logic.NotAuthorized):
            base.abort(404, _('Resource not found'))

        if str(tk.c.resource["format"]) != "CSV":
            return base.render('csvmetadata/resource_csv.html',
                           extra_vars={'status':'not_csv'})

        #getting form schema to pass to template
        form_schema = self.get_form_schema()
        
        #getting resource url, checking if it's good
        resource_url = tk.c.resource["url"] if "url" in tk.c.resource else ""
        if not resource_url:
            #No URL shown or empty URL, failing early
            return base.render('csvmetadata/resource_csv.html',
                           extra_vars={'status':'url_fail'})

        resource_filename = self.filename_from_url(resource_url)
        #Info about other resources - for JSON file lookup
        other_resources = [resource for resource in tk.c.pkg_dict["resources"] if resource["id"] != resource_id]
        
        #Checking if we're in a POST request - 
        #then we need to create JSON from received data and upload it
        if tk.request.method == 'POST':
            #Loading data from form
            form_data = tk.request.POST
            csvw_string = self.form_to_csvw(form_data)
            io_object = StringIO(csvw_string)
            
            filename = self.make_json_filename(resource_filename)
            #monkeypatching because ckanapi gets filename from descriptor
            io_object.name = filename

            resource_name = filename
            
            x, x, json_resource_id = self.find_existing_json_for_resource(tk.c.resource, tk.c.pkg_dict)
            if json_resource_id:
                log.info("Updating CSVW resource")
                json_resource = ckan_api.action.resource_update(id=json_resource_id, url="", upload=io_object)
            else:
                log.info("Creating a new CSVW resource")
                json_resource = ckan_api.action.resource_create(package_id=id, name=resource_name, url="", upload=io_object)

            self.link_json_to_csv(tk.c.resource, json_resource)
                            
            #Successfully uploaded, now redirecting to the package contents page to show that JSON file was created successfully
            core_helpers.redirect_to(
                controller='package',
                action='read',
                id=id
            )

        #POST request processing code didn't continue, assuming GET method
        json_url, url_type, x = self.find_existing_json_for_resource(tk.c.resource, tk.c.pkg_dict)
        values = {}
        if json_url:
            #Some kind of JSON URL is found, let's fetch it and get CSV header descriptions
            try:
                json_dict = self.fetch_json_return_values(json_url, url_type)
                values = self.csvw_to_form(json_dict)
            except Exception as e:
                logging.warning("Exception while getting JSON:")
                logging.warning(repr(e))
                pass #JSON is either unfetchable or badly constructed, so we won't use it
        
        url_type = tk.c.resource["url_type"] if "url_type" in tk.c.resource else None
        #Getting CSV from the resource url
        status, csv_headers, csv_info = self.get_csv_sample(resource_url, url_type)

        return base.render('csvmetadata/resource_csv.html',
                           extra_vars={'status':status, 
                                       'csv_headers':csv_headers, 
                                       'schema':form_schema, 
                                       'values':values,
                                       'csv_info':repr(csv_info)})


class CSVMetadataPlugin(p.SingletonPlugin, DefaultTranslation):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.ITranslation)

    #IConfigurer
    def update_config(self, config):
        tk.add_template_directory(config, 'templates')
        tk.add_resource('fanstatic', 'csvmetadata')

    #IConfigurable
    def configure(self, config):
        global ckan_site_url, ckan_root_path, ckan_api_key

        for config_option in ('ckan.site_url', 'csvmetadata.ckan_api_key'):
            if not config.get(config_option):
                raise Exception(
                    'Config option `{0}` must be set to use CSVMetadata.'
                    .format(config_option))
        ckan_site_url = config.get('ckan.site_url')
        ckan_root_path = config.get('ckan.root_path')
        if isinstance(ckan_root_path, basestring):
            if "{{LANG}}" in ckan_root_path:
                ckan_root_path = ckan_root_path.replace('{{LANG}}', '')
                ckan_root_path = ckan_root_path.replace('//', '/')
            ckan_root_path = ckan_root_path.strip('/')
        ckan_api_key = config.get('csvmetadata.ckan_api_key')

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
            action='resource_csv', ckan_icon='table')
        return m

    #ITemplateHelpers
    def get_helpers(self):
        return {'csvmetadata_language_text': csvmetadata_language_text}
