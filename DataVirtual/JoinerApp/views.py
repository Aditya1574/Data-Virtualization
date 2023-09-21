# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.http import HttpResponse
from django.shortcuts import render
import mysql.connector
from cassandra.cluster import Cluster
import json
import pandas as pd
import xml.etree.ElementTree as ET
import redis

cluster = Cluster()
session = cluster.connect('samplemeta')


# UTILITY FUNCTIONS

def get_filename(file_path):
    start = file_path.rfind('\\')
    end = file_path.rfind('.')
    file_name = file_path[start + 1:end]
    return file_name


def JSONflatten_dict(d,items, parent_key='', sep='_'):
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(JSONflatten_dict(v,items, new_key, sep=sep))
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                for i, item in enumerate(v):
                    items.update(JSONflatten_dict(item,items, f"{new_key}_{i}", sep=sep))
            else:
                if new_key not in items:
                    items[new_key] = v
        else:
            if new_key not in items:
                items[new_key] = v
    return items



# fucntion for column names

""" 
# Your first JSON record (replace with your data)
json_data = {
    "employee": {
        "id": 12345,
        "name": "John Doe",
        "phones": [{"type": "home", "number": "555-1234"}, {"type": "work", "number": "555-5678"}]
    }
}
# Flatten the JSON data
flattened_data = flatten_dict(json_data)

# Print the flattened data
for key, value in flattened_data.items():
    print(f"{key}: {value}")
"""


def flatten_json(json_obj, flattened_data,conf_list,primary_key, separator='_', parent_key=''):
    for key, value in json_obj.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened_data.update(flatten_json(value, flattened_data,conf_list,primary_key, separator, new_key))
        elif isinstance(value, list):
            sub_dict = {}
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    sub_dict.update(flatten_json(item, flattened_data,conf_list,primary_key, separator, f"{new_key}{separator}{i}"))
                else:
                    sub_key = f"{new_key}{separator}{key}"
                    if sub_key not in sub_dict:
                        sub_dict[sub_key] = []
                    sub_dict[sub_key].append(item)
            flattened_data.update(sub_dict)
        else:
            if new_key not in flattened_data:
                flattened_data[new_key] = []
            flattened_data[new_key].append(value)
    limit = len(flattened_data[primary_key])
    for col_name in conf_list:
        if col_name not in flattened_data:
            flattened_data[col_name] = []
        if len(flattened_data[col_name]) < limit:
            flattened_data[col_name].append("")
    return flattened_data

"""

# usage

xml_data = # enclosed inside triple quotes also inside root element

# Parse the XML data
root = ET.fromstring(xml_data)

# Flatten the XML data
flattened_data = {}
flattened_data = get_xml_data(root, flattened_data)

# Print the flattened data
for key, value in flattened_data.items():
    print(f"{key}: {value}")
"""


# for extractiing the column names for xml data using the first record

def flatten_xml(element, flattened_keys, parent_key='', separator='.'):
    for i, child_element in enumerate(element):
        child_key = parent_key + separator + child_element.tag
        if len(child_element) > 0:
            if len(child_element) == 2:
                child_key = child_key + f".[{i}]"
            flatten_xml(child_element, flattened_keys, child_key, separator)
        else:
            child_key = child_key
            if child_key not in flattened_keys:
                flattened_keys.append(child_key)
    return flattened_keys


"""

xml_data = # first xml record enclosed inside triple quotes

# Parse the XML data
root = ET.fromstring(xml_data)

# Flatten the XML data
flattened_data = []
flattened_data = flatten_xml(root, flattened_data)

# Print the flattened data
for element in flattened_data:
    print(element)
"""


def get_xml_data(element, flattened_data, parent_key='', separator='.'):
    for i, child_element in enumerate(element):
        child_key = parent_key + separator + child_element.tag
        if len(child_element) > 0:
            if len(child_element) == 2:
                child_key = child_key + f".[{i}]"
            child_data = get_xml_data(child_element, flattened_data, child_key, separator)
            if child_key in flattened_data:
                if not isinstance(flattened_data[child_key], list):
                    flattened_data[child_key] = [flattened_data[child_key]]
                flattened_data[child_key].append(child_data[child_key])
            else:
                flattened_data.update(child_data)
        else:
            child_key = child_key
            if child_key in flattened_data:
                if not isinstance(flattened_data[child_key], list):
                    flattened_data[child_key] = [flattened_data[child_key]]
                flattened_data[child_key].append(child_element.text)
            else:
                flattened_data[child_key] = [child_element.text]
    # logic for making sure that the number of records are equal to the number of users

    # print(flattened_data)
    return flattened_data


# function for extracting data from xml

def GetActualNameForCustomName(databasetype, custom_name, databasename):
    actual_name = ""
    if databasetype == 'relational':
        cassandra_query = f"select tablename from relationalmetadata where databasename='{databasename}'" \
                          f" and custom_name='{custom_name}' limit 1;"
        result = session.execute(cassandra_query)[0]
        actual_name = result.tablename
    elif databasetype == 'excel':
        cassandra_query = f"select sheetname from excelmetadata where filename='{databasename}' " \
                          f"and custom_name='{custom_name}' limit 1;"
        print(cassandra_query)
        result = session.execute(cassandra_query)[0]
        actual_name = result.sheetname
    return actual_name


def GetColumnNamesFromMeta(databasename, custom_name):
    # searching using custom_name as only that is available per table in databasee
    cassandra_query = f"select datafields from relationalmetadata where databasename='{databasename}' and custom_name='{custom_name}'"
    result = session.execute(cassandra_query)[0]
    return result.datafields


def GetColumnNamesFromSheetNameFromMeta(excel_filepath, custom_name):
    excel_filename = get_filename(excel_filepath)
    print(excel_filename)
    # searching using custom_name as only that is available per sheet in the excel_file[name]
    query = f"select datafieldmapper from excelmetadata where filename='{excel_filename}' and custom_name='{custom_name}'"
    result = session.execute(query)[0]
    column_names = list(result.datafieldmapper.values())
    return column_names


def GetActualColumnNamesFromSheetNameFromMeta(excel_filepath, custom_name):
    excel_filename = get_filename(excel_filepath)
    print("inside the get actual name function : ", excel_filename, excel_filepath)
    # searching using custom_name as only that is available per sheet in the excel_file[name]
    query = f"select datafieldmapper from excelmetadata where filename='{excel_filename}' AND custom_name='{custom_name}'"
    print(query)
    result = session.execute(query).one()
    print(result)
    column_names = list(result.datafieldmapper.keys())
    return column_names


def GetXMLFlattenAttrNamesFromMeta(xml_filepath):
    xml_filename = get_filename(xml_filepath)
    # searching using filename as filename is available using the filepath
    query = f"select datafieldmapper from xmlmetadata where filename='{xml_filename}';"
    result = session.execute(query)[0]
    AttrNames = list(result.datafieldmapper.values())
    return AttrNames


def GetActualXMLFlattenAttrNamesFromMeta(xml_filepath):
    xml_filename = get_filename(xml_filepath)
    # searching using filename as filename is available using the filepath
    print("inside the Get actual function : ", xml_filename, xml_filepath)
    query = f"select datafieldmapper from xmlmetadata where filename='{xml_filename}';"
    result = session.execute(query)[0]
    AttrNames = list(result.datafieldmapper.values())
    return AttrNames


def GetJSONFlattenAttrNamesFromMeta(json_filepath):
    json_filename = get_filename(json_filepath)
    # searching using filename as filename is available using the filepath
    query = f"select datafieldmapper from jsonmetadata where filename='{json_filename}';"
    result = session.execute(query)[0]
    AttrNames = list(result.datafieldmapper.values())
    return AttrNames


def GetActualJSONFlattenAttrNamesFromMeta(json_filepath):
    json_filename = get_filename(json_filepath)
    # searching using filename as filename is available using the filepath
    query = f"select datafieldmapper from jsonmetadata where filename='{json_filename}';"
    result = session.execute(query)[0]
    AttrNames = list(result.datafieldmapper.keys())
    return AttrNames


def GetXMLFlattenAttrNamesFromSource(xml_filepath):
    tree = ET.parse(xml_filepath)
    root = tree.getroot()

    flattened_data = []
    for element in root.iter(root.tag):
        flattened_data = flatten_xml(element,flattened_data)

    return [col_name[col_name[1:].find('.'):][1:] for col_name in flattened_data]

def GetJSONFlattenAttrNamesFromSource(json_filepath):
    with open(json_filepath, 'r') as file:
        data = json.load(file)

    # Access the first object (usually the first item in a JSON array)
    # first_object = data[0]

    items = {}
    for item in data:
        items = JSONflatten_dict(item, items)

    return list(items.keys())


def ReverseDict(original_dict):
    reversed_dict = {value: key for key, value in original_dict.items()}

    return reversed_dict


def ExcelCustomToOrignal(excel_filename, values, custom_name):
    query = f"select datafieldmapper from excelmetadata where filename='{excel_filename}' and custom_name='{custom_name}';"
    result = session.execute(query)[0]

    customtoorignal = ReverseDict(result.datafieldmapper)

    actual = []
    for col in values:
        actual.append(customtoorignal[col])

    return actual


def JSONCustomToOrignal(json_filename, values):
    query = f"select datafieldmapper from jsonmetadata where filename='{json_filename}';"
    result = session.execute(query)[0]
    customtoorignal = ReverseDict(result.datafieldmapper)
    actual = []
    for col in values:
        actual.append(customtoorignal[col])
    return actual


def XMLCustomToOrignal(xml_filename, values):
    query = f"select datafieldmapper from xmlmetadata where filename='{xml_filename}';"
    result = session.execute(query)[0]

    customtoorignal = ReverseDict(result.datafieldmapper)

    actual = []
    for col in values:
        actual.append(customtoorignal[col])

    return actual


def check_relationaldatabase(databasename):
    query = "select databasename from relationalmetadata group by databasename"
    result = session.execute(query)
    for row in result:
        if row.databasename == databasename:
            return True
    return False


def check_exceldatabase(filename):
    query = "select filename from excelmetadata group by filename"
    result = session.execute(query)
    for row in result:
        if row.filename == filename:
            return True
    return False


def check_jsondatabase(filename):
    query = "select filename from jsonmetadata group by filename"
    result = session.execute(query)
    for row in result:
        if row.filename == filename:
            return True
    return False


def check_xmldatabase(filename):
    query = "select filename from xmlmetadata group by filename"
    result = session.execute(query)
    for row in result:
        if row.filename == filename:
            return True
    return False


def GetRelationalDataAsDataFrame(databasename, table_name, column_names):
    cassandra_query = f"select db_config from relationalmetadata where databasename='{databasename}' limit 1"
    result = session.execute(cassandra_query)[0]
    # print(result)
    db_config = {
        "host": f"{result.db_config.host}",
        "user": f"{result.db_config.user}",
        "password": f"{result.db_config.password}",
        "database": f"{result.db_config.database}"
    }
    # 2. connect to mysql
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    # 3. make a query to mysql server and fetch the data
    columns_for_query = ",".join(column_names)
    query = f"select {columns_for_query} from {table_name}"
    cursor.execute(query)
    rows = cursor.fetchall()
    # 4. convert the data into polars dataframe
    relational_df = pd.DataFrame(rows, columns=column_names)

    # print("Relational Dataframe \n", relational_df)

    return relational_df


def GetJSONDataAsDataFrame(json_filepath,attr_names,primary_key='id'):
    # reading json data and gettig it as a dictionary
    myjson_data = []
    try:
        with open(json_filepath, 'r') as json_file:
            myjson_data = json_file.read()
        # Now, json_data contains the entire JSON content as a string
        myjson_data = json.loads(myjson_data)
    except FileNotFoundError:
        print(f"JSON File not found: {json_filepath}")

    conf_list = GetJSONFlattenAttrNamesFromSource(json_filepath)
    json_flattened_data = {}
    for json_data in myjson_data:
        json_flattened_data = flatten_json(json_data, json_flattened_data,conf_list,primary_key)

    # DF_JSON
    json_df = pd.DataFrame(json_flattened_data)

    print(json_df)
    return json_df[attr_names]


def GetXMLDataAsDataFrame(xml_filepath, attr_names):
    xml_data = ""
    try:
        with open(xml_filepath, 'r') as xml_file:
            xml_data = xml_file.read()
            # Now, xml_data contains the entire XML document as a string
    except FileNotFoundError:
        print(f"XML File not found: {xml_filepath}")

    xml_flattened_data = {}
    root = ET.fromstring(xml_data)
    print(root)

    # Flatten  XML data

    conf_list = GetXMLFlattenAttrNamesFromSource(xml_filepath)

    print(conf_list)
    xml_flattened_data = {}

    for elements in root.iter(root.tag):
        number_of_record = 0
        for element in elements:
            number_of_record = number_of_record + 1
            xml_flattened_data = get_xml_data(element, xml_flattened_data)
            print("\n",xml_flattened_data)
            for col in conf_list:
                if col not in xml_flattened_data:
                    xml_flattened_data[col] = []
                if len(xml_flattened_data[col]) < number_of_record:
                    xml_flattened_data[col].append("")
    # DF_XML
    xml_df = pd.DataFrame(xml_flattened_data)

    return xml_df[attr_names]


def GetExcelDataAsDataFrame(excel_filepath, sheet_name, colunms):
    try:
        # Read the Excel file into a Pandas DataFrame
        df = pd.read_excel(excel_filepath, sheet_name=sheet_name)

        # Select only the required columns from the DataFrame
        df = df[colunms]

        return df
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def BifervateElement(element):
    start = element.rfind(':')
    end = element.find(':')
    return element[:end], element[start + 1:]


def ConvertDICTCustomToOrignal(table_columns, excel_filename, json_filename, xml_filename):
    table_columns_orignal = dict()
    for key, values in table_columns.items():
        element_name, element_type = BifervateElement(key)
        if element_type == 'relational':
            table_columns_orignal[key] = values
        elif element_type == 'excel':
            table_columns_orignal[key] = ExcelCustomToOrignal(excel_filename, values, element_name)
        elif element_type == 'JSON':
            table_columns_orignal[key] = JSONCustomToOrignal(json_filename, values)
        elif element_type == 'XML':
            table_columns_orignal[key] = XMLCustomToOrignal(xml_filename, values)
    return table_columns_orignal


def ConvertJoiningConditionsToOrignal(table_joining_conditions, excel_filename, json_filename, xml_filename):
    table_joining_conditions_orignal = dict()
    print(table_joining_conditions)
    for index, (key, value) in enumerate(table_joining_conditions.items()):
        if index == len(table_joining_conditions) - 1:
            table_joining_conditions_orignal[key] = {'join_columns': []}
            continue
        element = value['join_table']
        element_name, element_type = BifervateElement(element)
        if element_type == 'relational':
            new_dict = dict()
            new_values = value['join_columns']
            new_dict['join_table'] = element
            new_dict['join_columns'] = new_values
            table_joining_conditions_orignal[key] = new_dict
        elif element_type == 'excel':
            new_dict = dict()
            new_values = ExcelCustomToOrignal(excel_filename, value['join_columns'], element_name)
            new_dict['join_table'] = element
            new_dict['join_columns'] = new_values
            table_joining_conditions_orignal[key] = new_dict
        elif element_type == 'JSON':
            new_dict = dict()
            new_values = JSONCustomToOrignal(json_filename, value['join_columns'])
            new_dict['join_table'] = element
            new_dict['join_columns'] = new_values
            table_joining_conditions_orignal[key] = new_dict
        elif element_type == 'XML':
            new_dict = dict()
            new_values = XMLCustomToOrignal(xml_filename, value['join_columns'])
            new_dict['join_table'] = element
            new_dict['join_columns'] = new_values
            table_joining_conditions_orignal[key] = new_dict

    return table_joining_conditions_orignal


# ROUTES

def TheRunner(request):
    return render(request, 'index.html')


def options_selected(request):
    if request.method == 'POST':
        selected_options = request.POST.getlist('options[]')
        context = {
            'data_sources': selected_options
        }
        return render(request, 'data_collection.html', context)


def collected_data_processing(request):
    if request.method == 'POST':
        relational_present = True
        xml_present = True
        json_present = True
        excel_present = True

        db_name = request.POST.get('db_name')
        if db_name:  # has come ?
            relational_present = check_relationaldatabase(db_name)
            if not relational_present:  # if not present then dont sent the name
                db_name = ""
        xml_file = request.POST.get('xml_file')
        if xml_file:
            xml_present = check_xmldatabase(xml_file)
            if not xml_present:
                xml_file = ""
        json_file = request.POST.get('json_file')
        if json_file:
            json_present = check_jsondatabase(json_file)
            if not json_present:
                json_file = ""
        excel_file = request.POST.get('excel_file')
        if excel_file:
            excel_present = check_exceldatabase(excel_file)
            if not excel_present:
                excel_file = ""

        if relational_present and xml_present and json_present and excel_present:
            final_tables = dict()
            excel_filepath = ""
            xml_filepath = ""
            json_filepath = ""
            if db_name:  # checking if the name was entered
                # fetch  custom_name and datafields
                cassandra_query = f"select custom_name,datafields from relationalmetadata where databasename='{db_name}'"
                result = session.execute(cassandra_query)
                print("all the entered  databases are present in the meta data repo : ",result)
                for element in result:
                    print("I am inside")
                    print(element)
                    final_tables[element.custom_name + ":relational"] = element.datafields

            if excel_file:
                # fetch  custom_name and datafieldampper : values
                cassandra_query = f"select custom_name,datafieldmapper from excelmetadata where filename='{excel_file}'"
                result = session.execute(cassandra_query)
                for element in result:
                    final_tables[element.custom_name + ":excel"] = list(element.datafieldmapper.values())
                cassandra_query = f"select filepath from excelmetadata where filename='{excel_file}'  limit 1"
                result = session.execute(cassandra_query)[0]
                excel_filepath = result.filepath
                print("inside of /selected_data  : route excel : ", excel_filepath)
            if json_file:
                # fetch  custom_name and datafieldmapper : values
                cassandra_query = f"select custom_name,datafieldmapper from jsonmetadata where filename='{json_file}' limit 1;"
                result = session.execute(cassandra_query)[0]
                final_tables[result.custom_name + ":JSON"] = list(result.datafieldmapper.values())
                cassandra_query = f"select filepath from jsonmetadata where filename='{json_file}' limit 1"
                result = session.execute(cassandra_query)[0]
                json_filepath = result.filepath
                print("inside of /selected_data  : route JSON : ", json_filepath)
            if xml_file:
                # fetch  custom_name and datafieldmapper :values
                cassandra_query = f"select custom_name,datafieldmapper from xmlmetadata where filename='{xml_file}' limit 1;"
                result = session.execute(cassandra_query)[0]
                final_tables[result.custom_name + ":XML"] = list(result.datafieldmapper.values())
                cassandra_query = f"select filepath from xmlmetadata where filename='{xml_file}' limit 1;"
                result = session.execute(cassandra_query)[0]
                xml_filepath = result.filepath
            print(final_tables)
            # print("in present route before : ", excel_filepath, xml_filepath, json_filepath)
            excel_filepath=excel_filepath.replace("\\", "\\\\")
            json_filepath=json_filepath.replace("\\", "\\\\")
            xml_filepath=xml_filepath.replace("\\", "\\\\")
            # print("in present route  : ", excel_filepath,xml_filepath,json_filepath)
            context = {
                'table_data': final_tables,
                'databasename': db_name,
                'excel_filepath': excel_filepath,
                'json_filepath': json_filepath,
                'xml_filepath': xml_filepath
            }
            return render(request, 'view_selection.html', context)

        context = {
            'databasename': db_name,
            'xml_filename': xml_file,
            'excel_filename': excel_file,
            'json_filename': json_file,
            'relational_present': relational_present,
            'xml_present': xml_present,
            'json_present': json_present,
            'excel_present': excel_present
        }
        return render(request, 'data_entry.html', context)


def send_to_tableSelection(request):
    if request.method == 'POST':
        # databasename for relational database :it must come only for those who exit in cassandra
        databasename = request.POST.get('databasename')
        # file-names : they must come only for those who exit in cassandra
        excel_filename = request.POST.get('excel_filename')
        xml_filename = request.POST.get('xml_filename')
        json_filename = request.POST.get('json_filename')
        # file-paths
        json_filepath = ""
        xml_filepath = ""
        excel_filepath = ""
        # RELATIONAL
        relational_present = request.POST.get('relational-data')
        # print("relational_present : ", relational_present, " ", type(relational_present))
        # print("database name : ", databasename," ", type(databasename))
        relational_query_index = dict()
        # database requirements
        host = ""
        user = ""
        password = ""
        # EXCEL
        excel_present = request.POST.get('excel-data')
        # print("excel_present : ", excel_present, " ", type(excel_present))
        # print("excel_filename : ", excel_filename, " ", type(excel_filename))
        excel_query_index = dict()
        # JSON
        json_present = request.POST.get('json-data')
        # print("json_present : ", json_present, " ", type(json_present))
        # print("json_filename : ", json_filename, " ", type(json_filename))
        json_columns = []
        #   XML
        xml_present = request.POST.get('xml-data')
        # print("xml_present : ", xml_present, " ", type(xml_present))
        # print("xml_filename : ", xml_filename, " ", type(xml_filename))
        xml_columns = []

        if relational_present is not None:
            # if the data for this was entered >> was chosen but not in meta
            # >> take the metadata to display using the datasource
            relational_data = json.loads(request.POST.get('relational-data'))
            db_config = {
                "host": relational_data['hostname'],
                "user": relational_data['user'],
                "password": relational_data['password'],
                "database": relational_data['database']
            }
            host = relational_data['hostname']
            user = relational_data['user']
            password = relational_data['password']
            databasename = relational_data['database']

            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()

            # Replace with the desired table name
            query = "SHOW TABLES"

            # Execute the query
            cursor.execute(query)

            # Fetch the table names
            tables = cursor.fetchall()

            # Extract table names from the fetched data
            table_names = [table[0] for table in tables]

            for table_name in table_names:
                try:
                    query = f"SHOW COLUMNS FROM {table_name}"

                    cursor.execute(query)

                    # Fetch the column information
                    columns = cursor.fetchall()

                    relational_query_index[table_name] = [column[0] for column in columns]
                except mysql.connector.Error as err:
                    print("Error:", err)
        elif databasename != "None":  #
            cassandra_query = f"select db_config from relationalmetadata where databasename='{databasename}' limit 1;"
            result = session.execute(cassandra_query)[0]
            host = result.db_config.host
            user = result.db_config.user
            password = result.db_config.password
            databasename = result.db_config.database

        if excel_present is not None:
            excel_data = json.loads(request.POST.get('excel-data'))
            excel_filepath = excel_data['excel_filepath']
            excel_filename = get_filename(excel_filepath)
            # Using ExcelFile object
            xls = pd.ExcelFile(excel_filepath)
            sheet_names = xls.sheet_names
            for sheet_name in sheet_names:
                df = pd.read_excel(excel_filepath, sheet_name=sheet_name)
                excel_query_index[sheet_name] = df.columns.tolist()

        elif excel_filename != "None":
            # required for getting the path of the corrosponding file which is existing in cassandra and send it from here on!
            cassandra_query = f"select filepath from excelmetadata where filename='{excel_filename}' limit 1"
            result = session.execute(cassandra_query)[0]
            excel_filepath = result.filepath

        if xml_present is not None:
            xml_data = json.loads(request.POST.get('xml-data'))
            xml_filepath = xml_data['xml_filepath']
            xml_filename = get_filename(xml_filepath)
            xml_columns = GetXMLFlattenAttrNamesFromSource(xml_filepath)
        elif xml_filename != "None":
            cassandra_query = f"select filepath from xmlmetadata where filename='{xml_filename}' limit 1"
            result = session.execute(cassandra_query)[0]
            xml_filepath = result.filepath

        if json_present is not None:
            json_data = json.loads(request.POST.get('json-data'))
            json_filepath = json_data['json_filepath']
            json_filename = get_filename(json_filepath)
            json_columns = [key for key in GetJSONFlattenAttrNamesFromSource(json_filepath)]
        elif json_filename != "None":
            cassandra_query = f"select filepath from jsonmetadata where filename='{json_filename}' limit 1"
            result = session.execute(cassandra_query)[0]
            json_filepath = result.filepath

        context = {
            'table_data': relational_query_index,
            'excel_data': excel_query_index,
            'json_columns': json_columns,
            'xml_columns': xml_columns,
            'excel_filename': excel_filename,
            'json_filename': json_filename,
            'xml_filename': xml_filename,
            'excel_filepath': excel_filepath,
            'xml_filepath': xml_filepath,
            'json_filepath': json_filepath,
            'db_name': databasename,
            'host': host,
            'user': user,
            'password': password
        }
        return render(request, 'add_tables_sheets.html', context)


def SeeSampleData(request):
    if request.method == 'POST':
        table_name = request.POST.get('table_name')

        print(table_name)
        element_name, element_type = BifervateElement(table_name)
        show_df = pd.DataFrame()
        column_names = []
        column_names_show = []
        if element_type == 'relational':
            databasename = request.POST.get('databasename')
            column_names = GetColumnNamesFromMeta(databasename, element_name)
            column_names_show = column_names
            tablename = GetActualNameForCustomName(element_type, element_name, databasename)
            show_df = GetRelationalDataAsDataFrame(databasename, tablename, column_names)
        elif element_type == 'excel':
            excel_filepath = request.POST.get('excel_filepath')
            excel_filename = get_filename(excel_filepath)
            print("excel_filepath : ", excel_filepath)
            column_names = GetActualColumnNamesFromSheetNameFromMeta(excel_filepath, element_name)
            column_names_show = GetColumnNamesFromSheetNameFromMeta(excel_filepath, element_name)
            sheetname = GetActualNameForCustomName(element_type, element_name, excel_filename)
            show_df = GetExcelDataAsDataFrame(excel_filepath, sheetname, column_names)
        elif element_type == 'JSON':
            json_filepath = request.POST.get('json_filepath')
            column_names_show = GetJSONFlattenAttrNamesFromMeta(json_filepath)
            column_names = GetActualJSONFlattenAttrNamesFromMeta(json_filepath)
            json_primary_key = getprimarykeyforjsonfile(json_filepath,element_name)
            show_df = GetJSONDataAsDataFrame(json_filepath, column_names,json_primary_key)
        elif element_type == 'XML':
            xml_filepath = request.POST.get('xml_filepath')
            column_names = GetActualXMLFlattenAttrNamesFromMeta(xml_filepath)
            column_names_show = GetXMLFlattenAttrNamesFromMeta(xml_filepath)
            show_df = GetXMLDataAsDataFrame(xml_filepath, column_names)

        dataframe_dict_list = show_df.to_dict(orient='records')
        print(column_names)
        # print(dataframe_dict_list)
        row_list = []
        for row in dataframe_dict_list:
            row_elem = []
            for col in column_names:
                row_elem.append(row[col])
            row_list.append(row_elem)

        # print(row_list)
        context = {
            'rows': row_list,
            'actual_columns': column_names,
            'columns': column_names_show
        }
        return render(request, 'combined_result.html', context)


def SaveAddedtableMetaData(request):
    if request.method == 'POST':
        # SAVING METADATA FOR RELATIONAL DATABASE
        db_name = request.POST.get('db_name')
        excel_filepath = request.POST.get('excel_filepath')
        json_filepath = request.POST.get('json_filepath')
        xml_filepath = request.POST.get('xml_filepath')
        final_tables = dict()
        # print("Relational : ", request.POST.get('selected_primary_keys_forTables'),
        # bool(json.loads(request.POST.get('selected_primary_keys_forTables'))))
        print("databasename : ", db_name, " ", type(db_name))

        if request.POST.get('selected_primary_keys_forTables') != "{}" and request.POST.get(
                'selected_primary_keys_forTables') != "":
            tables_primary_keys = json.loads(request.POST.get('selected_primary_keys_forTables'))
            db_name = request.POST.get('db_name')
            db_config = {
                "host": request.POST.get('host'),
                "user": request.POST.get('user'),
                "password": request.POST.get('password'),
                "database": request.POST.get('db_name')
            }
            db_config_string = f"host: '{request.POST.get('host')}'," \
                               f"user : '{request.POST.get('user')}'," \
                               f"password: '{request.POST.get('password')}', " \
                               f"database : '{request.POST.get('db_name')}'"
            # 3. taking out the column names for "Adding tables to the POC" : inserting data to cassandra
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            for table_name, primary_key in tables_primary_keys.items():
                try:
                    query = f"SHOW COLUMNS FROM {table_name}"
                    cursor.execute(query)

                    # Fetch the column information
                    columns = cursor.fetchall()
                    # this goes up for being displayed in the front-end
                    column_names = [f"'{column[0]}'" for column in columns]
                    column_names_string = ','.join(map(str, column_names))
                    custom_name = request.POST[f"relational_custom_{table_name}"]
                    final_tables[custom_name + ":relational"] = [column[0] for column in columns]
                    cassandra_query = f"insert into relationalmetadata(databasename,custom_name,primary_key,tablename," \
                                      f"db_config,datafields) values(" \
                                      f"'{db_name}'," \
                                      f"'{custom_name}'," \
                                      f"'{primary_key}'," \
                                      f"'{table_name}'," \
                                      "{" + f"{db_config_string}" + "}," \
                                                                    f"[{column_names_string}]" \
                                                                    f");"
                    print(cassandra_query, "\n", "Saved Table metadata to cassandra using the abobe query \n")
                    session.execute(cassandra_query)
                except mysql.connector.Error as err:
                    print("Error:", err)
        elif db_name != "None" and db_name != "":  # means this exists in the meta data repo
            cassandra_query = f"select custom_name,datafields from relationalmetadata where databasename='{db_name}'"
            result = session.execute(cassandra_query)
            for element in result:
                final_tables[result.custom_name + ":relational"] = element.datafields
            # fetch tablename,custom_name and datafieds

        print("Excel : ", request.POST.get('selected_primary_keys_forSheets'))
        print("excel_filepath : ", excel_filepath, " ", type(excel_filepath))

        if request.POST.get('selected_primary_keys_forSheets') != "{}" and request.POST.get(
                'selected_primary_keys_forSheets') != "":
            sheet_primary_keys = json.loads(request.POST.get('selected_primary_keys_forSheets'))
            excel_filepath = request.POST.get('excel_filepath')
            excel_filename = get_filename(excel_filepath)
            for sheet_name, primary_key in sheet_primary_keys.items():
                fetcher_id = f"custom_names_excel_{sheet_name}"
                actual_custom = json.loads(request.POST.get(fetcher_id))
                datafieldmapper_string = ""
                for index, (key, value) in enumerate(actual_custom.items()):
                    datafieldmapper_string += f"'{key}':'{value}'"
                    if index == len(actual_custom.items()) - 1:
                        continue
                    datafieldmapper_string += ","

                custom_name = request.POST[f"sheet_custom_{sheet_name}"]
                final_tables[custom_name + ":excel"] = list(actual_custom.values())
                cassandra_query = f"insert into excelmetadata(filename,custom_name,sheetname," \
                                  f"datafieldmapper,filepath,primary_key)" \
                                  f"values('{excel_filename}'," \
                                  f"'{custom_name}'," \
                                  f"'{sheet_name}'," \
                                  "{" + f"{datafieldmapper_string}" + "}," \
                                                                      f"'{excel_filepath}'," \
                                                                      f"'{primary_key}')"
                session.execute(cassandra_query)
                print(cassandra_query, "\n", "Saved excel metadata to cassandra using the abobe query \n")
        elif excel_filepath != "None" and excel_filepath != "":
            excel_file = get_filename(excel_filepath)
            cassandra_query = f"select custom_name,datafieldmapper from excelmetadata where filename='{excel_file}'"
            result = session.execute(cassandra_query)
            for element in result:
                final_tables[element.custom_name + ":excel"] = list(element.datafieldmapper.values())
            # fetch filename,sheetname, custom_name and datafieldmapper

        print("JSON : ", request.POST.get('selected_primary_keys_forJSON'))
        print("json_filepath : ", json_filepath, " ", type(json_filepath))

        if request.POST.get('selected_primary_keys_forJSON') != "{}" and request.POST.get(
                'selected_primary_keys_forJSON') != "":
            json_filepath = request.POST.get('json_filepath')
            filename_pk = json.loads(request.POST.get('selected_primary_keys_forJSON'))
            filename, primary_key = next(iter(filename_pk.items()))
            actual_custom = json.loads(request.POST.get('custom_names_json'))
            datafieldmapper_string = ""
            for index, (key, value) in enumerate(actual_custom.items()):
                datafieldmapper_string += f"'{key}':'{value}'"
                if index == len(actual_custom.items()) - 1:
                    continue
                datafieldmapper_string += ","

            custom_name = request.POST[f"jsonfile_custom_{filename}"]
            final_tables[custom_name + ":JSON"] = list(actual_custom.values())
            cassandra_query = f"insert into jsonmetadata(filename,custom_name,filepath," \
                              f"datafieldmapper,primary_key) values(" \
                              f"'{filename}'," \
                              f"'{custom_name}'," \
                              f"'{json_filepath}'," \
                              "{" + f"{datafieldmapper_string}" + "}," \
                               f"'{primary_key}" \
                               f")"
            session.execute(cassandra_query)
            print(cassandra_query, "\n", "Saved JSON metadata to cassandra using the abobe query \n")
        elif json_filepath != "None" and json_filepath != "":
            json_file = get_filename(json_filepath)
            cassandra_query = f"select custom_name,datafieldmapper from jsonmetadata where filename='{json_file}'"
            result = session.execute(cassandra_query)[0]
            print(result)
            final_tables[result.custom_name + ":JSON"] = list(result.datafieldmapper.values())
            # fetch filename, custom_name and datafieldmapper : values

        print("XML : ", request.POST.get('selected_primary_keys_forXML'))
        print("xml_filepath : ", xml_filepath, " ", type(xml_filepath))

        if request.POST.get('selected_primary_keys_forXML') != "{}" and request.POST.get(
                'selected_primary_keys_forXML') != "":
            xml_filepath = request.POST.get('xml_filepath')
            filename_pk = json.loads(request.POST.get('selected_primary_keys_forXML'))
            filename, primary_key = next(iter(filename_pk.items()))
            actual_custom = json.loads(request.POST.get('custom_names_xml'))
            datafieldmapper_string = ""
            for index, (key, value) in enumerate(actual_custom.items()):
                datafieldmapper_string += f"'{key}':'{value}'"
                if index == len(actual_custom.items()) - 1:
                    continue
                datafieldmapper_string += ","

            custom_name = request.POST[f"xmlfile_custom_{filename}"]
            final_tables[custom_name + ":XML"] = list(actual_custom.values())
            cassandra_query = f"insert into xmlmetadata(filename,custom_name,filepath,datafieldmapper,primary_key) values(" \
                              f"'{filename}'," \
                              f"'{custom_name}'," \
                              f"'{xml_filepath}'," \
                              "{" + f"{datafieldmapper_string}" + "}," \
                               f"'{primary_key}'" \
                               f")"
            session.execute(cassandra_query)
            print(cassandra_query, "\n", "Saved XML metadata to cassandra using the abobe query \n")
        elif xml_filepath != "None" and xml_filepath != "":
            xml_file = get_filename(xml_filepath)
            cassandra_query = f"select custom_name,datfieldmapper from xmlmetadata where filename='{xml_file}' limit 1"
            result = session.execute(cassandra_query)[0]
            final_tables[result.custom_name + ":XML"] = list(result.datafieldmapper.values())
            # fetch filename, custom_name and datafieldmapper

        print(final_tables)
        excel_filepath=excel_filepath.replace('\\','\\\\')
        json_filepath=json_filepath.replace('\\','\\\\')
        xml_filepath=xml_filepath.replace('\\','\\\\')
        print("in saving data route  : ", excel_filepath, xml_filepath, json_filepath)
        context = {
            'table_data': final_tables,
            'databasename': db_name,
            'json_filepath': json_filepath,
            'xml_filepath': xml_filepath,
            'excel_filepath': excel_filepath
        }
        return render(request, 'view_selection.html', context)


def GenerateColumns(request):
    if request.method == 'POST':
        final_tables_data = dict()
        excel_filepath = request.POST.get('excel_filepath')
        json_filepath = request.POST.get('json_filepath')
        xml_filepath = request.POST.get('xml_filepath')
        databasename = request.POST.get('databasename')
        selected_data = request.POST.getlist('selected_tables[]')

        # Looping through the selected_data thing getting table,sheets,json and xml files in order
        for element_name in selected_data:

            entity_name, db_type = BifervateElement(element_name)
            # 1. relational database
            # must get using cassandra
            if db_type == 'relational':
                final_tables_data[element_name] = GetColumnNamesFromMeta(databasename, entity_name)
            # 2. excel database
            elif db_type == 'excel':
                final_tables_data[element_name] = GetColumnNamesFromSheetNameFromMeta(excel_filepath, entity_name)
            # 3. json database
            elif db_type == 'JSON':
                final_tables_data[element_name] = GetJSONFlattenAttrNamesFromMeta(json_filepath)
            # 4. xml database
            else:
                final_tables_data[element_name] = GetXMLFlattenAttrNamesFromMeta(xml_filepath)

        print(final_tables_data)

        return HttpResponse("Data is selected look into the python console.!")
        # context = {
        #     'data' : final_tables_data,
        #     'databasename' : databasename,
        #     'excel_filepath':excel_filepath,
        #     'json_filepath':json_filepath,
        #     'xml_filepath':xml_filepath
        # # }
        # return render(request,'multiple_ds_select_join_data.html', context)


def getprimarykeyforjsonfile(json_filepath,custom_name):
    json_filename = get_filename(json_filepath)
    cassandra_query = f"select primary_key from jsonmetadata where filename='{json_filename}' and custom_name='{custom_name}';"
    result = session.execute(cassandra_query)[0]
    return result.primary_key

def Datajoiner(request):
    if request.method == 'POST':
        databasename = request.POST.get('databasename')

        excel_filepath = request.POST.get('excel_filepath')
        excel_filename = get_filename(excel_filepath)

        json_filepath = request.POST.get('json_filepath')
        json_filename = get_filename(json_filepath)

        xml_filepath = request.POST.get('xml_filepath')
        xml_filename = get_filename(xml_filepath)

        table_column = json.loads(request.POST.get('displayColumnsData'))
        table_column_orignal = ConvertDICTCustomToOrignal(table_column, excel_filename, json_filename, xml_filename)

        table_join_conditions = json.loads(request.POST.get('joinConditionsData'))
        table_join_conditions_orignal = ConvertJoiningConditionsToOrignal(table_join_conditions, excel_filename,
                                                                          json_filename, xml_filename)

        table_joining_keys = json.loads(request.POST.get('joinedColumnsData'))
        table_joining_keys_orignal = ConvertDICTCustomToOrignal(table_joining_keys, excel_filename, json_filename,
                                                                xml_filename)

        print("inside /get_join_data : \n")

        final_df = pd.DataFrame()

        element, values = next(iter(table_column.items()))
        element_name, element_type = BifervateElement(element)

        ele_custom, value_custom = next(iter(table_column.items()))
        ele_orignal, value_orignal = next(iter(table_column_orignal.items()))
        columns_required = value_orignal
        columns_required_show = value_custom
        # print(element_name,  " : ",element_type )
        # joining columns for the current column
        joining_keys_for_element = table_joining_keys[element]
        # getting all the required columns : (columns requested by the user UNION columns required for joining)
        values = list(set(values).union(set(joining_keys_for_element)))

        if element_type == 'relational':
            final_df = GetRelationalDataAsDataFrame(databasename, element_name, values)
        elif element_type == 'excel':
            values = ExcelCustomToOrignal(excel_filename, values, element_name)
            final_df = GetExcelDataAsDataFrame(excel_filepath, element_name, values)
        elif element_type == 'JSON':
            values = JSONCustomToOrignal(json_filename, values)
            primary_key = getprimarykeyforjsonfile(json_filepath,element_name)
            final_df = GetJSONDataAsDataFrame(json_filepath, values,primary_key)
        elif element_type == 'XML':
            values = XMLCustomToOrignal(xml_filename, values)
            final_df = GetXMLDataAsDataFrame(xml_filepath, values)

        for i, (table_name, columns) in enumerate(table_column.items()):

            if i == len(table_column) - 1:
                continue

            joining_to = table_join_conditions[table_name]['join_table']

            element_name, element_type = BifervateElement(joining_to)

            values = list(set(table_column[joining_to]).union(table_join_conditions[table_name]['join_columns']))
            values = list(set(values).union(table_joining_keys[joining_to]))
            joining_df = pd.DataFrame()

            print(f"joining {table_name} to {joining_to} with {values}")
            print(f"columns of final_df  : {i} ", final_df.columns)

            joining_from_columns_test = final_df.columns

            if element_type == 'relational':
                joining_df = GetRelationalDataAsDataFrame(databasename, element_name, values)
            elif element_type == 'excel':
                values = ExcelCustomToOrignal(excel_filename, values, element_name)
                joining_df = GetExcelDataAsDataFrame(excel_filepath, element_name, values)
            elif element_type == 'JSON':
                values = JSONCustomToOrignal(json_filename, values)
                json_primary_key = getprimarykeyforjsonfile(json_filepath,element_name)
                joining_df = GetJSONDataAsDataFrame(json_filepath, values,json_primary_key)
            elif element_type == 'XML':
                values = XMLCustomToOrignal(xml_filename, values)
                joining_df = GetXMLDataAsDataFrame(xml_filepath, values)

            # print(joining_df)
            # change to table_joining_keys_orignal
            joining_from_columns = table_joining_keys_orignal[table_name]
            # change to table_join_conditions_orignal
            joining_to_columns = table_join_conditions_orignal[table_name]['join_columns']

            # print("Dislaying data types of from : \n")
            # for col in joining_from_columns:
            #     print(col, " : " , final_df[col].dtype)
            #
            # print("Dislaying data types of to : \n")
            # for col in joining_to_columns:
            #     print(col, " : ", joining_df[col].dtype)

            print(table_name, " left : ", joining_from_columns, f" {len(joining_from_columns)}")
            print(table_join_conditions[table_name]['join_table'], " right : ", joining_to_columns,
                  f" {len(joining_to_columns)}")

            for index in range(len(joining_to_columns)):
                joining_df[joining_to_columns[index]] = joining_df[joining_to_columns[index]].astype(
                    final_df[joining_from_columns[index]].dtype)

            # logic for preventing any wrong comparisons due to renaming using _delme

            # values list elements elements occuring in from df will get renamed hence un-renamed already existing may get
            # compared in the next join

            common_from_to = list(set(values) & set(joining_from_columns_test))

            # hence renaming in the table_joining_keys for ensuring correct join next time too

            for common_item in common_from_to:
                # change : table_joining_keys >> table_joining_keys_orignal, table_joining_conditions_orignal, table_column >> table_column_orignal
                if common_item in table_joining_keys_orignal[joining_to] and common_item not in \
                        table_join_conditions_orignal[table_name]['join_columns']:
                    i = table_joining_keys_orignal[joining_to].index(common_item)
                    table_joining_keys_orignal[joining_to][i] = common_item + '_delme'
                if common_item in table_column_orignal[joining_to] and common_item not in \
                        table_join_conditions_orignal[table_name]['join_columns']:
                    i = table_column_orignal[joining_to].index(common_item)
                    table_column_orignal[joining_to][i] = common_item + '_delme'

            print("changed in joinnig_to whihch will be next joining from  : ", table_joining_keys_orignal[joining_to])
            final_df = final_df.merge(
                joining_df,
                left_on=joining_from_columns,
                right_on=joining_to_columns,
                how='inner',
                suffixes=('', '_delme')
            )
            # this is representing the custom names
            columns_required = columns_required + table_column_orignal[joining_to]
            columns_required_show = columns_required_show + table_column[joining_to]

        print("Complete data Frame : \n", final_df)
        print("Trimmed data Frame : \n", final_df[columns_required])
        # final_df[columns_required].to_csv("C:\\Users\\Dell\\Desktop\\generated_test_data\\data.csv", header=True, index=False)

        # final_df[columns_required] :  all the data
        # columns_required : required_columns
        dataframe_dict_list = final_df[columns_required].to_dict(orient='records')
        print(columns_required)
        # print(dataframe_dict_list)
        row_list = []
        for row in dataframe_dict_list:
            row_elem = []
            for col in columns_required:
                row_elem.append(row[col])
            row_list.append(row_elem)

        # print(row_list)
        context = {
            'rows': row_list,
            'actual_columns': columns_required,
            'columns': columns_required_show
        }

        return render(request, 'combined_result.html', context)


def redis_sql(data):
    try:
        r = redis.Redis(host='localhost', port=6379,
                        decode_responses=True)
        # print(r)
        print('connected to redis...')
        for row in data:
            value = row[0:2] + row[3:]
            r.hset(name='name', key=str(row[2]), value=str(value), mapping=None, items=None)
            # print(row)
        print('Cached data to redis...')
    except Exception as e:
        print('something went wrong!', e)
