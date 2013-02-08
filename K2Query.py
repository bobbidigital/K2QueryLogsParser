__author__ = 'Jeffery.Smith'
from lxml import etree
import re
import types
import os
import time
import argparse

logfields = { 'K2AssistSuggest': ('client', 'time', 'suggest_time',
                        'query','query_suggestion', 'collections'),

              'K2DocStream' : ('time','dockey','query','client',
                         'fields'),

              'K2CollSearch': ('time','search_time','service_search_time','hit_num','total_docs',
                                'source_query', 'query','query_parser','client',
                                'collections','fields', 'from_cache'),

              'Collection':('collection_server_alias', 'collection_hit_num', 'collection_num_proc',
                                'collection_service_search_time', 'collection_kernel_search_time')
}

xpaths = { 'time' : '//Time', 'dockey' : '//K2DocKey', 'client' : '//Client',
        'fields' : '//Field', 'collection_server_alias' : '@serverAlias',
        'collection_hit_num' : '@hitNum', 'collection_num_proc' : '@numProc',
        'collection_service_search_time' : '@serviceSearchTime',
        'collection_kernel_search_time' : '@kernelSearchTime',
        'search_time' :'//SearchTime', 'hit_num' : '//HitNum',
        'source_query' : '//SourceQuery',
        'query_parser' : '//Query/@queryParser','collections' :'//Collection',
        'collection_queue_length' : '@queueLength',
        'suggest_time' : '//SuggestTime',
        'query_suggestion' : '//QuerySuggestion', 'total_docs': '//TotalDocs',
        'from_cache' : '@fromCache',
        'query' : '//Query', 'service_search_time' : '//ServiceSearchTime'
 }

class K2LogEntry(object):
    """ Object representation of an individual log entry

        The K2LogEntry object takes an XML entry from the K2Log and creates an object representation of it. Each field
        becomes accessible via a property accessor. Since this is data being retrieved from logs, we don't allow
        modification of these fields.
    """
    def __init__(self, root_xml):
        """ Constructor method

            root_xml -- A well formed XML element from the K2Log. It should be the root element of an individual log
            entry. The XML should be passed as a string.
        """
        root_element = etree.fromstring(root_xml)
        try:
            properties = logfields[root_element.tag]
        except:
            raise ValueError("Tag not recognized")
        self.log_type = root_element.tag
        self._build_properties(properties, root_element)

    def _build_properties(self,properties,root_element):
        """ Build the appropriate properties for the log entry:
             properties -- An iterable object whose values are strings that correspond to property names.
             root_element -- An etree XML object that contains the log entry. This will be used to populate the property
              values
        """
        for property in properties:
            value =  root_element.xpath(xpaths[property])
                #Check to see if our property is a single value or multiple
            if len(value) == 1:
                #Some values are straight text, others are more element objects. We just want the text
                try:
                    value = value[0].text.strip()
                except AttributeError:
                    value = value[0].strip()
                except IndexError:
                    value = None
            else:
                #Our property has multiple entries. Build a list to contain them all
                value_list = []
                for index_value in value:
                    if len(index_value.items()):
                        attributes = { attribute[0] :attribute[1] for
                                attribute in index_value.items() }
                        attributes['name'] = index_value.text.strip()
                        value_list.append(attributes)
                    else:
                        index_value = index_value.text.strip()
                        if index_value:
                            value_list.append(index_value)
                value = value_list
            if property == 'time':
                value = value.replace("GMT","").strip()
            setattr(self,property,value)

class K2DataStore:
    """ Abstract class for Datastore objects. These objects define persistence strategies for the log entries.

    """

    def save(self):
        """ save method should persist the data to the store. EX - write tables to database, save data to file etc.
        """
        raise NotImplemented("Subclass of K2DataStore must implement save() method")

    def add(self,K2LogEntry):
        """ add method provides data that will need to be written to the store. The add method does not need to
            immediately persist the data to the store. That action should be done by the save method.
        """
        raise NotImplemented("Subclass of K2Datastore must implement add() method")

    def close(self):
        """ close method should cleanup any connections or final writes to the store
        """
        raise NotImplemented("Subclass of K2Datastore must implement close() method")

class K2SQLStore(K2DataStore):

    def __init__(self,connection):
        self.conn = None
        self._cursor = None
        self._MAX_COMMIT_SIZE = 100
        self.current_commits = 0
        try:
            self._server = connection['server']
            self._password = connection['password']
            self._user = connection['user']
            self._database = connection['database']
            self._initial = False
            if 'initial' in connection:
                self._initial = True
        except KeyError:
            raise TypeError("K2SQLStore received a dictionary without all required values")


    def open(self):
        import pymssql
        self.conn = pymssql.connect(host=self._server, user=self._user, password=self._password,
            database=self._database)
        self.cursor = self.conn.cursor()

    def save(self):
        return
        self.conn.commit()
        self.current_commits = 0

    def close(self):
        self.conn.close()

    def _execute_sql(self,sql):
        """ Executes a SQL statement using the internal pymssql object

            sql -- the SQL statements that need to be execute. Then our method wraps that sql into a transaction and
            executes. To avoid having too many transactions committed at once, we keep track of how many record entries
            have been submitted. Once we hit our threshold, we persist to the database.
        """
        sql_list = []
        sql_list.append("BEGIN TRANSACTION DECLARE @id int")
        sql_list.append("%s COMMIT TRANSACTION" % sql)
        self.cursor.execute(''.join(sql_list))
        self.current_commits += 1
        if self.current_commits >= self._MAX_COMMIT_SIZE:
            self.save()

    def _build_insert_sql(self, fields=None, table_name=None, save_identity=False):
        """ Builds an INSERT statement based on the fields that are passed.
            Keyword Arguments:
            fields -- A dictionary object with the field names as the key and the field's value as the matching
                dict value
            table_name -- The table name the insert is being built for
            save_identity -- Determines whether the INSERT statement should also save the new record's Primary Key value
        """
        field_list = []
        field_value = []

        for field, value in fields.iteritems():

            if not isinstance(value, basestring):
                continue
            if field_list: field_list.append(",")
            field_list.append(field)
            if field_value: field_value.append(",")
            try:
                #Converting to UTF-8 to handle some special characters that are part of some queries
                value = value.encode('utf_8')
            except TypeError:
                value = field.encode('utf_8')
            try:
                if value[0] == '@':
                    field_value.append(value.replace("'", "''"))
                else:
                    field_value.append("""'%s'""" % value.replace("'", "''"))
            except IndexError:
                field_value.append("""'%s'""" % value.replace("'", "''"))

        sql = " INSERT INTO %s (%s) VALUES (%s)" % (table_name, ''.join(field_list), ''.join(field_value))
        if save_identity:
            sql += " select @id = SCOPE_IDENTITY() "
        return sql


    def add(self,k2log):

        object_fields = { 'K2AssistSuggest' :
        [('collections','K2AssistCollections','k2assistsuggest_id'),
            ('query_suggestion', 'K2AssistSuggestions', 'k2assistsuggestion_id')],
        'K2DocStream': [('fields', 'K2DocStreamFields','k2docstream_id')],
        'K2CollSearch' : [('fields','CollSearchFields','k2collsearch_id'),
            ('collection', 'CollSearchDetail','k2collsearch_id')] }

        fields_to_parse = logfields[k2log.log_type]
        sql_fields = {field : getattr(k2log,field) for field in fields_to_parse }
        sql = []
        sql.append(self._build_insert_sql(fields=sql_fields,table_name=k2log.log_type,save_identity=True))

        related_data = object_fields[k2log.log_type]
        for relation in related_data:
            if hasattr(k2log,relation[0]):
                for data in getattr(k2log,relation[0]):
                    if isinstance(data, types.DictType):
                        fields = { key: data.get(key,-1) for key in fields_to_parse }
                    else:
                        fields = { 'name' : data, relation[2]: '@id' }
                    sql.append(self._build_insert_sql(fields=fields,table_name=relation[1]))
            self._execute_sql(''.join(sql))

def main():
    ENTRIES_TO_LOG = ("K2AssistSuggest", "K2DocStream", "K2CollSearch")

    def parse_args():
        parser = argparse.ArgumentParser(description='Processes querylogs from the Verity K2 Search Platforms ' \
                                                     'and save them to CSV, SQLITE or SQL Server stores')
        parser.add_argument('--logs',help='The directory that holds the logfiles you wish to process',
            required=True, action='store', metavar='<log file path>')
        parser.add_argument('--server', help='SQL Server Name', required=True, action='store',
            metavar='<sql server name>')
        parser.add_argument('--database', help='Database name', required=True, action='store',
            metavar='<database name>')
        parser.add_argument('--user', help='SQL user name', required=True, action='store',
            metavar='<sql username>')
        parser.add_argument('--password', help='SQL users password', required=True,action='store',
            metavar='<sql password>')

        return parser.parse_args()

    args = parse_args()
    connection = { 'server' : args.server, 'database' : args.database, 'user' : args.user, 'password' : args.password }
    datastore = K2SQLStore(connection)
    datastore.open()
    directory = os.listdir(args.logs)
    for file in directory:
        try:
            if not re.match('.+\.xml$',file):
                continue
            start_time = time.time()
            print "\nProcessing file %s.............." % file
            f = open("%s/%s" % (args.logs, file))
            context = etree.iterparse(f, events=("end",))
            for action, elem in context:
                if elem.tag in ENTRIES_TO_LOG:
                    K2Object = K2LogEntry(etree.tostring(elem))
                    datastore.add(K2Object)
                    datastore.save()
            elapsed_time = time.time() - start_time
            print "Elapsed time is %s" % elapsed_time
            f.close()
            f = None
        except Exception as ex:
            print "Error processing %s \n %s" % (file,ex.message)
    datastore.close()

if __name__ == "__main__":
    main()

