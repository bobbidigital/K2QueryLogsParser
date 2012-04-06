__author__ = 'Jeffery.Smith'
from lxml import etree
import re

def propertycheck(property):
    """ Decorator method to if the K2LogEntry object has the required property

        The K2LogEntry object is designed to represent all of the various K2 Log entries. Instead of subclassing a base
        class, I decided to put all of the necessary properties into a single object. This decorator simply checks to
        ensure that the object actually has the property being requested. If not, raise an exception. This lets us
        eliminate the need for subclasses, but still protect the object from having values modified.
    """
    def _decorator(fn):
        def wrapped(*args):
            try:
                return fn(*args)
            except AttributeError:
                raise AttributeError("'%s' object has no attribute '%s'" % (args[0].__class__.__name__, property))
        return wrapped
    return _decorator

class K2DocPaths(object):
    """ Object used to provide field information for the K2Logs

        This object provides XPATH lookup variables as well as other log entry structure specific data. We also
        house the fields that are required in this object. They probably should live somewhere else, but that's for
        another time and refactoring effort.
    """
    def log_fields(self,logtype):
        """ Provides the fields that log entries have.

            This provides all of the fields for a log entry that need to be processed.
            NOTE: This needs to be moved into the K2LogEntry object. Remove the logtype parameter and retrieve that
            value from the variable self.name
        """
        if logtype == "K2AssistSuggest":
            return 'client', 'time', 'suggest_time', 'query','query_suggestion', 'collections'

        if logtype == "K2DocStream":
            return 'time','dockey','query','client', 'fields'

        if logtype == "K2CollSearch":
            return 'time','search_time','service_search_time','hit_num','total_docs', \
                    'source_query', 'query','query_parser','client', 'collections','fields', 'from_cache'

        if logtype == "Collection":
            return  'collection_server_alias', 'collection_hit_num', 'collection_num_proc', \
                     'collection_service_search_time', 'collection_kernel_search_time'

    @property
    def time(self):
        return "//Time"

    @property
    def dockey(self):
        return "//K2DocKey"

    @property
    def query(self):
        return "//Query"

    @property
    def client(self):
        return "//Client"
    @property
    def fields(self):
        return "//Field"

    @property
    def collection_server_alias(self):
        return "@serverAlias"

    @property
    def collection_hit_num(self):
        return "@hitNum"

    @property
    def collection_num_proc(self):
        return "@numProc"

    @property
    def collection_service_search_time(self):
        return "@serviceSearchTime"

    @property
    def collection_kernel_search_time(self):
        return "@kernelSearchTime"

    @property
    def search_time(self):
        return "//SearchTime"

    @property
    def service_search_time(self):
        return "//ServiceSearchTime"

    @property
    def hit_num(self):
        return "//HitNum"

    @property
    def source_query(self):
        return "//SourceQuery"

    @property
    def query_parser(self):
        return "//Query/@queryParser"

    @property
    def collections(self):
        return "//Collection"

    @property
    def collection_queue_length(self):
        return "@queueLength"

    @property
    def suggest_time(self):
        return "//SuggestTime"

    @property
    def query_suggestion(self):
        return "//QuerySuggestion"

    @property
    def total_docs(self):
        return "//TotalDocs"

    @property
    def from_cache(self):
        return "@fromCache"

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
        xpathValues = K2DocPaths()
        properties = xpathValues.log_fields(root_element.tag)
        self.log_type = root_element.tag
        self._build_properties(properties, root_element)

    def _build_properties(self,properties,root_element):
        """ Dynamically builds the properties for the object

            This method is used to build the appropriate properties for a given log entry type. Prevents me from needing
            to subclass for each log entry type. This sets the values based on a naming convention that must be adhered
            to. Each property has a name "<property>". That property name is associated with an internal variable named
            "_<property>". There should also be a corresponding property in the K2DOCPATH object named "<property>".
             By sticking to this convention, we can build properties dynamically and access the correct XPATH without
             knowing what type of log entry is being passed.

             properties -- An iterable object whose values are strings that correspond to property names.

             root_element -- An etree XML object that contains the log entry. This will be used to populate the property
              values
        """
        xpathValues = K2DocPaths()
        for property in properties:
            value =  root_element.xpath(getattr(xpathValues,property))
            property = "_%s" % property
            if value:
                #Check to see if our property is a single value or multiple
                if len(value) == 1:
                    #Some values are straight text, others are more element objects. We just want the text
                    try:
                        value = value[0].text.strip()
                    except AttributeError:
                        value = value[0].strip()
                    except IndexError:
                        pass
                        value = None
                else:
                    #Our property has multiple entries. Build a list to contain them all
                    value_list = []
                    for index_value in value:
                        #Check to see if index_value has attributes
                        if len(index_value.items()):
                            attributes = {}
                            for attribute in index_value.items():
                                attributes[attribute[0]] = attribute[1]
                            attributes['name'] = index_value.text.strip()
                            value_list.append(attributes)
                        else:
                            index_value = index_value.text.strip()
                            if index_value:
                                value_list.append(index_value)
                    value = value_list
            else:
                value = None
            setattr(self,property,value)
    @property
    @propertycheck("fields")
    def fields(self):
        return self._fields

    @property
    @propertycheck("time")
    def time(self):
        #Getting rid of the GMT. It's assumed that all times are in GMT. Makes it less of a hassle to deal with when
        #importing into various databases
        t = self._time.replace("GMT", "")
        return t.strip()

    @property
    @propertycheck("dockey")
    def dockey(self):
        return self._dockey

    @property
    @propertycheck("query")
    def query(self):
        return self._query

    @property
    @propertycheck("client")
    def client(self):
        return self._client

    @property
    @propertycheck("query")
    def query(self):
        return self._query


    @property
    @propertycheck("server_alias")
    def server_alias(self):
        return self._server_alias

    @property
    @propertycheck("collection_server_alias")
    def collection_server_alias(self):
        return self._collection_server_alias
    @property
    @propertycheck("collection_hit_num")
    def collection_hit_num(self):
        return self._collection_hit_num

    @property
    @propertycheck("collection_num_proc")
    def collection_num_proc(self):
        return self._collection_num_proc

    @property
    @propertycheck("collection_service_search_time")
    def collection_service_search_time(self):
        return self._collection_service_search_time

    @property
    @propertycheck("collection_kernel_search_time")
    def collection_kernel_search_time(self):
        return self._collection_kernel_search_time

    @property
    @propertycheck("hit_num")
    def hit_num(self):
        return self._hit_num

    @property
    @propertycheck("num_proc")
    def num_proc(self):
        return self._num_proc

    @property
    @propertycheck("service_search_time")
    def service_search_time(self):
        return self._service_search_time

    @property
    @propertycheck("kernel_search_time")
    def kernel_search_time(self):
        return self._kernel_search_time

    @property
    @propertycheck("collection")
    def collection(self):
        return self._collection

    @property
    @propertycheck("collections")
    def collections(self):
        return self._collections

    @property
    @propertycheck("query_suggestion")
    def query_suggestion(self):
        if not self._query_suggestion:
            return ""
        else:
            return self._query_suggestion

    @property
    @propertycheck("suggest_time")
    def suggest_time(self):
        return self._suggest_time

    @property
    @propertycheck("from_cache")
    def from_cache(self):
        return self._from_cache

    @property
    @propertycheck("search_time")
    def search_time(self):
        return self._search_time

    @property
    @propertycheck("total_docs")
    def total_docs(self):
        return self._total_docs

    @property
    @propertycheck("query_parser")
    def query_parser(self):
        return self._query_parser

    @property
    @propertycheck("source_query")
    def source_query(self):
        return self._source_query

class K2DataStore(object):
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

        for field in fields:
            if field_list:
                #field_list += ","
                field_list.append(",")
            #field_list += field
            field_list.append(field)
            if field_value:
                #field_value += ","
                field_value.append(",")
            #We may have been pase
            try:
                #Converting to UTF-8 to handle some special characters that are port of some queries
                value = fields[field].encode('utf_8')
            except TypeError:
                value = field.encode('utf_8')
            try:
                if value[0] == '@':
                    #field_value += value.replace("'", "''")
                    field_value.append(value.replace("'", "''"))
                else:
                    #field_value += """'%s'""" % value.replace("'", "''")
                    field_value.append("""'%s'""" % value.replace("'", "''"))
            except IndexError:
                #field_value += """'%s'""" % value.replace("'", "''")
                field_value.append("""'%s'""" % value.replace("'", "''"))

        sql = " INSERT INTO %s (%s) VALUES (%s)" % (table_name, ''.join(field_list), ''.join(field_value))
        if save_identity:
            sql += " select @id = SCOPE_IDENTITY() "
        return sql


    def _insert_k2assist(self,k2assist):
        """ Insert K2Assist Element entries from the log

            k2assist -- A K2LogEntry object that contains a K2Assist log entry.
            NOTE: DRY. We repeat this pattern for each of the assist types, but they largely follow the same pattern.
                Need to find an approach to handle this so we're not repeating code.
        """

        sql_fields = { 'client' : k2assist.client, 'time' : k2assist.time, 'suggest_time' : k2assist.suggest_time,
                        'query' : k2assist.query }

        #There's a lot of potential concatenation going on. We'll build our string as a list and the convert at the end
        sql = []
        #sql = self._build_insert_sql(fields=sql_fields, table_name='K2AssistSuggest', save_identity=True)
        sql.append(self._build_insert_sql(fields=sql_fields, table_name='K2AssistSuggest', save_identity=True))
        for collection in k2assist.collections:
            collection_fields = { 'name' : collection, 'k2assistsuggest_id' : '@id' }
            #sql += self._build_insert_sql(fields=collection_fields, table_name='K2AssistCollections')
            sql.append(self._build_insert_sql(fields=collection_fields, table_name='K2AssistCollections'))
            collection_fields = None

        for suggestion in k2assist.query_suggestion:
            suggestion_fields = {'query_suggestion' : suggestion, 'k2assistsuggest_id' : '@id' }
            #sql += self._build_insert_sql(fields=suggestion_fields, table_name='K2AssistSuggestions')
            sql.append(self._build_insert_sql(fields=suggestion_fields, table_name='K2AssistSuggestions'))
            suggestion_fields = None

        #Use join to convert our list into a string
        self._execute_sql(''.join(sql))


    def _insert_k2docstream(self,k2docstream):
        """ Insert K2Docstream Element entries from the log

            k2assist -- A K2LogEntry object that contains a K2Docstream log entry.
            NOTE: DRY. We repeat this pattern for each of the assist types, but they largely follow the same pattern.
                Need to find an approach to handle this so we're not repeating code.
        """
        sql_fields = { 'time' : k2docstream.time, 'dockey' : k2docstream.dockey, 'query' : k2docstream.query,
                       'client' : k2docstream.client}
        sql = []
        #sql = self._build_insert_sql(fields=sql_fields, table_name='K2DocStream', save_identity=True)
        sql.append(self._build_insert_sql(fields=sql_fields, table_name='K2DocStream', save_identity=True))
        try:
            for field in k2docstream.fields:
                k2fields = { 'name' : field, 'k2docstream_id' : '@id' }
                #sql += self._build_insert_sql(fields=k2fields, table_name='K2DocStreamFields')
                sql.append(self._build_insert_sql(fields=k2fields, table_name='K2DocStreamFields'))
                k2fields = None
        except TypeError:
            pass
        #self._execute_sql(sql)
        self._execute_sql(''.join(sql))

    def _insert_k2collsearch(self,k2collsearch):

        sql_fields = { 'time' : k2collsearch.time, 'from_cache' : k2collsearch.from_cache,
            'search_time' : k2collsearch.search_time, 'service_search_time' : k2collsearch.service_search_time,
            'hit_num' : k2collsearch.hit_num, 'total_docs' : k2collsearch.total_docs, 'query_parser' : k2collsearch.query_parser,
            'query' : k2collsearch.query, 'source_query' : k2collsearch.source_query, 'client' : k2collsearch.client }

        sql = []
        #sql = self._build_insert_sql(fields=sql_fields, table_name='K2CollSearch', save_identity=True)
        sql.append(self._build_insert_sql(fields=sql_fields, table_name='K2CollSearch', save_identity=True))
        try:
            for field in k2collsearch.fields:
                k2fields = None
                k2fields = {'name' : field, 'k2collsearch_id' : '@id' }
                #sql += self._build_insert_sql(fields=k2fields, table_name='CollSearchFields')
                sql.append(self._build_insert_sql(fields=k2fields, table_name='CollSearchFields'))
        except TypeError:
            pass

        try:
            for collection in k2collsearch.collections:
                k2fields = None
                #Make sure each dictionary has an object. Sometimes the XML file may not have all the fields necessary
                for x in ('hitNum', 'numProc', 'serviceSearchTime', 'kernelSearchTime'):
                    if not x in collection:
                        collection[x] = "-1"

                k2fields = { 'hit_num' : collection['hitNum'], 'num_proc' : collection['numProc'],
                             'service_search_time' : collection['serviceSearchTime'], 'kernel_search_time' :
                                collection['kernelSearchTime'], 'collection' : collection['name'],
                                'k2collsearch_id' : '@id' }
                #sql += self._build_insert_sql(fields=k2fields, table_name='CollSearchDetail')
                sql.append(self._build_insert_sql(fields=k2fields, table_name='CollSearchDetail'))
        except TypeError:
            pass

        #self._execute_sql(sql)
        self._execute_sql(''.join(sql))

    def add(self,K2LogEntry):

        if K2LogEntry.log_type == "K2AssistSuggest":
            self._insert_k2assist(K2LogEntry)

        elif K2LogEntry.log_type == "K2DocStream":
            self._insert_k2docstream(K2LogEntry)

        elif K2LogEntry.log_type == "K2CollSearch":
            self._insert_k2collsearch(K2LogEntry)
        else:
            raise TypeError("Element type %s not recognized" % K2LogEntry.log_type)


def main():
    import re
    import os
    import time
    ENTRIES_TO_LOG = ("K2AssistSuggest", "K2DocStream", "K2CollSearch")

    def parse_args():
        import argparse
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










