import sqlparse
from os import system
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Wildcard
import csv

class DBEngine():
    
    mark = False
    
    def getType(self, parsed):
        '''
        Takes the sql statement, parses it and returns the type of sql statement 
        '''
        if sqlparse.sql.Statement(parsed).get_type().encode('ascii', 'ignore') == 'SELECT':
            print 'Given query is SELECT statement'
            self.mark = True
            return 
        else:
            exit(0)
    
    def getCols(self, parsed):
        '''
        Takes the sql statement and returns columns in it.
        '''
        temp = []
        for each in parsed:
            if self.mark == True and isinstance(each, IdentifierList):
                for here in each.get_identifiers():
                    temp.append(here.get_name().encode('ascii', 'ignore'))
            if self.mark == True and isinstance(each, Identifier):
                temp.append(each.get_name().encode('ascii', 'ignore'))
            if each.ttype is Wildcard:
                temp.append(each.value.encode('ascii', 'ignore'))
            if each.ttype is Keyword and each.value.upper() == 'FROM':
                self.mark = False
        return temp
        
    def is_subselect(self, parsed):
        if not parsed.is_group():
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False
    
    def extract_from_part(self, parsed):
        from_seen = False
        for item in parsed:
            if from_seen:
                if self.is_subselect(item):
                    for x in self.extract_from_part(item):
                        yield x
                elif item.ttype is Keyword:
                    raise StopIteration
                else:
                    yield item
            elif item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True
    
    
    def extract_table_identifiers(self, token_stream):
        for item in token_stream:
            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    yield identifier.get_name()
            elif isinstance(item, Identifier):
                yield item.get_name()
            elif item.ttype is Keyword:
                yield item.value
    
    
    def extract_tables(self, parsed):
        '''
        Takes the sql statement and returns the tables name in it.
        '''
        stream = self.extract_from_part(parsed)
        return list(self.extract_table_identifiers(stream))
    
    def readMetaData(self):
        '''
        Reads the metadata file
        '''
        begin = False
        table_name = []
        attribs = []
        f = open('metadata.txt')
        for each in f.readlines():
            if 'begin_table' in each:
                begin = True
                temp = []
            elif 'end_table' in each:
                begin = False
                attribs.append(temp)
            elif begin == True and 'table' in each:
                table_name.append(each[:-2])
            elif begin == True and 'table' not in each:
                temp.append(each[:-2])
        return table_name, attribs
            
    def getFunction(self, parsed):
        '''
        returns None if no agg. functions are present
        else returns the function itself
        '''
        func = ''
        temp = []
        for each in parsed:
            if isinstance(each, sqlparse.sql.Function):
                func += each.get_name().encode('ascii', 'ignore')
                for here in each.get_parameters():
                    temp.append(here.get_name().encode('ascii', 'ignore'))
        return func, temp
    
    def getMax(self):
        temp = self.result
        temp.sort()
        return temp[-1]
    
    def getMin(self):
        temp = self.result
        temp.sort()
        print temp
        return temp[0]
    
    def getAvg(self):
        sum = 0
        for each in self.result:
            sum += each
        return (sum / len(self.result))
    
    def getSum(self):
        sum = 0
        for each in self.result:
            sum += each
        return sum
        
    def executeQuery(self, tables, cols=None, function='', parameters=None):
        '''
        Executes given query and prints the result
        Check : WHERE CLAUSE
        '''
        table_list = {each:open(each + '.csv') for each in tables}
        count = 0
        for each in table_list.keys():
            self.result = []
            cdr = csv.DictReader(table_list[each])
                
            if cols is not None or parameters is not None:
                # Normal Query
                cols = parameters
                try:
                    if len(cols) == 1:
                        if cols[0] == '*':
                            temp = self.meta_attrbs[self.meta_tables.index(each)]
                            for i in cdr:
                                self.result.append([int(i[j]) for j in temp])
                        else:
                            for here in cdr:
                                self.result.append(int(here[cols[0]]))
                    else:
                        for i in cdr:
                            self.result.append([int(i[j]) for j in cols])
                            
                    if function == '':
                        print [x for x in self.result]
                    else:
                        if function == 'max':
                            print self.getMax()
                        elif function == 'min':
                            print self.getMin()
                        elif function == 'sum':
                            print self.getSum()
                        elif function == 'avg':
                            print self.getAvg()
                except Exception:
                    print 'Table doesnt contain specified attrbs or error in schema'
            else:
                print "Error"
                return
            count += 1
    
    def prepareFile(self, tables):
        '''
        prepares the file acc. to format mentioned
        '''
        for each in tables:
            f = open(each + '.csv')
            f1 = open(each + '_.csv', 'w')
            print >> f1, ','.join(self.meta_attrbs[self.meta_tables.index(each)])
            for here in f.readlines():
                print >> f1, here
            f1.close()
            f.close()
            system('mv %s %s' % (each + '_.csv', each + '.csv'))

    def main(self):
        self.meta_tables, self.meta_attrbs = self.readMetaData()
        s = str(raw_input())
        # Tokenize the sql statement by parsing it
        parsed = sqlparse.parse(s)[0].tokens
        self.getType(parsed)
        cols = self.getCols(parsed)
        tables = self.extract_tables(parsed)
        function, parameters = self.getFunction(parsed)
        
        self.prepareFile(tables)
        
        if len(cols) == 0:
            self.executeQuery(tables, None, function, parameters)
        elif len(parameters) == 0:
            self.executeQuery(tables, cols, function)
        else:
            self.executeQuery(tables, cols, function, parameters)
        
if __name__ == '__main__':
    db = DBEngine()
    db.main()
