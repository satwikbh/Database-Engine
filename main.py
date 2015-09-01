import csv
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, Wildcard

class Phase1:
    
    def is_subselect(self, parsed):
        if not parsed.is_group():
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False
    
    def extract_from_part(self, parsed):
        from_seen = False
        for item in parsed.tokens:
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
                yield item.get_name().encode('ascii', 'ignore')
            elif item.ttype is Keyword:
                yield item.value
    
    def extract_tables(self, s):
        stream = self.extract_from_part(sqlparse.parse(s)[0])
        return list(self.extract_table_identifiers(stream))
        
    def parseSQL(self, s):
        '''
        Parses an sql statement and checks its correctness
        '''
        for item in sqlparse.parse(s)[0].tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
            else:
                return False
    
    def openCSV(self, tableNames):
        '''
        To read the csv file and print its contents
        '''
        for table in tableNames:
            with open(table + '.csv') as f:
                reader = csv.reader(f)
                for each in reader:
                    print each
    
    def getColumns(self,s):
        '''
        Get the columns from the table
        '''
        mark = False
        for item in sqlparse.parse(s)[0].tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                mark = True
            if mark == True:
                if item.ttype is Wildcard and item.value.upper()=='*':
                    return 0
        return 1
    
    def projColumns(self,l,s):
        '''
        To perform operations on columns such as
        select and project 
        '''
        sqlparse.sql.IdentifierList.get_identifiers(sqlparse.parse(s)[0].tokens[2])
    
    def main(self):
        # print 'Enter the sql statement : '
        # s = str(raw_input())
        s = 'select col1,col2 from table1;'
        if not self.parseSQL(s):
            print 'The given query is not a SELECT query'
            exit(0)
        l = self.extract_tables(s)
        self.projColumns(l, s)
        '''
        if not self.getColumns(s):
            self.openCSV(l)
        else:
            self.projColumns(l,s)
        '''
        
if __name__ == "__main__":
    phase1 = Phase1()
    phase1.main()
