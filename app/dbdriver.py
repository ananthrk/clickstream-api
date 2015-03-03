# dbdriver.py
import sqlite3
import sys
import os

ERR_MSG = 'ERROR : %s \nLINE NUMBER : %s'

class DB:    
    def __init__(self, db_file=None):
        try:
            if not db_file:
                self.DATABASE = os.path.abspath(os.path.join(os.path.dirname( __file__ ), 'db/pageview_stats.db'))
            else:
                self.DATABASE = db_file
            #print self.DATABASE
            self.conn = sqlite3.connect(self.DATABASE)
            self.conn.text_factory = str
            self.cursor = self.conn.cursor()
        except:
            error_msg = ERR_MSG % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno)
            print error_msg
        
    def close_connection(self):
        # close cursor
        self.cursor.close()
        # close connection
        self.conn.close()
    
    def fetchall(self, sql):
        results = []        
        try:
            # Execute the SQL command
            self.cursor.execute(sql)
            # Fetch all the rows in a list of lists.
            results = self.cursor.fetchall()
        except:
            error_msg = ERR_MSG % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno)
            print error_msg
        finally:
            #close connection
            self.close_connection()
        
        return results
    
    def execute(self, sql):
        try:            
            # Execute the SQL command
            self.cursor.execute(sql)
            # Commit all the rows
            self.conn.commit()
        except:            
            error_msg = ERR_MSG % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno)
            print error_msg
            # Rollback in case there is any error
            self.conn.rollback()
        finally:
            #close connection
            self.close_connection()

    def execute_many(self, sql, values):
        try:            
            # Execute the SQL command
            self.cursor.executemany(sql, values)
            # Commit all the rows
            self.conn.commit()
        except:            
            error_msg = ERR_MSG % (sys.exc_info()[1], sys.exc_info()[2].tb_lineno)
            print error_msg
            # Rollback in case there is any error
            self.conn.rollback()
        finally:
            #close connection
            self.close_connection()