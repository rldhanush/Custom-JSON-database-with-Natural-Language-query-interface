from typing import Optional
import cmd2
import pprint
import subprocess, os

from cmd2.parsing import Statement
from src.data_storage.data_manager import *
from src.query_processing.query_parser import parse_query
from src.query_processing.query_executor import execute_query


#source venv/bin/activate
#./start_db.sh

class JSONDatabaseShell(cmd2.Cmd):
    prompt = '>> '
    intro = "\nWelcome to the Natural Language JSON Database\n" \
            "----------------------------------------------\n\n" \
            "Entering interactive mode. Type 'help' to know what I can do. Type 'quit' to exit.\n"

    current_database = None 

    def do_use_document(self, arg):
        """Select a database to use."""
        # Implement functionality here
        if not arg:
            self.poutput('Error: A document name must be provided.\n')
            return

        # Additional check: ensure that the specified database exists
        if arg not in list_databases():
            self.poutput(f"Error: No such documents found: '{arg}'.\n")
            return
        
        # Update the current database
        self.current_database = arg
        self.poutput(f"Processing on '{arg}' document.\n")

    def do_stop_using_document(self, arg):
        """Stop using the current database."""
        if self.current_database is None:
            self.poutput("No document is currently in use.\n")
            return
        
        self.current_database = None
        self.poutput("Stopped using the document.\n")

    def do_show_documents(self, arg):
        """Show all databases."""
        databases = list_databases()
        if databases != []:
            self.poutput("Available Databases:\n")
            for db in databases:
                self.poutput(f"- {db}")
        else:
            print("No documents found.\n")

    def do_create_document(self, arg):
        """Create a new database."""
        if not arg:
            self.poutput('Error: A document name must be provided.\n')
        else:
            # Call your create_database function
            try:
                create_database(arg)
            except Exception as e:
                self.poutput(str(e))

    def do_quit(self, arg):
        """Quit the CLI."""
        self.poutput("Goodbye!\n")
        return True
    
    def do_help(self,arg):
        """Show help"""
        self.poutput("Available commands:\n")
        self.poutput("- show_documents")
        self.poutput("- create_document [name]")
        self.poutput("- use_document [name]")
        self.poutput("- delete_document [name]")
        self.poutput("- stop_using_document")
        self.poutput("- execute_query")
        self.poutput("- clear")
        self.poutput("- quit\n")

    def do_clear(self, arg):
        """Clear the console."""
        try:
            subprocess.run('cls' if os.name == 'nt' else 'clear', shell=True, check=True)
            print(">> Entering interactive mode. Type 'help' to know what I can do. Type 'quit' to exit.")
        except subprocess.CalledProcessError as e:
            self.poutput(f"Failed to clear console: {str(e)}")

    def do_delete_document(self, arg):
        """
        Delete a database.
        Usage: delete_database [name]
        """
        if not arg:
            print("Please specify a document name.\n")
            return
        
        try:
            delete_database(arg)
        except Exception as e:
            print(str(e))

    def do_execute_query(self, arg):
        """Clear the screen and wait for user query input."""
        if not self.current_database:
            self.poutput("No document selected. Please use 'use_document' to select a document.\n")
            return
        
        query = input("Please enter your query: ")
        
        try:
            # Parse the natural language query to a structured format.
            parsed_query = parse_query(query)
            # Execute the parsed query using the query_executor.
            execute_query(parsed_query, self.current_database)

        except Exception as e:
            traceback.print_exc()
            self.poutput(f"An error occurred while executing the query: {str(e)}\n")

    def default(self, line):
        print(f"Command not recognized: {line}\n Type 'help' to see the available commands.")
    
if __name__ == '__main__':
    shell = JSONDatabaseShell()
    shell.cmdloop()
