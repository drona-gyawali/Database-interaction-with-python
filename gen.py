import cryptography
import os
import pandas as pd
from sqlalchemy import create_engine

class filetransfer:
    #File location
    def __init__(self,filepath):
        self.filepath=filepath
        self.connection= None

    #Connection establishing
    def __enter__(self):
            try:
                conn_string =f"mysql+pymysql://root:xxxxxxx@localhost:12345/{self.filepath}"
                real_conn = create_engine(conn_string)

                #Test the condition
                self.connection=real_conn.connect()
                print('Connection Established')
                return self
            except Exception as e:
                print('Connection lost:', e)
                
        
    def __exit__(self,exc_type,exc_val,exc_tb):
        # Closing the connection
        if self.connection:
             self.connection.close()
             print('Conenction closed')
        return False
    
    def file_generator(self,folderpath):
         for filename in os.listdir(folderpath):
              if filename.endswith('.csv'):
                   yield os.path.join(folderpath,filename)
    
    def data_transfer(self, folderpath):
        try:
            for file_path in self.file_generator(folderpath):
                print(f'Processing File {file_path}')
                df = pd.read_csv(file_path)
                table_name = os.path.splitext(os.path.basename(file_path))[0]
                df.to_sql(name=table_name, con=self.connection, if_exists='append', index=False)
                print(f'File {os.path.basename(file_path)} transferred to Table {table_name} finished')
        except Exception as e:
            print(f'Error during file transfer: {e}')
         
folder_path= r'C:\Users\DORNA\Downloads\Test'    

with filetransfer('leetcode') as file:
     file.data_transfer(folder_path)
     



    





            
    



