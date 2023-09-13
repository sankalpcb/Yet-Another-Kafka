# class _Getch:
#     """Gets a single character from standard input.  Does not echo to the screen."""
#     def __init__(self):
#         self.impl = _GetchWindows()
    
#     def __call__(self): 
#         return self.impl()

# class _GetchWindows:
#     def __init__(self):
#         import msvcrt

#     def __call__(self):
#         import msvcrt
#         return msvcrt.getch()

# getch = _Getch()


import os,shutil
PWD=os.getcwd()
leader=1
brokers=[1,2,3]
brokers.remove(leader)

source_folder = PWD+'/broker'+ str(leader)
destination_folder1 = PWD+'/broker'+ str(brokers[0])
destination_folder2 = PWD+'/broker'+str(brokers[1])


shutil.rmtree(destination_folder2)
shutil.copytree(source_folder, destination_folder2)