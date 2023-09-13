import os
PWD= os.getcwd()


def all_topic_offset():
    os.chdir(PWD+'/broker1')
    all_topics=os.listdir()
    all_topics.remove('log.txt')

    last_seek={}
    for i in all_topics:
        os.chdir(PWD+'/broker1/'+i)
        all_files=os.listdir()
        length=len(all_files)
        last_file=all_files[length-1]
        f=open(last_file,'r')
        data = f.read()
        number_of_characters = len(data)
        last_seek[i] = (last_file,number_of_characters)
    
    os.chdir(PWD)
    
    return last_seek



class Topic_cons:
    instances={}
    def __init__(self,id) -> None:
        self.id=id
        if id not in Topic_cons.instances:
            Topic_cons.instances[self.id]=all_topic_offset()
        
    
    
    def consume(self,topic_name,beg):
        os.chdir(PWD+'/broker1/'+topic_name)
        all_files=os.listdir()
        ret=''
        offset=0
        if beg!='--from-beginning':
            index = all_files.index(Topic_cons.instances[self.id][topic_name][0])
            all_files=all_files[index:]
            offset = Topic_cons.instances[self.id][topic_name][1]
        
        off_flag=0
        for i in all_files:
                
            f=open(i,'r')
            if off_flag==0: 
                f.seek(offset)
                off_flag=1
            
            ret+=f.read()
            f.close()
        f=open(i,'r')
        data=f.read()
        Topic_cons.instances[self.id][topic_name] = (i,len(data))
        f.close()
        os.chdir(PWD+'/broker1')
        f=open('log.txt','a+')
        f.write(str(Topic_cons.instances)+'\n')
        os.chdir(PWD)
        
        return ret