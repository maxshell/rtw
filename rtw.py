import os
import sys
import shutil
import sqlite3
import hashlib

class DbManager():
  __connection = None
  __cursor = None

  def __init__(self, dbpath):
    self.__connection = sqlite3.connect(dbpath)
    self.__cursor = self.__connection.cursor()

  def execute(self, query, params=[]):
    self.__cursor.execute(query, params)
    result = self.__cursor.fetchall()
    self.__connection.commit()
    return result
 
  def close(self):
    self.__del__()

  def __del__(self):
    self.__connection.close()

class RepoManager():
  __reponame = ".rtw"
  __repopath = os.path.join(os.getcwd(), __reponame)
  __revisionsname = "revisions"
  __revisionspath = os.path.join(__repopath, __revisionsname)
  __dbname = "db"
  __dbpath = os.path.join(__repopath, __dbname)
  __dbman = None

  def __init__(self):
    if os.path.exists(self.__dbpath):
      self.__dbman = DbManager(self.__dbpath)

  def close(self):
    self.__del__()

  def __del__(self):
    if self.__dbman!=None:
      self.__dbman.close()  

  def __validate(self, stopexecution=True):
    result = (os.path.exists(self.__repopath)
            and os.path.exists(self.__revisionspath) 
            and os.path.exists(self.__dbpath))
    if not result:
      if stopexecution:
        print("Repository doesn't exist.")
        self.close()
        sys.exit(2)
    else:
      return result
  
  def __gethead(self):
    if self.__validate():
      query = "select revision, branch from head"
      (revision, branch) = self.__dbman.execute(query)[0]
      return (revision, branch)
    
  def __getstaging(self):
    if self.__validate():
      query = "select file from staging"
      result = self.__dbman.execute(query)
      return [wrappedfilename[0] for wrappedfilename in result]
  
  def __innerstatus(self):
    if self.__validate():
      filestocommit = []
      missedfiles=[]
      (revisiontocommit, branch) = self.__gethead()
      stagedfiles = self.__getstaging()
      maxrevision = self.__dbman.execute("select max(revision) from revisions where branch=?", (branch,))
      maxrevision = maxrevision[0][0]
      if len(stagedfiles)>0:
        if maxrevision!=None:  
          maxrevisionpath = os.path.join(self.__revisionspath, str(maxrevision))
          if os.path.exists(maxrevisionpath):
            for stagedfile in stagedfiles: 
              wtfilename = os.path.join(os.getcwd(), stagedfile)
              repofilename = os.path.join(maxrevisionpath, stagedfile)
              if os.path.exists(wtfilename):
                wtfile = open(wtfilename, 'rb')
                wthash = self.__getMD5(wtfile)
                wtfile.close()
                if os.path.exists(repofilename):
                  repofile = open(repofilename, 'rb')
                  repohash = self.__getMD5(repofile)
                  repofile.close()
                  if wthash != repohash:
                    filestocommit.append(stagedfile)
                else:
                  filestocommit.append(stagedfile)           
              else:
                missedfiles.append(stagedfile)
        else:
          filestocommit=stagedfiles   
      return filestocommit, missedfiles
  
  def __getMD5(self, fileh):
    md5 = hashlib.md5()
    while True:
      data = fileh.read(128)
      if not data:
        break
      md5.update(data)
    return md5.digest()
      
  def initialize(self):
    if not self.__validate(False):
      try:
        os.makedirs(self.__revisionspath)
        queries = []
        queries.append("create table staging (file text PRIMARY KEY)")
        queries.append("create table branches (branch varchar(15) PRIMARY KEY)")
        queries.append("create table head (revision integer PRIMARY KEY, \
                        branch varchar(15) not null, FOREIGN KEY(branch) REFERENCES branches(branch))")
        queries.append("create table revisions (revision integer PRIMARY KEY, \
                        branch varchar(15) not null, comment text, \
                        FOREIGN KEY(branch) REFERENCES branches(branch))")
        queries.append("create table staginghistory (revision integer not null, \
                        file text not null, \
                        PRIMARY KEY(revision, file), \
                        FOREIGN KEY(revision) REFERENCES revisions(revision))")
        queries.append("insert into branches values('master')")
        queries.append("insert into head values(1, 'master')")
        self.__dbman = DbManager(self.__dbpath)
        for query in queries:
          self.__dbman.execute(query)
        print("Repository is initialized.")
      except BaseException:
        print("Cannot initialize repository.")
        if os.path.exists(self.__repopath):
          shutil.rmtree(self.__repopath)    
    else:
      print("Rejected. Repository exists.")
   
  def add(self, files):
    if self.__validate():
      if len(files)>0:
        for filen in files:    
          path = os.path.abspath(filen)
          if os.path.exists(path):
            relpath = os.path.relpath(path, os.getcwd())
            query = "insert into staging values(?)"
            try:
              self.__dbman.execute(query, (relpath,))
              print("File {0} is added to the staging area.".format(filen)) 
            except sqlite3.IntegrityError:
              print("File {0} exists in staging area.".format(filen))
          else:
            print("File {0} is not exist and could not be added to the staging area.".format(filen))
      self.showstaging()
  
  def rm(self, files):
    if self.__validate():
      if len(files)>0:
        for filen in files:    
          path=os.path.abspath(filen)
          relpath=os.path.relpath(path, os.getcwd())
          query = "delete from staging where file=?"
          self.__dbman.execute(query, (relpath,))
          print("File {0} doesn't exist in staging area for now.".format(filen))
          if os.path.exists(path):
            os.remove(path)
            print("File {0} is removed from the file system.".format(filen)) 
  
  def mv(self, src, dst):
    if self.__validate():
      os.rename(src, dst)
      self.add((dst,))
      self.rm((src,))  

  def current(self):
    if self.__validate():
      (revision, branch) = self.__gethead()
      print("Current branch:     {0}".format(branch))
      print("Revision to commit: {0}".format(revision))                                     
      query = "select revision, comment from revisions where branch=?"
      branchrevisions = self.__dbman.execute(query, (branch,))
      if len(branchrevisions) > 0:
        print("{:<16}    {:<62}".format("Revision:", "Comment:"))
        for row in branchrevisions:
          print("{:<16}    {:<62}".format(row[0], row[1])) 
  
  def commit(self):
    if self.__validate():
      (revision, branch) = self.__gethead() 
      filestocommit, missedfiles = self.__innerstatus()
      if len(missedfiles)>0:
        for filename in missedfiles:
          print("Missed file: {0}".format(filename))
        print("Commit is denied.")
        self.close()
        sys.exit(5)  
      if len(filestocommit)>0:
        stagedfiles=self.__getstaging()
        newrevisionpath = os.path.join(self.__revisionspath, str(revision))
        if not os.path.exists(newrevisionpath): os.makedirs(newrevisionpath) 
        comment=""
        prompt="Input comment for the revision:\n"
        if 'raw_input' in globals():
          comment = raw_input(prompt)
        else:
          comment = input(prompt)
        for filename in stagedfiles:
          fileabspath = os.path.join(os.getcwd(), filename)
          if os.path.exists(fileabspath):     
            filedir = os.path.dirname(fileabspath)
            relpath = os.path.relpath(filedir, os.getcwd())
            destinationpath = newrevisionpath
            if relpath != ".": destinationpath = os.path.join(newrevisionpath, relpath)   
            if not os.path.exists(destinationpath): os.makedirs(destinationpath)
            shutil.copy(filename, destinationpath)
            print("{0} is added to the repository.".format(filename ))
        revisiontocommit = int(revision) + 1 
        self.__dbman.execute("delete from head")
        self.__dbman.execute("insert into head (revision, branch) values(?,?)", (revisiontocommit, branch))
        self.__dbman.execute("insert into revisions (revision, branch, comment) values (?,?,?)", (revision, branch, comment))
        self.__dbman.execute("insert into staginghistory (revision, file) select ?, file from staging", (revision, ))
        print("Revision {0} is commited successfully to the {1} branch.".format(revision, branch))
      else:
        print("Nothing to commit.")
   
  def reset(self, revision):
    if self.__validate():
      resetrevisionpath = os.path.join(self.__revisionspath, str(revision))
      if os.path.exists(resetrevisionpath):
        (revisiontocommit, branch) = self.__gethead()                                   
        query = "select revision from revisions where branch=?"
        branchrevisions = self.__dbman.execute(query, (branch,))
        branchrevisions = [branchrevision[0] for branchrevision in branchrevisions]
        if revision in branchrevisions:
          walkresults = os.walk(resetrevisionpath)
          filenames = [] 
          for walkresult in walkresults:
            if len(walkresult[2]) > 0:
              dirpath = os.path.join(os.getcwd(), walkresult[0])
              relpath = os.path.relpath(dirpath, resetrevisionpath)  
              if relpath == ".": relpath = ""
              for filename in walkresult[2]:
                filetoadd = os.path.join(relpath, filename)
                filenames.append(filetoadd)
          for filenametocopy in filenames:
            workingtreefilename = os.path.join(os.getcwd(), filenametocopy)
            workingtreefiledirname = os.path.dirname(workingtreefilename)
            if workingtreefiledirname != os.getcwd(): 
              if not os.path.exists(workingtreefiledirname): os.makedirs(workingtreefiledirname)
            if os.path.exists(workingtreefilename): os.remove(workingtreefilename)
            shutil.copy(os.path.join(resetrevisionpath, filenametocopy), workingtreefiledirname)
          self.__dbman.execute("delete from staging")
          self.__dbman.execute("insert into staging select file from staginghistory where revision=?", (revision, ))
          print("Working tree is reset to the {0} revision.".format(revision))
        else:
          print("'{0}'branch doesn't contain {1} revision.".format(branch, revision))
      else:
        print("Revision {0} doesn't exist.".format(revision))

  def branch(self, newbranch):
    if self.__validate():
      (revision, currentbranch) = self.__gethead()
      if currentbranch != newbranch:
        branches = self.__dbman.execute("select branch from branches where branch=?", (newbranch,))
        isexists = len(branches) > 0 and "existed" or "new"
        self.__dbman.execute("delete from head")
        self.__dbman.execute("insert into head (revision, branch) values(?,?)", (revision, newbranch))
        print("Switched to the {0} {1} branch.".format(isexists, newbranch))
      else:
        print("You try to switch to the current branch.")
      self.current()
  
  def merge(self, branchto):
    if self.__validate():
      filestocommit, missedfiles = self.__innerstatus()
      if len(filestocommit)==0 and len(missedfiles)==0: 
        (revisiontocommit, currentbranch) = self.__gethead()
        if currentbranch != branchto:
          branches = self.__dbman.execute("select branch from branches where branch=?", (branchto,))
          if len(branches)==0:
            print("You try to merge to the non-existing branch.")
          else:
            maxrevisionto = self.__dbman.execute("select max(revision) from revisions where branch=?", (branchto,))
            maxrevisionto = maxrevisionto[0][0]
            self.branch(branchto)
            self.reset(maxrevisionto)
            maxrevisionfrom = self.__dbman.execute("select max(revision) from revisions where branch=?", (currentbranch,))
            maxrevisionfrom = maxrevisionfrom[0][0]
            self.branch(currentbranch)
            self.reset(maxrevisionfrom)
            self.__dbman.execute("delete from staging")
            query="insert into staging(file) select  file from staginghistory \
                   where revision=? and file not in (select file from  staginghistory \
                   where revision=?) union select file from staginghistory \
                   where revision=?"
            self.__dbman.execute(query, (maxrevisionto, maxrevisionfrom, maxrevisionfrom))
        else: 
          print("You try to merge to the current branch.")
          self.current()
      else:
        print("Merge is denied.")
        self.status() 

  def status(self):
    filestocommit, missedfiles = self.__innerstatus()
    if len(missedfiles)>0:
      for filename in missedfiles:
        print("Missed file: {0}".format(filename))
    if len(filestocommit)>0:  
      for filename in filestocommit:
        print("File for commit: {0}".format(filename)) 
    else:
      print("Nothing to commit.")

  def showstaging(self):
    print("Staged files:")
    for filen in self.__getstaging():
      print(filen)
           
allowedcommands = ["init", "add", "current", "commit", "reset", "branch", 
                   "status", "rm", "mv", "showstaging", "merge"]
repomanager = RepoManager()

def usage():
  print("usage: command [arguments]")
  exitrtw(1) 

def exitrtw(status):
  repomanager.close()
  sys.exit(status)

def main():
  try:
    args = sys.argv[1:]
    if len(args) > 0:   
      command = args[0]
      if command in allowedcommands:
        if command == "init":
          repomanager.initialize()
          exitrtw(0)
        elif command == "add":
          files = args[1:]
          if len(files) > 0:  
            repomanager.add(files)
            exitrtw(0)
        elif command=="rm":
          files = args[1:]
          if len(files) > 0:  
            repomanager.rm(files)
            exitrtw(0) 
        elif command=="mv":
          files=args[1:3]
          if len(files)==2:
            (src, dst)=files  
            repomanager.mv(src, dst)
            exitrtw(0) 
        elif command == "current":
          repomanager.current()
          exitrtw(0)
        elif command == "showstaging":
          repomanager.showstaging()
          exitrtw(0)
        elif command == "commit":
          repomanager.commit()
          exitrtw(0)
        elif command == "reset":
          if len(args[1:]) == 1:
            try:
              revision = int(args[1])
            except (TypeError, ValueError): 
              print("Please, provide integer revision number.")
              exitrtw(3)
            if revision<1:
              print("Please, provide positive revision number.") 
              exitrtw(4)
            else:
              repomanager.reset(revision)
              exitrtw(0)
        elif command == "branch":
          if len(args[1:]) == 1:
            repomanager.branch(str(args[1]))
            exitrtw(0)
        elif command == "merge":
          if len(args[1:]) == 1:
            repomanager.merge(str(args[1]))
            exitrtw(0)
        elif command == "status":
          repomanager.status()
          exitrtw(0)
           
    usage()
  except BaseException:
    repomanager.close()
    raise

if __name__ == "__main__":
  main()
