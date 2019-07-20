import os
try:
    import pyArango.connection as cn
except Exception as e:
    print("ERROR importing pyArango lib: ", e)

class ADB(object):
    def __init__(self, username=None, pwd=None,
                 db=None, forceCreate=False):
        self.user = self.getUser(username)
        self.pwd = self.getPassword(pwd)
        self.forceCreate = forceCreate
        self.dbName = db
        self.conn = None
        self.dbConn = None

        ## Init
        self.getConn()
        self.getDB(db)
    
    def getUser(self, user):
        if not user:
            return os.getenv("ARANGODB_USERNAME")
        return user

    def getPassword(self, pwd):
        if not pwd:
            return os.getenv("ARANGODB_PASSWORD")
        return pwd

    def createConn(self):
        return cn.Connection(username=self.user, password=self.pwd)

    def getConn(self):
        if not self.conn:
            self.conn = self.createConn()
        return self.conn

    def getDB(self, name=None):
        if not name:
            name = self.dbName
        if not name:
            print("ERROR DB name not provided.")
            return
        if not self.conn:
            getConn()
        
        try:
            self.dbConn = self.conn[name]
        except Exception as e:
            if not self.forceCreate:
                print("ERROR DB not found and forceCreate=false.")
                raise e
            self.dbConn = self.conn.createDatabase(name=name)
        return self.dbConn

    def getCollection(self, colName):
        if not colName:
            print("Collection Name not provided.")
            return
        try:
            col = self.dbConn[colName]
        except Exception as e:
            if not self.forceCreate:
                print("ERROR: Collection not found and forceCreate=false.")
                raise e
            col = self.dbConn.createCollection(name=colName)
            print("Collection created.")
        return col
