import os
try:
    from arango import ArangoClient
    from arango import exceptions as AExceptions
except Exception as e:
    print("ERROR importing pyArango lib: ", e)

class ADB(object):
    def __init__(self,
                 username=None, pwd=None,
                 hostProto=None, hostName=None, hostPort=None,
                 db=None, forceCreate=False):
        self.hostProto = self.getHostProto(hostProto)
        self.hostName = self.getHostName(hostName)
        self.hostPort = self.getHostPort(hostPort)
        self.user = self.getUser(username)
        self.pwd = self.getPassword(pwd)
        self.forceCreate = forceCreate
        self.dbName = db
        self.conn = None
        self.dbConn = None

        ## Init
        self.getConn()
        self.getDB(name=self.dbName)

    def getHostProto(self, hostProto):
        if not hostProto:
            return os.getenv("ARANGODB_URL", 'http://localhost:8529').split('://')[0]
        return hostProto

    def getHostName(self, hostName):
        if not hostName:
            return os.getenv("ARANGODB_URL", 'http://localhost:8529').split('://')[1].split(':')[0]
        return hostName

    def getHostPort(self, hostPort):
        if not hostPort:
            return os.getenv("ARANGODB_URL", 'http://localhost:8529').split('://')[1].split(':')[1]
        return hostPort

    def getUser(self, user):
        if not user:
            return os.getenv("ARANGODB_USERNAME")
        return user

    def getPassword(self, pwd):
        if not pwd:
            return os.getenv("ARANGODB_PASSWORD")
        return pwd

    def createConn(self):
        return ArangoClient(hosts="{}://{}:{}".format(self.hostProto, self.hostName, self.hostPort) )

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
            self.dbConn = self.conn.db(name,
                    username=self.user,
                    password=self.pwd
                )
        except Exception as e:
            if not self.forceCreate:
                print("ERROR DB not found and forceCreate=false.")
                raise e
            # try to conn on _system db to create new db
            try:
                sys_db = self.conn.db('_system',
                        username=self.user,
                        password=self.pwd
                    )
            except Exception as e2:
                print("ERROR DB {} not found and unable to conn to _system db: {}".format(name, e2))
                raise e2

            try:
                if not sys_db.has_database(name):
                    sys_db.create_database(name)
                
                self.dbConn = self.dbConn.db(name,
                        username=self.user,
                        password=self.pwd
                    )
            except Exception as e2:
                print("ERROR DB {} not found and unable to create it: {}".format(name, e2))
                raise e2

        return self.dbConn

    def getCollection(self, colName, edge=False):
        if not colName:
            print("Collection Name not provided.")
            return None
        
        if not self.dbConn:
            print(self.dbName)
            self.dbConn = self.getDB(name=self.dbName)
 
        if self.dbConn.has_collection(colName):
            return self.dbConn.collection(colName)

        if not self.forceCreate:
            print("ERROR: Collection not found and forceCreate=false.")
            return None

        return self.dbConn.create_collection(colName, edge=edge)


    def insertDocument(self, col, doc):
        return self.dbConn.insert_document(col, doc)

    def insertOrUpdateDocument(self, colName, doc):
        if doc['_key']:
            if self.dbConn.has_document("{}/{}".format(colName, doc['_key'])):
                newDoc = doc
                newDoc['_id'] = "{}/{}".format(colName, doc['_key'])
                del newDoc['_key']
                return self.dbConn.update_document(newDoc)

        return self.insertDocument(colName, doc)
