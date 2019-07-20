import argparse
import json
from adb.adb import ADB


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ADB - Load DB From JSON")
    
    parser.add_argument("-f", "--file", help="JSON filename to load")
    parser.add_argument("-d", "--db", help="ArangoDB Database")
    parser.add_argument("-c", "--collection", help="ArangoDB Collection")
    parser.add_argument("-u", "--user", help="ArangoDB User")
    parser.add_argument("-p", "--password", help="ArangoDB Password")
    parser.add_argument("-k", "--key", help="Document Key in file data set")

    args = parser.parse_args()

    if not (args.key or args.collection or args.db):
        print("# Missing arguments: --key|--collection|--db")
        exit(1)

    adb = ADB(username=args.user, pwd=args.password, db=args.db, forceCreate=True)
    col = adb.getCollection(args.collection)
    try:
        f = open(args.file, "r")
    except Exception as e:
        print("# ERROR loading file {}: {}".format(args.file, e))

    try:
        for line in f:
            line = unicode(line, 'utf-8')
            p = json.loads(line)
            doc = col.createDocument()
            doc._key = p[args.key]
            for pk in p.keys():
                doc[pk] = p[pk]
            print("Loading {}".format(p[args.key]))
            doc.save()
    except Exception as e:
        print("# ERROR loading documents: {}".format(e))
