from os import path

# path to crawler dir
CRAWLERDIR = "crawler"
# path to log
LOGPATH = path.join(CRAWLERDIR, "log.txt")
# path to result
RESULTPATH = path.join(CRAWLERDIR, "result.json")
# path to mapping.json
MAPPINGPATH = path.join(CRAWLERDIR, "mapping.json")
# path to data directory
DATADIR = path.join("data")
# path to company directory
COMPANYDIR = path.join(DATADIR, "company")
# path to profile
PROFILEPATH = path.join(DATADIR, "stock_profile.csv")
# path to backup profile
PROFILEBACKPATH = path.join(DATADIR, "stock_profile_backup.csv")
