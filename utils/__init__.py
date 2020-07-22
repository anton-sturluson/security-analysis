import os

# path to log
LOGPATH = os.path.join("crawler", "log.txt")
# path to result
RESULTPATH = os.path.join("crawler", "result.json")
# path to data directory
DATADIR = os.path.join("data")
# path to company directory
COMPANYDIR = os.path.join(DATADIR, "company")
# path to profile
PROFILEPATH = os.path.join(DATADIR, "stock_profile.csv")
# path to backup profile
PROFILEBACKPATH = os.path.join(DATADIR, "stock_profile_backup.csv")
